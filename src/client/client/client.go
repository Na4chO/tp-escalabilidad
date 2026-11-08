package client

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/common/article"
	"github.com/Na4chO/tp-escalabilidad/common/external"
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputPath  string
	OutputPath string
	BatchSize  int
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
	batch   []article.Article
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	slog.Info("Connected to server", "server", net.JoinHostPort(config.ServerHost, config.ServerPort))

	client := &Client{conn: conn, config: config}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	addr := net.JoinHostPort(host, port)
	var err error

	for attempt := 1; attempt <= connectionAttempts; attempt++ {
		conn, dialErr := net.Dial("tcp", addr)
		if dialErr == nil {
			return conn, nil
		}
		err = dialErr
		slog.Warn("Connection attempt failed", "attempt", attempt, "server", addr, "err", dialErr)
		if attempt < connectionAttempts {
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
		}
	}

	return nil, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	go client.handleSignals()
	start := time.Now()

	slog.Info("Client started",
		"input", client.config.InputPath,
		"output", client.config.OutputPath,
		"batch_size", client.config.BatchSize)

	results := make(chan error, 1)
	go func() { results <- receiveResults(client.conn, client.config.OutputPath) }()

	slog.Info("Sending articles")
	if err := client.sendArticles(); err != nil {
		if !client.running.Load() {
			slog.Info("Client stopped before sending all articles")
			return nil
		}
		return fmt.Errorf("sending articles: %w", err)
	}

	slog.Info("Waiting for results")
	if err := <-results; err != nil {
		if !client.running.Load() {
			slog.Info("Client stopped before receiving all results")
			return nil
		}
		return fmt.Errorf("receiving results: %w", err)
	}
	slog.Info("Results received", "output", client.config.OutputPath, "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

func (client *Client) sendArticles() error {
	files, err := listParquetFiles(client.config.InputPath)
	if err != nil {
		return fmt.Errorf("listing input files: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no .parquet files in %s", client.config.InputPath)
	}

	start := time.Now()

	for _, path := range files {
		slog.Info("Reading file", "path", path)
		if err := readParquetFile(path, client.running.Load, client.handleArticle); err != nil {
			return err
		}
	}

	if err := client.flushArticles(); err != nil {
		return err
	}
	if err := external.WriteEndOfFile(client.conn); err != nil {
		return err
	}

	slog.Info("Dataset sent",
		"files", len(files),
		"elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

func (client *Client) handleArticle(a article.Article) error {
	client.batch = append(client.batch, a)
	if len(client.batch) < client.config.BatchSize {
		return nil
	}
	return client.flushArticles()
}

func (client *Client) flushArticles() error {
	if len(client.batch) == 0 {
		return nil
	}
	err := external.WriteArticles(client.conn, client.batch)
	client.batch = client.batch[:0]
	return err
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	sig := <-signals
	slog.Info("Shutting down", "signal", sig.String())
	client.running.Store(false)
	client.conn.Close()
}
