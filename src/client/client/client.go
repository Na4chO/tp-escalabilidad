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
}

type Client struct {
	conn    net.Conn
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	go client.handleSignals()

	results := make(chan error, 1)
	go func() { results <- receiveResults(client.conn, client.config.OutputPath) }()

	if err := client.sendArticles(); err != nil {
		if !client.running.Load() {
			slog.Info("Client stopped before sending all articles")
			return nil
		}
		return fmt.Errorf("sending articles: %w", err)
	}

	if err := <-results; err != nil {
		if !client.running.Load() {
			slog.Info("Client stopped before receiving all results")
			return nil
		}
		return fmt.Errorf("receiving results: %w", err)
	}
	slog.Info("Results received", "output", client.config.OutputPath)
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

	var totalArticles, totalBytes int
	start := time.Now()

	for _, path := range files {
		slog.Info("Reading file", "path", path)
		if err := readParquetFile(path, client.running.Load, client.handleArticle); err != nil {
			return err
		}
	}

	if err := external.WriteEndOfFile(client.conn); err != nil {
		return err
	}

	slog.Info("Dataset sent",
		"files", len(files),
		"articles", totalArticles,
		"serialized_bytes", totalBytes,
		"elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

func (client *Client) handleArticle(a article.Article) error {
	return external.WriteArticle(client.conn, a)
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}
