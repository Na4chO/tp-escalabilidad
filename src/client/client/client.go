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
)

const connectionAttempts = 3
const connectionAttemptsDelayMs = 300

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputPath  string
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

	// Envio de archivos .parquet
	// Lanzar goroutine para esperar resultados

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
		if err := readParquetFile(path, batchSize, client.running.Load, handleBatch); err != nil {
			return err
		}
	}

	slog.Info("Dataset read",
		"files", len(files),
		"articles", totalArticles,
		"serialized_bytes", totalBytes,
		"elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

func handleBatch(batch []Article) error {
	var totalArticles, totalBytes int

	payload := SerializeBatch(batch)
	// TODO: armar el mensaje con este payload y escribirlo en client.conn
	totalArticles += len(batch)
	totalBytes += len(payload)
	slog.Debug("Batch serialized", "articles", len(batch), "bytes", len(payload))
	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}
