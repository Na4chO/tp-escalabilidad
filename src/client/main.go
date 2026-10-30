package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/client/client"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputPath := os.Getenv("INPUT_PATH")
	if inputPath == "" {
		return client.ClientConfig{}, errors.New("INPUT_PATH environment variable is required")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputPath:  inputPath,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	wikiClient, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := wikiClient.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
