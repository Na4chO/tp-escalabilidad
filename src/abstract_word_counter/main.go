package main

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/abstract_word_counter/counter"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

func loadConfig() (middleware.ConnSettings, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return middleware.ConnSettings{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return middleware.ConnSettings{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	return middleware.ConnSettings{Hostname: momHost, Port: momPort}, nil
}

func run() int {
	connSettings, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	wordCounter, err := counter.NewCounter(connSettings)
	if err != nil {
		slog.Error("While initializing abstract word counter", "err", err)
		return 1
	}
	defer wordCounter.Close()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		slog.Info("SIGTERM signal received")
		wordCounter.Stop()
	}()

	slog.Info("Abstract word counter running")
	if err := wordCounter.Run(); err != nil {
		slog.Error("Abstract word counter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
