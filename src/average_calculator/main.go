package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/average_calculator/calculator"
	"github.com/Na4chO/tp-escalabilidad/average_calculator/config"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	calc, err := calculator.NewCalculator(cfg)
	if err != nil {
		slog.Error("While initializing average calculator", "err", err)
		return 1
	}
	defer calc.Close()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		slog.Info("SIGTERM signal received")
		_ = calc.Stop()
	}()

	slog.Info("Average calculator running")
	if err := calc.Run(); err != nil {
		slog.Error("Average calculator stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
