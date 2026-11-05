package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/article_processing_system/frequency_collector/config"
	fc "github.com/Na4chO/tp-escalabilidad/article_processing_system/frequency_collector/frequency_collector"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := fc.NewFrequencyCollector(cfg)
	if err != nil {
		slog.Error("While initializing Frequency Collector", "err", err)
		return 1
	}

	if err := node.Run(); err != nil {
		slog.Error("Frequency Collector stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
