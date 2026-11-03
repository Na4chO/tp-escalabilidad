package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/top_aggregator/config"
	fc "github.com/Na4chO/tp-escalabilidad/top_aggregator/top_aggregator"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := fc.NewTopAggregator(cfg)
	if err != nil {
		slog.Error("While initializing Top Aggregator", "err", err)
		return 1
	}

	if err := node.Run(); err != nil {
		slog.Error("Top Aggregator stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
