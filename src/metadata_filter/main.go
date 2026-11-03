package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/metadata_filter/config"
	mf "github.com/Na4chO/tp-escalabilidad/metadata_filter/metadata_filter"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := mf.NewMetadataFilter(cfg)
	if err != nil {
		slog.Error("While creating metadata filter", "err", err)
		return 1
	}

	if err := node.Run(); err != nil {
		slog.Error("Metadata Filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
