package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/image_fetcher/config"
	f "github.com/Na4chO/tp-escalabilidad/image_fetcher/image_fetcher"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := f.NewImageFetcher(cfg)
	if err != nil {
		slog.Error("While creating image fetcher", "err", err)
		return 1
	}

	if err := node.Run(); err != nil {
		slog.Error("Image Fetcher stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
