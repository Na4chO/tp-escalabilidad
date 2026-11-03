package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/abstract_preprocessing/config"
	p "github.com/Na4chO/tp-escalabilidad/abstract_preprocessing/preprocessing"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := p.NewPreprocessing(cfg)
	if err != nil {
		slog.Error("While creating Abstract Preprocessing", "err", err)
		return 1
	}
	if err := node.Run(); err != nil {
		slog.Error("Abstract Preprocessing stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
