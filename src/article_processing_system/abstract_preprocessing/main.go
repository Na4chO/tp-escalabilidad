package main

import (
	"log/slog"
	"os"

	p "github.com/Na4chO/tp-escalabilidad/article_processing_system/abstract_preprocessing/abstract_preprocessing"
	"github.com/Na4chO/tp-escalabilidad/article_processing_system/abstract_preprocessing/config"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	node, err := p.NewAbstractPreprocessing(cfg)
	if err != nil {
		slog.Error("While creating Abstract Preprocessing", "err", err)
		return 1
	}
	defer node.Close()

	if err := node.Run(); err != nil {
		slog.Error("Abstract Preprocessing stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
