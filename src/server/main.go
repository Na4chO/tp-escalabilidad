package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/server/config"
	"github.com/Na4chO/tp-escalabilidad/server/server"
)

func run() int {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	sv, err := server.NewServer(*cfg)
	if err != nil {
		slog.Error("While initializing server", "err", err)
		return 1
	}

	if err := sv.Run(); err != nil {
		slog.Error("Server stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
