package main

import (
	"log/slog"
	"os"

	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/server/config"
	"github.com/Na4chO/tp-escalabilidad/server/server"
)

func run() {
	cfg, err := config.NewConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
	}

	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	intakeQueue, err := middleware.CreateQueueMiddleware("intake", connSettings)
	if err != nil {
		slog.Error("While creating abstracts queue", "err", err)
		return
	}

	resultsQueue, err := middleware.CreateQueueMiddleware("results", connSettings)
	if err != nil {
		intakeQueue.Close()
		slog.Error("While creating results queue", "err", err)
		return
	}

	sv, err := server.NewServer(cfg)
	if err != nil {
		slog.Error("While initializing server", "err", err)
		return
	}

	if err := sv.Run(); err != nil {
		slog.Error("Server stopped with error", "err", err)
		return
	}
	return
}

func main() {
	os.Exit(run())
}
