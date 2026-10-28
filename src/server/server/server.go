package server

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	articleHandler "github.com/Na4chO/tp-escalabilidad/server/articlehandler"
	"github.com/Na4chO/tp-escalabilidad/server/clientregistry"
	"github.com/Na4chO/tp-escalabilidad/server/config"
	"github.com/Na4chO/tp-escalabilidad/server/external"
)

type Server struct {
	registry     clientregistry.ClientRegistry
	intakeQueue  middleware.Middleware
	resultsQueue middleware.Middleware
	listener     net.Listener
	running      atomic.Bool
}

func NewServer(cfg config.Config) (*Server, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	resultsQueue, err := middleware.CreateQueueMiddleware("results", connSettings)
	if err != nil {
		return nil, err
	}

	intakeQueue, err := middleware.CreateQueueMiddleware("intake", connSettings)
	if err != nil {
		resultsQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.ServerHost+":"+cfg.ServerPort)
	if err != nil {
		resultsQueue.Close()
		intakeQueue.Close()
		return nil, err
	}

	sv := &Server{resultsQueue: resultsQueue, intakeQueue: intakeQueue, listener: listener}
	sv.running.Store(true)
	return sv, nil
}

func (s *Server) Run() error {
	defer s.listener.Close()

	go s.intakeQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		s.handleClientResponse(msg, ack, nack)
	})
	go s.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !s.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := articleHandler.NewArticleHandler()
		client := clientregistry.ClientState{Conn: conn, Handler: &handler}
		s.registry.Add(client)

		go s.handleClientRequest(client)
	}

	s.intakeQueue.StopConsuming()
	s.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			client.Conn.Close()
		}
	})
	return nil
}

func (s *Server) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	s.running.Store(false)
	s.listener.Close()
}

func (s *Server) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.Article:
			if err := s.handleArticleMessage(client); err != nil {
				slog.Debug("While handling record message", "err", err)
				return
			}

		case external.EndOfFile:
			if err := s.handleEndOfRecordsMessage(client); err != nil {
				slog.Debug("While handling end of records message", "err", err)
				return
			}
			break loop

		default:
			slog.Debug("Read unexpected message type")
			return
		}
	}
}

func (s *Server) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	return
}
