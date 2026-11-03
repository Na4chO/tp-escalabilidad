package server

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	articleHandler "github.com/Na4chO/tp-escalabilidad/server/articlehandler"
	"github.com/Na4chO/tp-escalabilidad/server/clientregistry"
	"github.com/Na4chO/tp-escalabilidad/server/config"
)

type Server struct {
	registry      *clientregistry.ClientRegistry
	handler       *articleHandler.ArticleHandler
	metadataQueue *common.PacketQueue[inner.Metadata]
	abstractQueue *common.PacketQueue[inner.Abstract]
	resultQueue   *common.PacketQueue[inner.QueryResult]
	listener      net.Listener
	running       atomic.Bool
}

func NewServer(cfg config.Config) (*Server, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	resultsQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		return nil, err
	}

	metadataQueue, err := common.NewPacketQueue[inner.Metadata](common.QueueMetadata, connSettings)
	if err != nil {
		resultsQueue.Close()
		return nil, err
	}

	abstractQueue, err := common.NewPacketQueue[inner.Abstract](common.QueueAbstract, connSettings)
	if err != nil {
		resultsQueue.Close()
		metadataQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.ServerHost+":"+cfg.ServerPort)
	if err != nil {
		resultsQueue.Close()
		metadataQueue.Close()
		abstractQueue.Close()
		return nil, err
	}
	handler := articleHandler.NewArticleHandler(metadataQueue, abstractQueue)
	sv := &Server{
		registry:      clientregistry.NewClientRegistry(),
		handler:       handler,
		metadataQueue: metadataQueue,
		abstractQueue: abstractQueue,
		resultQueue:   resultsQueue,
		listener:      listener,
	}
	sv.running.Store(true)
	return sv, nil
}

func (s *Server) Run() error {
	defer s.listener.Close()

	go func() {
		err := s.resultQueue.StartConsuming(func(packet external.Packet[inner.QueryResult], ack, nack func()) {
			s.handleClientResponse(packet, ack, nack)
		})
		if err != nil {
			slog.Error("Consuming results", "err", err)
		}
	}()
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

		clientID := s.registry.Add(clientregistry.ClientState{Conn: conn})
		go s.handleClientRequest(clientID, conn)
	}

	s.resultQueue.StopConsuming()
	var conns []net.Conn
	s.registry.WithLock(func(clients map[uint32]clientregistry.ClientState) {
		for _, client := range clients {
			conns = append(conns, client.Conn)
		}
	})
	for _, conn := range conns {
		conn.Close()
	}
	s.resultQueue.Close()
	s.abstractQueue.Close()
	s.metadataQueue.Close()
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

func (s *Server) handleClientRequest(clientID uint32, conn net.Conn) {
loop:
	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			slog.Debug("While reading message type", "err", err)
			s.registry.Remove(clientID)
			conn.Close()
			return
		}

		switch msgType {
		case external.Article:
			a, err := external.ReadArticle(conn)
			if err != nil {
				slog.Debug("While reading article", "err", err)
				s.registry.Remove(clientID)
				conn.Close()
				return
			}

			if err := s.handler.HandleArticle(clientID, a); err != nil {
				slog.Debug("While handling article", "err", err)
				s.registry.Remove(clientID)
				conn.Close()
				return
			}

		case external.EndOfFile:
			if err := s.handler.HandleEOF(clientID); err != nil {
				slog.Debug("While handling end of file", "err", err)
				s.registry.Remove(clientID)
				conn.Close()
				return
			}
			break loop

		default:
			slog.Debug("Read unexpected message type")
			s.registry.Remove(clientID)
			conn.Close()
			return
		}
	}
}

func (s *Server) handleClientResponse(packet external.Packet[inner.QueryResult], ack func(), nack func()) {
	client, ok := s.registry.Get(packet.ClientID)
	if !ok {
		slog.Debug("Client not found in registry", "clientID", packet.ClientID)
		nack()
		return
	}
	if packet.IsEof {
		s.registry.Remove(packet.ClientID)
		if err := client.Conn.Close(); err != nil {
			slog.Error("Closing connection", "err", err)
		}
		ack()
		return
	}
	if err := external.WriteQueryResult(client.Conn, packet.Payload); err != nil {
		s.registry.Remove(packet.ClientID)
		if err := client.Conn.Close(); err != nil {
			slog.Error("Closing connection", "err", err)
		}
		slog.Debug("While writing query result", "err", err)
		ack()
		return
	}
	ack()
}
