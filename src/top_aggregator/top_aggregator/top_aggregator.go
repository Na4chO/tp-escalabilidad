package top_aggregator

import (
	"errors"
	"go/types"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/top_aggregator/config"
)

type clientState struct{}

type TopAggregator struct {
	running         atomic.Bool
	clientStates    types.Nil
	partialTopQueue *common.PacketQueue[inner.PartialTop]
	resultQueue     *common.PacketQueue[inner.QueryResult]
}

func NewFrequencyCollector(cfg *config.Config) (*TopAggregator, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	partialTopQueue, err := common.NewPacketQueue[inner.PartialTop]("partialTops", connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("result", connSettings)
	if err != nil {
		partialTopQueue.Close()
		return nil, err
	}

	fc := &TopAggregator{partialTopQueue: partialTopQueue, resultQueue: resultQueue}
	fc.running.Store(true)
	return fc, nil
}

func (ta *TopAggregator) Run() error {
	go ta.handleSignals()

	err := errors.Join(
		ta.partialTopQueue.StartConsuming(func(packet external.Packet[inner.PartialTop], ack, nack func()) {
			ta.handlePartialTopPacket(packet, ack, nack)
		}),
		ta.closeMiddlewares(),
	)

	if ta.running.Load() {
		return err
	}

	return nil
}

func (ta *TopAggregator) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	ta.running.Store(false)
	_ = ta.partialTopQueue.StopConsuming()
}

func (ta *TopAggregator) closeMiddlewares() error {
	return errors.Join(
		ta.partialTopQueue.StopConsuming(),
		ta.partialTopQueue.Close(),
		ta.resultQueue.Close(),
	)
}

func (ta *TopAggregator) handlePartialTopPacket(packet external.Packet[inner.PartialTop], ack func(), nack func()) {
	if packet.IsEof {
		if err := ta.handleEOFPacket(packet); err != nil {
			slog.Error("While handling EOF packet", "err", err)
			nack()
		}
		ack()
		return
	}

	if err := ta.handleAbstractWords(packet); err != nil {
		slog.Error("While handling abstract words message", "err", err)
		nack()
	}
	ack()
}
