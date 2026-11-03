package top_aggregator

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/top_aggregator/config"
)

type clientState struct {
	topWords      []inner.WordCount
	receivedCount uint32
	expectedTotal uint32
	hasEOF        bool
}

type TopAggregator struct {
	running         atomic.Bool
	topSize         int
	clientStates    map[uint32]*clientState
	partialTopQueue *common.PacketQueue[inner.PartialTop]
	resultQueue     *common.PacketQueue[inner.QueryResult]
}

func NewTopAggregator(cfg *config.Config) (*TopAggregator, error) {
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

	fc := &TopAggregator{
		topSize:         cfg.TopSize,
		clientStates:    make(map[uint32]*clientState),
		partialTopQueue: partialTopQueue,
		resultQueue:     resultQueue,
	}
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
	var err error
	if packet.IsEof {
		err = ta.handleEOFPacket(packet)
	} else {
		err = ta.handleDataPacket(packet)
	}

	if err != nil {
		slog.Error("Error processing partial top packet", "clientID", packet.ClientID, "isEOF", packet.IsEof, "err", err)
		nack()
		return
	}
	ack()
}

func (ta *TopAggregator) handleDataPacket(packet external.Packet[inner.PartialTop]) error {
	state := ta.clientStateFor(packet.ClientID)

	incoming := packet.Payload.Top
	slices.SortFunc(incoming, func(a, b inner.WordCount) int {
		return cmp.Compare(b.Count, a.Count)
	})

	state.topWords = mergeTops(state.topWords, incoming, ta.topSize)
	state.receivedCount++

	return ta.checkAndFinalize(packet.ClientID, state)
}

func mergeTops(current, incoming []inner.WordCount, topSize int) []inner.WordCount {
	if len(incoming) == 0 {
		return current
	}
	if len(current) == 0 {
		if len(incoming) > topSize {
			return incoming[:topSize]
		}
		return incoming
	}

	newTop := make([]inner.WordCount, min(topSize, len(current)+len(incoming)))

	i, j := 0, 0
	for (i+j < topSize) && (i < len(current) || j < len(incoming)) {
		switch {
		case j >= len(incoming):
			newTop[i+j] = current[i]
			i++
		case i >= len(current):
			newTop[i+j] = incoming[j]
			j++
		case current[i].Count < incoming[j].Count:
			newTop[i+j] = incoming[j]
			j++
		default:
			newTop[i+j] = current[i]
			i++
		}
	}

	return newTop
}

func (ta *TopAggregator) handleEOFPacket(packet external.Packet[inner.PartialTop]) error {
	state := ta.clientStateFor(packet.ClientID)
	state.hasEOF = true
	state.expectedTotal = packet.Total

	return ta.checkAndFinalize(packet.ClientID, state)
}

func (ta *TopAggregator) checkAndFinalize(clientID uint32, state *clientState) error {
	if !state.hasEOF || state.receivedCount < state.expectedTotal {
		return nil
	}

	result := inner.QueryResult{
		QueryType: inner.QueryTopWords,
		TopWords:  state.topWords,
	}

	if err := ta.resultQueue.SendData(clientID, result); err != nil {
		return fmt.Errorf("sending result for client %d: %w", clientID, err)
	}

	if err := ta.resultQueue.SendEOFWithPayload(clientID, inner.QueryResult{QueryType: inner.QueryTopWords}); err != nil {
		return fmt.Errorf("sending EOF result for client %d: %w", clientID, err)
	}

	delete(ta.clientStates, clientID)
	return nil
}

func (ta *TopAggregator) clientStateFor(clientID uint32) *clientState {
	state, exists := ta.clientStates[clientID]
	if !exists {
		state = &clientState{topWords: []inner.WordCount{}}
		ta.clientStates[clientID] = state
	}
	return state
}
