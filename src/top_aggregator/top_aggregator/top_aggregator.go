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
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
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
	running            atomic.Bool
	topSize            int
	clientStates       map[clientid.ID]*clientState
	partialTopExchange middleware.Router
	resultQueue        *common.PacketQueue[inner.QueryResult]
}

func NewTopAggregator(cfg *config.Config) (*TopAggregator, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	inputKey := common.PartitionKey(common.ExchangePartialTops, cfg.ID)
	partialTopExchange, err := middleware.CreateExchangeMiddleware(common.ExchangePartialTops, []string{inputKey}, connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		partialTopExchange.Close()
		return nil, err
	}

	ta := &TopAggregator{
		topSize:            cfg.TopSize,
		clientStates:       make(map[clientid.ID]*clientState),
		partialTopExchange: partialTopExchange,
		resultQueue:        resultQueue,
	}
	ta.running.Store(true)
	return ta, nil
}

func (ta *TopAggregator) Run() error {
	go ta.handleSignals()

	err := errors.Join(
		ta.partialTopExchange.StartConsuming(ta.handlePartialTopMessage),
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
	_ = ta.partialTopExchange.StopConsuming()
}

func (ta *TopAggregator) closeMiddlewares() error {
	return errors.Join(
		ta.partialTopExchange.StopConsuming(),
		ta.partialTopExchange.Close(),
		ta.resultQueue.Close(),
	)
}

func (ta *TopAggregator) handlePartialTopMessage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.PartialTop]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding partial top message", "err", err)
		nack()
		return
	}

	ta.handlePartialTopPacket(packet, ack, nack)
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
		if byCount := cmp.Compare(b.Count, a.Count); byCount != 0 {
			return byCount
		}
		return cmp.Compare(a.Word, b.Word)
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
		case current[i].Count == incoming[j].Count && incoming[j].Word < current[i].Word:
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

func (ta *TopAggregator) checkAndFinalize(clientID clientid.ID, state *clientState) error {
	if !state.hasEOF || state.receivedCount < state.expectedTotal {
		return nil
	}

	result := inner.QueryResult{
		QueryType: inner.QueryTopWords,
		TopWords:  state.topWords,
	}

	if err := ta.resultQueue.SendData(clientID, result); err != nil {
		return fmt.Errorf("sending result for client %s: %w", clientID, err)
	}

	if err := ta.resultQueue.SendEOFWithPayload(clientID, inner.QueryResult{QueryType: inner.QueryTopWords}); err != nil {
		return fmt.Errorf("sending EOF result for client %s: %w", clientID, err)
	}

	delete(ta.clientStates, clientID)
	return nil
}

func (ta *TopAggregator) clientStateFor(clientID clientid.ID) *clientState {
	state, exists := ta.clientStates[clientID]
	if !exists {
		state = &clientState{topWords: []inner.WordCount{}}
		ta.clientStates[clientID] = state
	}
	return state
}
