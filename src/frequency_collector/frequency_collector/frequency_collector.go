package frequency_collector

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
	"github.com/Na4chO/tp-escalabilidad/frequency_collector/config"
)

type idiomsFrequency struct {
	EnglishFrequency uint32
	FrenchFrequency  uint32
}

type clientState struct {
	wordsFreq     map[string]*idiomsFrequency
	receivedCount uint32
	expectedTotal uint32
	hasEOF        bool
}

type FrequencyCollector struct {
	running         atomic.Bool
	topSize         int
	clientStates    map[clientid.ID]*clientState
	wordsQueue      *common.PacketQueue[inner.AbstractWords]
	partialTopQueue *common.PacketQueue[inner.PartialTop]
	resultQueue     *common.PacketQueue[inner.QueryResult]
}

func NewFrequencyCollector(cfg *config.Config) (*FrequencyCollector, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	wordsQueue, err := common.NewPacketQueue[inner.AbstractWords](common.QueueAbstractWords, connSettings)
	if err != nil {
		return nil, err
	}

	partialTopQueue, err := common.NewPacketQueue[inner.PartialTop](common.QueuePartialTops, connSettings)
	if err != nil {
		wordsQueue.Close()
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		partialTopQueue.Close()
		wordsQueue.Close()
		return nil, err
	}

	fc := &FrequencyCollector{
		topSize:         cfg.TopSize,
		clientStates:    make(map[clientid.ID]*clientState),
		wordsQueue:      wordsQueue,
		partialTopQueue: partialTopQueue,
		resultQueue:     resultQueue,
	}
	fc.running.Store(true)
	return fc, nil
}

func (fc *FrequencyCollector) Run() error {
	go fc.handleSignals()

	err := errors.Join(
		fc.wordsQueue.StartConsuming(func(packet external.Packet[inner.AbstractWords], ack, nack func()) {
			fc.handleAbstractWordsPacket(packet, ack, nack)
		}),
		fc.closeMiddlewares(),
	)

	if fc.running.Load() {
		return err
	}

	return nil
}

func (fc *FrequencyCollector) handleAbstractWordsPacket(packet external.Packet[inner.AbstractWords], ack func(), nack func()) {
	if packet.IsEof {
		if err := fc.handleEOFPacket(packet); err != nil {
			slog.Error("While handling EOF packet", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := fc.handleAbstractWords(packet); err != nil {
		slog.Error("While handling abstract words message", "err", err)
		nack()
		return
	}
	ack()
}

func (fc *FrequencyCollector) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	fc.running.Store(false)
	_ = fc.wordsQueue.StopConsuming()
}

func (fc *FrequencyCollector) closeMiddlewares() error {
	return errors.Join(
		fc.wordsQueue.StopConsuming(),
		fc.wordsQueue.Close(),
		fc.partialTopQueue.Close(),
		fc.resultQueue.Close(),
	)
}

func (fc *FrequencyCollector) clientStateFor(clientId clientid.ID) *clientState {
	state, ok := fc.clientStates[clientId]
	if !ok {
		state = &clientState{wordsFreq: make(map[string]*idiomsFrequency)}
		fc.clientStates[clientId] = state
	}
	return state
}

func (fc *FrequencyCollector) handleAbstractWords(packet external.Packet[inner.AbstractWords]) error {
	var intersections []string
	state := fc.clientStateFor(packet.ClientID)

	if packet.Payload.InLanguage != "en" && packet.Payload.InLanguage != "fr" {
		state.receivedCount++
		return fc.checkAndFinalize(packet.ClientID, state)
	}

	for _, word := range packet.Payload.Words {
		wordFreq, ok := state.wordsFreq[word]
		if !ok {
			wordFreq = &idiomsFrequency{}
			state.wordsFreq[word] = wordFreq
		}
		if isIntersection(packet.Payload.InLanguage, wordFreq) {
			intersections = append(intersections, word)
		}

		if packet.Payload.InLanguage == "en" {
			wordFreq.EnglishFrequency++
		} else if packet.Payload.InLanguage == "fr" {
			wordFreq.FrenchFrequency++
		}
	}

	if len(intersections) > 0 {
		if err := fc.sendIntersections(packet.ClientID, intersections); err != nil {
			return err
		}
	}

	state.receivedCount++
	return fc.checkAndFinalize(packet.ClientID, state)
}

func isIntersection(inLanguage string, wordFreq *idiomsFrequency) bool {
	switch inLanguage {
	case "en":
		return wordFreq.EnglishFrequency == 0 && wordFreq.FrenchFrequency > 0
	case "fr":
		return wordFreq.FrenchFrequency == 0 && wordFreq.EnglishFrequency > 0
	default:
		return false
	}
}

func (fc *FrequencyCollector) sendIntersections(clientId clientid.ID, intersections []string) error {
	return fc.resultQueue.SendData(clientId, inner.QueryResult{
		QueryType:         inner.QueryIntersections,
		IntersectionWords: intersections,
	})
}

func (fc *FrequencyCollector) handleEOFPacket(packet external.Packet[inner.AbstractWords]) error {
	state := fc.clientStateFor(packet.ClientID)
	state.hasEOF = true
	state.expectedTotal = packet.Total

	return fc.checkAndFinalize(packet.ClientID, state)
}

func (fc *FrequencyCollector) checkAndFinalize(clientID clientid.ID, state *clientState) error {
	if !state.hasEOF || state.receivedCount < state.expectedTotal {
		return nil
	}

	top := fc.topWords(state)

	if len(top) > 0 {
		if err := fc.partialTopQueue.SendData(clientID, inner.PartialTop{Top: top}); err != nil {
			return fmt.Errorf("sending partial top for client %s: %w", clientID, err)
		}
	}

	if err := fc.partialTopQueue.SendEOF(clientID); err != nil {
		return fmt.Errorf("sending partial top EOF for client %s: %w", clientID, err)
	}

	if err := fc.resultQueue.SendEOFWithPayload(clientID, inner.QueryResult{QueryType: inner.QueryIntersections}); err != nil {
		return fmt.Errorf("sending intersections EOF for client %s: %w", clientID, err)
	}

	delete(fc.clientStates, clientID)
	return nil
}

func (fc *FrequencyCollector) topWords(state *clientState) []inner.WordCount {
	top := make([]inner.WordCount, 0, len(state.wordsFreq))
	for word, freq := range state.wordsFreq {
		top = append(top, inner.WordCount{
			Word:  word,
			Count: freq.EnglishFrequency + freq.FrenchFrequency,
		})
	}

	slices.SortFunc(top, func(a, b inner.WordCount) int {
		if byCount := cmp.Compare(b.Count, a.Count); byCount != 0 {
			return byCount
		}
		return cmp.Compare(a.Word, b.Word)
	})

	if len(top) > fc.topSize {
		top = top[:fc.topSize]
	}

	return top
}
