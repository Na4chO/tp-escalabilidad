package frequency_collector

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
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
	wordsFreq map[string]*idiomsFrequency
}

type FrequencyCollector struct {
	running         atomic.Bool
	clientStates    map[uint32]*clientState
	wordsQueue      *common.PacketQueue[inner.AbstractWords]
	partialTopQueue *common.PacketQueue[inner.PartialTop]
	resultQueue     *common.PacketQueue[inner.QueryResult]
}

func NewFrequencyCollector(cfg *config.Config) (*FrequencyCollector, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	wordsQueue, err := common.NewPacketQueue[inner.AbstractWords]("abstractWords", connSettings)
	if err != nil {
		return nil, err
	}

	partialTopQueue, err := common.NewPacketQueue[inner.PartialTop]("partialTops", connSettings)
	if err != nil {
		wordsQueue.Close()
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("result", connSettings)
	if err != nil {
		partialTopQueue.Close()
		wordsQueue.Close()
		return nil, err
	}

	fc := &FrequencyCollector{wordsQueue: wordsQueue, partialTopQueue: partialTopQueue, resultQueue: resultQueue}
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
		}
		ack()
		return
	}

	if err := fc.handleAbstractWords(packet); err != nil {
		slog.Error("While handling abstract words message", "err", err)
		nack()
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

func (fc *FrequencyCollector) clientStateFor(clientId uint32) *clientState {
	state, ok := fc.clientStates[clientId]
	if !ok {
		fc.clientStates[clientId] = &clientState{wordsFreq: make(map[string]*idiomsFrequency)}
	}
	return state
}

func (fc *FrequencyCollector) handleAbstractWords(packet external.Packet[inner.AbstractWords]) error {
	var intersections []string
	state := fc.clientStateFor(packet.ClientID)

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
	return fc.sendIntersections(packet.ClientID, intersections)
}

func isIntersection(in_language string, wordFreq *idiomsFrequency) bool {
	return (in_language == "en" && wordFreq.EnglishFrequency == 0 && wordFreq.FrenchFrequency > 0) || (in_language == "en" && wordFreq.FrenchFrequency == 0 && wordFreq.EnglishFrequency > 0)
}

func (fc *FrequencyCollector) sendIntersections(clientId uint32, intersections []string) error {
	return fc.resultQueue.Send(external.Packet[inner.QueryResult]{
		ClientID: clientId,
		Payload:  inner.QueryResult{IntersectionWords: intersections},
	})
}
