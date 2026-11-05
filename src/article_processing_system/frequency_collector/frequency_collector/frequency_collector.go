package frequencycollector

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/article_processing_system/frequency_collector/config"
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
)

type FrequencyCollector struct {
	topSize             int
	topAggregatorAmount int
	wordsExchange       middleware.Router
	partialTopsExchange middleware.Router
	resultQueue         *common.PacketQueue[inner.QueryResult]
	partialTopSync      *synchronizer.Synchronizer
	resultsSync         *synchronizer.Synchronizer

	mu           sync.Mutex
	clientStates map[clientid.ID]*clientState
	finished     map[clientid.ID]map[syncTarget]bool
}

func NewFrequencyCollector(cfg *config.Config) (*FrequencyCollector, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	wordsKey := common.PartitionKey(common.ExchangeAbstractWords, cfg.ID)
	wordsExchange, err := middleware.CreateExchangeMiddleware(common.ExchangeAbstractWords, []string{wordsKey}, connSettings)
	if err != nil {
		return nil, err
	}

	partialTopsExchange, err := middleware.CreateExchangeMiddleware(common.ExchangePartialTops, nil, connSettings)
	if err != nil {
		wordsExchange.Close()
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings, common.ResultsBatchSize)
	if err != nil {
		partialTopsExchange.Close()
		wordsExchange.Close()
		return nil, err
	}

	fc := &FrequencyCollector{
		topSize:             cfg.TopSize,
		topAggregatorAmount: cfg.TopAggregatorAmount,
		wordsExchange:       wordsExchange,
		partialTopsExchange: partialTopsExchange,
		resultQueue:         resultQueue,
		clientStates:        make(map[clientid.ID]*clientState),
		finished:            make(map[clientid.ID]map[syncTarget]bool),
	}

	instanceID := strconv.Itoa(cfg.ID)

	fc.partialTopSync, err = fc.createSynchronizer(instanceID, partialTopsSyncExchange, syncPartialTops, connSettings)
	if err != nil {
		_ = fc.Close()
		return nil, fmt.Errorf("creating partial tops synchronizer: %w", err)
	}

	fc.resultsSync, err = fc.createSynchronizer(instanceID, resultsSyncExchange, syncResults, connSettings)
	if err != nil {
		_ = fc.Close()
		return nil, fmt.Errorf("creating results synchronizer: %w", err)
	}

	return fc, nil
}

func (fc *FrequencyCollector) createSynchronizer(instanceID, exchangeName string, target syncTarget, connSettings middleware.ConnSettings) (*synchronizer.Synchronizer, error) {
	return synchronizer.NewSynchronizer(
		instanceID,
		exchangeName,
		connSettings,
		func(clientID clientid.ID) { fc.onSynced(clientID, target) },
		func(clientID clientid.ID, total uint32) { fc.onFinished(clientID, target, total) },
	)
}

func (fc *FrequencyCollector) Run() error {
	go fc.handleSignals()

	errs := make(chan error, 3)
	go func() { errs <- fc.wordsExchange.StartConsuming(fc.handleAbstractWordsMessage) }()
	go func() { errs <- fc.partialTopSync.StartConsuming() }()
	go func() { errs <- fc.resultsSync.StartConsuming() }()

	var firstErr error
	for range 3 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
			fc.Stop()
		}
	}

	return errors.Join(firstErr, fc.Close())
}

func (fc *FrequencyCollector) Stop() {
	if fc.wordsExchange != nil {
		_ = fc.wordsExchange.StopConsuming()
	}
	if fc.partialTopSync != nil {
		_ = fc.partialTopSync.StopConsuming()
	}
	if fc.resultsSync != nil {
		_ = fc.resultsSync.StopConsuming()
	}
}

func (fc *FrequencyCollector) Close() error {
	var errs []error
	if fc.wordsExchange != nil {
		errs = append(errs, fc.wordsExchange.Close())
	}
	if fc.partialTopsExchange != nil {
		errs = append(errs, fc.partialTopsExchange.Close())
	}
	if fc.resultQueue != nil {
		errs = append(errs, fc.resultQueue.Close())
	}
	if fc.partialTopSync != nil {
		errs = append(errs, fc.partialTopSync.Close())
	}
	if fc.resultsSync != nil {
		errs = append(errs, fc.resultsSync.Close())
	}
	return errors.Join(errs...)
}

func (fc *FrequencyCollector) handleAbstractWordsMessage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.AbstractWords]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding abstract words message", "err", err)
		nack()
		return
	}

	fc.handleAbstractWordsPacket(packet, ack, nack)
}

func (fc *FrequencyCollector) handleAbstractWordsPacket(packet external.Packet[inner.AbstractWords], ack func(), nack func()) {
	var err error
	if packet.IsEof {
		err = fc.handleEOFPacket(packet)
	} else {
		err = fc.handleAbstractWords(packet)
	}

	if err != nil {
		slog.Error("While handling abstract words packet", "client", packet.ClientID, "isEOF", packet.IsEof, "err", err)
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
	fc.Stop()
}

func (fc *FrequencyCollector) handleAbstractWords(packet external.Packet[inner.AbstractWords]) (err error) {
	defer func() { err = errors.Join(err, fc.markProcessed(packet.ClientID, uint32(len(packet.Items)))) }()

	for _, abstractWords := range packet.Items {
		intersections := fc.updateFrequencies(packet.ClientID, abstractWords)
		if len(intersections) == 0 {
			continue
		}
		if sendErr := fc.sendIntersections(packet.ClientID, intersections); sendErr != nil {
			err = errors.Join(err, sendErr)
			continue
		}
		fc.resultsSync.Sent(packet.ClientID)
	}
	return err
}

func (fc *FrequencyCollector) handleEOFPacket(packet external.Packet[inner.AbstractWords]) error {
	return errors.Join(
		fc.partialTopSync.EndOfData(packet.ClientID, packet.Total),
		fc.resultsSync.EndOfData(packet.ClientID, packet.Total),
	)
}
