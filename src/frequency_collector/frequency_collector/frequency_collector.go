package frequency_collector

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
	"github.com/Na4chO/tp-escalabilidad/frequency_collector/config"
)

type FrequencyCollector struct {
	topSize         int
	wordsQueue      *common.PacketQueue[inner.AbstractWords]
	partialTopQueue *common.PacketQueue[inner.PartialTop]
	resultQueue     *common.PacketQueue[inner.QueryResult]
	partialTopSync  *synchronizer.Synchronizer
	resultsSync     *synchronizer.Synchronizer

	mu           sync.Mutex
	clientStates map[clientid.ID]*clientState
	finished     map[clientid.ID]map[syncTarget]bool
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
		wordsQueue:      wordsQueue,
		partialTopQueue: partialTopQueue,
		resultQueue:     resultQueue,
		clientStates:    make(map[clientid.ID]*clientState),
		finished:        make(map[clientid.ID]map[syncTarget]bool),
	}

	instanceID, err := os.Hostname()
	if err != nil {
		_ = fc.Close()
		return nil, fmt.Errorf("getting instance id: %w", err)
	}

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
	go func() { errs <- fc.wordsQueue.StartConsuming(fc.handleAbstractWordsPacket) }()
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
	if fc.wordsQueue != nil {
		_ = fc.wordsQueue.StopConsuming()
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
	if fc.wordsQueue != nil {
		errs = append(errs, fc.wordsQueue.Close())
	}
	if fc.partialTopQueue != nil {
		errs = append(errs, fc.partialTopQueue.Close())
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

func (fc *FrequencyCollector) handleAbstractWords(packet external.Packet[inner.AbstractWords]) error {
	intersections := fc.updateFrequencies(packet.ClientID, packet.Payload)

	if len(intersections) > 0 {
		if err := fc.sendIntersections(packet.ClientID, intersections); err != nil {
			return err
		}
		fc.resultsSync.Sent(packet.ClientID)
	}

	return fc.markProcessed(packet.ClientID)
}

func (fc *FrequencyCollector) handleEOFPacket(packet external.Packet[inner.AbstractWords]) error {
	return errors.Join(
		fc.partialTopSync.EndOfData(packet.ClientID, packet.Total),
		fc.resultsSync.EndOfData(packet.ClientID, packet.Total),
	)
}
