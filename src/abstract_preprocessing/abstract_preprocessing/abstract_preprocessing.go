package abstract_preprocessing

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/abstract_preprocessing/config"
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
)

type AbstractPreprocessing struct {
	inputQueue    *common.PacketQueue[inner.Abstract]
	wordsQueue    *common.PacketQueue[inner.AbstractWords]
	until2020     middleware.Router
	from2021      *common.PacketQueue[inner.AbstractWordCount]
	wordsSync     *synchronizer.Synchronizer
	from2021Sync  *synchronizer.Synchronizer
	until2020Sync *synchronizer.Synchronizer

	mu       sync.Mutex
	finished map[clientid.ID]map[syncTarget]bool
}

func NewAbstractPreprocessing(cfg *config.Config) (*AbstractPreprocessing, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	inputQueue, err := common.NewPacketQueue[inner.Abstract](common.QueueAbstract, connSettings)
	if err != nil {
		return nil, err
	}

	wordsQueue, err := common.NewPacketQueue[inner.AbstractWords](common.QueueAbstractWords, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	until2020, err := middleware.CreateExchangeMiddleware(common.ExchangeAbstractsUntil2020, nil, connSettings)
	if err != nil {
		inputQueue.Close()
		wordsQueue.Close()
		return nil, err
	}

	from2021, err := common.NewPacketQueue[inner.AbstractWordCount](common.QueueAbstractsFrom2021, connSettings)
	if err != nil {
		inputQueue.Close()
		wordsQueue.Close()
		until2020.Close()
		return nil, err
	}

	p := &AbstractPreprocessing{
		inputQueue: inputQueue,
		wordsQueue: wordsQueue,
		until2020:  until2020,
		from2021:   from2021,
		finished:   make(map[clientid.ID]map[syncTarget]bool),
	}

	instanceID, err := os.Hostname()
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("getting instance id: %w", err)
	}

	p.wordsSync, err = p.createSynchronizer(instanceID, wordsSyncExchange, syncWords, connSettings)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("creating words synchronizer: %w", err)
	}

	p.from2021Sync, err = p.createSynchronizer(instanceID, from2021SyncExchange, syncFrom2021, connSettings)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("creating abstracts from 2021 synchronizer: %w", err)
	}

	p.until2020Sync, err = p.createSynchronizer(instanceID, until2020SyncExchange, syncUntil2020, connSettings)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("creating abstracts until 2020 synchronizer: %w", err)
	}

	return p, nil
}

func (p *AbstractPreprocessing) createSynchronizer(instanceID, exchangeName string, target syncTarget, connSettings middleware.ConnSettings) (*synchronizer.Synchronizer, error) {
	return synchronizer.NewSynchronizer(
		instanceID,
		exchangeName,
		connSettings,
		func(clientID clientid.ID) { p.onSynced(clientID, target) },
		func(clientID clientid.ID, total uint32) { p.onFinished(clientID, target, total) },
	)
}

func (p *AbstractPreprocessing) Run() error {
	go p.handleSignals()

	errs := make(chan error, 4)
	go func() { errs <- p.inputQueue.StartConsuming(p.handlePacket) }()
	go func() { errs <- p.wordsSync.StartConsuming() }()
	go func() { errs <- p.from2021Sync.StartConsuming() }()
	go func() { errs <- p.until2020Sync.StartConsuming() }()

	var firstErr error
	for range 4 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
			p.Stop()
		}
	}

	return errors.Join(firstErr, p.Close())
}

func (p *AbstractPreprocessing) Stop() {
	if p.inputQueue != nil {
		_ = p.inputQueue.StopConsuming()
	}
	if p.wordsSync != nil {
		_ = p.wordsSync.StopConsuming()
	}
	if p.from2021Sync != nil {
		_ = p.from2021Sync.StopConsuming()
	}
	if p.until2020Sync != nil {
		_ = p.until2020Sync.StopConsuming()
	}
}

func (p *AbstractPreprocessing) Close() error {
	var errs []error
	if p.inputQueue != nil {
		errs = append(errs, p.inputQueue.Close())
	}
	if p.wordsQueue != nil {
		errs = append(errs, p.wordsQueue.Close())
	}
	if p.until2020 != nil {
		errs = append(errs, p.until2020.Close())
	}
	if p.from2021 != nil {
		errs = append(errs, p.from2021.Close())
	}
	if p.wordsSync != nil {
		errs = append(errs, p.wordsSync.Close())
	}
	if p.from2021Sync != nil {
		errs = append(errs, p.from2021Sync.Close())
	}
	if p.until2020Sync != nil {
		errs = append(errs, p.until2020Sync.Close())
	}
	return errors.Join(errs...)
}

func (p *AbstractPreprocessing) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	p.Stop()
}

func (p *AbstractPreprocessing) handlePacket(packet external.Packet[inner.Abstract], ack func(), nack func()) {
	var err error
	if packet.IsEof {
		err = p.handleEOF(packet)
	} else {
		err = p.handleAbstract(packet)
	}

	if err != nil {
		slog.Error("While handling abstract packet", "client", packet.ClientID, "isEOF", packet.IsEof, "err", err)
		nack()
		return
	}
	ack()
}

func (p *AbstractPreprocessing) handleAbstract(packet external.Packet[inner.Abstract]) error {
	words := extractWords(packet.Payload.Abstract)

	if err := p.wordsQueue.Send(external.Packet[inner.AbstractWords]{
		ClientID: packet.ClientID,
		Payload: inner.AbstractWords{
			Words:      words,
			InLanguage: packet.Payload.InLanguage,
		},
	}); err != nil {
		return fmt.Errorf("sending abstract words: %w", err)
	}
	p.wordsSync.Sent(packet.ClientID)

	if err := p.handleWordCount(packet, uint32(len(words))); err != nil {
		return err
	}

	return p.markProcessed(packet.ClientID)
}

func (p *AbstractPreprocessing) handleWordCount(packet external.Packet[inner.Abstract], count uint32) error {
	date, ok := parseDateModified(packet.Payload.DateModified)
	if !ok {
		slog.Warn("Invalid date modified", "client", packet.ClientID, "date", packet.Payload.DateModified)
		return nil
	}

	wordCount := inner.AbstractWordCount{
		Url:   packet.Payload.Url,
		Count: count,
	}

	if date.Year() <= 2020 {
		if err := p.sendUntil2020(external.Packet[inner.AbstractWordCount]{
			ClientID: packet.ClientID,
			Payload:  wordCount,
		}); err != nil {
			return fmt.Errorf("sending word count to abstracts until 2020: %w", err)
		}
		p.until2020Sync.Sent(packet.ClientID)
		return nil
	}

	if err := p.from2021.Send(external.Packet[inner.AbstractWordCount]{
		ClientID: packet.ClientID,
		Payload:  wordCount,
	}); err != nil {
		return fmt.Errorf("sending word count to abstracts from 2021: %w", err)
	}
	p.from2021Sync.Sent(packet.ClientID)
	return nil
}

func (p *AbstractPreprocessing) handleEOF(packet external.Packet[inner.Abstract]) error {
	return errors.Join(
		p.wordsSync.EndOfData(packet.ClientID, packet.Total),
		p.from2021Sync.EndOfData(packet.ClientID, packet.Total),
		p.until2020Sync.EndOfData(packet.ClientID, packet.Total),
	)
}

func (p *AbstractPreprocessing) sendUntil2020(packet external.Packet[inner.AbstractWordCount]) error {
	data, err := packet.Encode()
	if err != nil {
		return err
	}
	return p.until2020.SendTo(
		fmt.Sprintf("%s-%d", packet.ClientID.Server, packet.ClientID.Seq),
		middleware.Message{Body: string(data)},
	)
}

func parseDateModified(value string) (time.Time, bool) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}
