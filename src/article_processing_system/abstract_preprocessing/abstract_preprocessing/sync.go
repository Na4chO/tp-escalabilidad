package abstractpreprocessing

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
)

const (
	wordsSyncExchange     = "abstract_preprocessing_words_sync"
	from2021SyncExchange  = "abstract_preprocessing_from_2021_sync"
	until2020SyncExchange = "abstract_preprocessing_until_2020_sync"
)

type syncTarget uint8

const (
	syncWords syncTarget = iota
	syncFrom2021
	syncUntil2020
)

func (t syncTarget) String() string {
	switch t {
	case syncWords:
		return "abstractWords"
	case syncFrom2021:
		return "abstracts_from_2021"
	case syncUntil2020:
		return "abstracts_until_2020"
	default:
		return "unknown"
	}
}

func (p *AbstractPreprocessing) markProcessed(clientID clientid.ID, count uint32) error {
	return errors.Join(
		p.wordsSync.Processed(clientID, count),
		p.from2021Sync.Processed(clientID, count),
		p.until2020Sync.Processed(clientID, count),
	)
}

func (p *AbstractPreprocessing) onSynced(clientID clientid.ID, target syncTarget) {
	if !p.markFinished(clientID, target) {
		return
	}

	syncer := p.synchronizerFor(target)
	if syncer == nil {
		return
	}

	if err := p.flush(clientID, target); err != nil {
		slog.Error("While flushing", "client", clientID, "output", target, "err", err)
	}
	if err := syncer.Finished(clientID); err != nil {
		slog.Error("While sending finished", "client", clientID, "output", target, "err", err)
	}

	p.clearFinishedIfDone(clientID)
}

func (p *AbstractPreprocessing) onFinished(clientID clientid.ID, target syncTarget, total uint32) {
	if err := p.sendEOF(clientID, target, total); err != nil {
		slog.Error("While sending output EOF", "client", clientID, "output", target, "total", total, "err", err)
		return
	}

	slog.Info("Client output finished", "client", clientID, "output", target, "total", total)
}

func (p *AbstractPreprocessing) sendEOF(clientID clientid.ID, target syncTarget, total uint32) error {
	switch target {
	case syncWords:
		return p.sendAbstractWordsEOF(clientID, total)
	case syncFrom2021:
		return p.from2021.Send(external.Packet[inner.AbstractWordCount]{
			ClientID: clientID,
			IsEof:    true,
			Total:    total,
		})
	case syncUntil2020:
		return p.sendUntil2020EOF(clientID, total)
	default:
		return fmt.Errorf("unknown sync target: %s", target)
	}
}

func (p *AbstractPreprocessing) flush(clientID clientid.ID, target syncTarget) error {
	switch target {
	case syncWords:
		return p.words.Flush(clientID)
	case syncFrom2021:
		return p.from2021.Flush(clientID)
	case syncUntil2020:
		return p.until2020Counts.Flush(clientID)
	default:
		return fmt.Errorf("unknown sync target: %s", target)
	}
}

func (p *AbstractPreprocessing) sendAbstractWordsEOF(clientID clientid.ID, total uint32) error {
	partition := common.ClientPartition(clientID, p.frequencyCollectorAmount)
	key := common.PartitionKey(common.ExchangeAbstractWords, partition)
	data, err := external.Packet[inner.AbstractWords]{
		ClientID: clientID,
		IsEof:    true,
		Total:    total,
	}.Encode()
	if err != nil {
		return err
	}

	return p.wordsExchange.SendTo(key, middleware.Message{Body: string(data)})
}

func (p *AbstractPreprocessing) synchronizerFor(target syncTarget) *synchronizer.Synchronizer {
	switch target {
	case syncWords:
		return p.wordsSync
	case syncFrom2021:
		return p.from2021Sync
	case syncUntil2020:
		return p.until2020Sync
	default:
		return nil
	}
}

func (p *AbstractPreprocessing) markFinished(clientID clientid.ID, target syncTarget) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	targets := p.finished[clientID]
	if targets == nil {
		targets = make(map[syncTarget]bool)
		p.finished[clientID] = targets
	}
	if targets[target] {
		return false
	}

	targets[target] = true
	return true
}

func (p *AbstractPreprocessing) clearFinishedIfDone(clientID clientid.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	targets := p.finished[clientID]
	if targets[syncWords] && targets[syncFrom2021] && targets[syncUntil2020] {
		delete(p.finished, clientID)
	}
}
