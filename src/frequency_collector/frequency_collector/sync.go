package frequency_collector

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

const (
	partialTopsSyncExchange = "frequency_collector_partial_tops_sync"
	resultsSyncExchange     = "frequency_collector_results_sync"
)

type syncTarget uint8

const (
	syncPartialTops syncTarget = iota
	syncResults
)

func (t syncTarget) String() string {
	switch t {
	case syncPartialTops:
		return "partialTops"
	case syncResults:
		return "queryResult"
	default:
		return "unknown"
	}
}

func (fc *FrequencyCollector) markProcessed(clientID clientid.ID, count uint32) error {
	return errors.Join(
		fc.partialTopSync.Processed(clientID, count),
		fc.resultsSync.Processed(clientID, count),
	)
}

func (fc *FrequencyCollector) onSynced(clientID clientid.ID, target syncTarget) {
	if !fc.markFinished(clientID, target) {
		return
	}

	switch target {
	case syncPartialTops:
		fc.flushPartialTop(clientID)
	case syncResults:
		if err := fc.resultQueue.Flush(clientID); err != nil {
			slog.Error("While flushing results", "client", clientID, "err", err)
		}
		if err := fc.resultsSync.Finished(clientID); err != nil {
			slog.Error("While sending finished", "client", clientID, "output", target, "err", err)
		}
	}

	fc.clearFinishedIfDone(clientID)
}

func (fc *FrequencyCollector) flushPartialTop(clientID clientid.ID) {
	fc.mu.Lock()
	state := fc.clientStates[clientID]
	delete(fc.clientStates, clientID)
	var top []inner.WordCount
	if state != nil {
		top = fc.topWords(state)
	}
	fc.mu.Unlock()

	if len(top) > 0 {
		if err := fc.sendPartialTop(clientID, external.Packet[inner.PartialTop]{
			ClientID: clientID,
			Count:    1,
			Items:    []inner.PartialTop{{Top: top}},
		}); err != nil {
			slog.Error("While sending partial top", "client", clientID, "err", err)
		} else {
			fc.partialTopSync.Sent(clientID)
		}
	}

	if err := fc.partialTopSync.Finished(clientID); err != nil {
		slog.Error("While sending finished", "client", clientID, "output", syncPartialTops, "err", err)
	}
}

func (fc *FrequencyCollector) onFinished(clientID clientid.ID, target syncTarget, total uint32) {
	var err error
	switch target {
	case syncPartialTops:
		err = fc.sendPartialTop(clientID, external.Packet[inner.PartialTop]{
			ClientID: clientID,
			IsEof:    true,
			Total:    total,
		})
	case syncResults:
		err = fc.resultQueue.Send(external.Packet[inner.QueryResult]{
			ClientID: clientID,
			IsEof:    true,
			Total:    total,
			Payload:  inner.QueryResult{QueryType: inner.QueryIntersections},
		})
	default:
		err = fmt.Errorf("unknown sync target: %s", target)
	}

	if err != nil {
		slog.Error("While sending output EOF", "client", clientID, "output", target, "total", total, "err", err)
		return
	}

	slog.Info("Client output finished", "client", clientID, "output", target, "total", total)
}

func (fc *FrequencyCollector) sendPartialTop(clientID clientid.ID, packet external.Packet[inner.PartialTop]) error {
	data, err := packet.Encode()
	if err != nil {
		return err
	}

	partition := common.ClientPartition(clientID, fc.topAggregatorAmount)
	key := common.PartitionKey(common.ExchangePartialTops, partition)
	return fc.partialTopsExchange.SendTo(key, middleware.Message{Body: string(data)})
}

func (fc *FrequencyCollector) markFinished(clientID clientid.ID, target syncTarget) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	targets := fc.finished[clientID]
	if targets == nil {
		targets = make(map[syncTarget]bool)
		fc.finished[clientID] = targets
	}
	if targets[target] {
		return false
	}

	targets[target] = true
	return true
}

func (fc *FrequencyCollector) clearFinishedIfDone(clientID clientid.ID) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	targets := fc.finished[clientID]
	if targets[syncPartialTops] && targets[syncResults] {
		delete(fc.finished, clientID)
	}
}
