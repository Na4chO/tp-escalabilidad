package metadata_filter

import (
	"errors"
	"fmt"
	"log/slog"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

const (
	resultsSyncExchange = "metadata_results_sync"
	imageSyncExchange   = "metadata_images_sync"
)

type syncTarget uint8

const (
	syncResults syncTarget = iota
	syncImages
)

func (t syncTarget) String() string {
	switch t {
	case syncImages:
		return "imageCandidates"
	case syncResults:
		return "queryResult"
	default:
		return "unknown"
	}
}

func (mf *MetadataFilter) markProcessed(clientID clientid.ID, count uint32) error {
	return errors.Join(
		mf.resultsSync.Processed(clientID, count),
		mf.imageSync.Processed(clientID, count),
	)
}

func (mf *MetadataFilter) onSynced(clientID clientid.ID, target syncTarget) {
	if !mf.markFinished(clientID, target) {
		return
	}

	switch target {
	case syncImages:
		if err := mf.imageCandidates.Flush(clientID); err != nil {
			slog.Error("While flushing image candidates", "client", clientID, "err", err)
		}
		if err := mf.imageSync.Finished(clientID); err != nil {
			slog.Error("While sending finished for images sync", "client", clientID, "output", target, "err", err)
		}
	case syncResults:
		if err := mf.resultQueue.Flush(clientID); err != nil {
			slog.Error("While flushing results", "client", clientID, "err", err)
		}
		if err := mf.resultsSync.Finished(clientID); err != nil {
			slog.Error("While sending finished for results sync", "client", clientID, "output", target, "err", err)
		}
	}

	mf.clearFinishedIfDone(clientID)
}

func (mf *MetadataFilter) onFinished(clientID clientid.ID, target syncTarget, total uint32) {
	var err error
	switch target {
	case syncImages:
		err = mf.sendImageCandidatesEOF(clientID, total)
	case syncResults:
		err = mf.resultQueue.Send(external.Packet[inner.QueryResult]{
			ClientID: clientID,
			IsEof:    true,
			Total:    total,
			Payload:  inner.QueryResult{QueryType: inner.QueryMetadata},
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

func (mf *MetadataFilter) markFinished(clientID clientid.ID, target syncTarget) bool {
	mf.mu.Lock()
	defer mf.mu.Unlock()

	targets := mf.finished[clientID]
	if targets == nil {
		targets = make(map[syncTarget]bool)
		mf.finished[clientID] = targets
	}
	if targets[target] {
		return false
	}

	targets[target] = true
	return true
}

func (mf *MetadataFilter) clearFinishedIfDone(clientID clientid.ID) {
	mf.mu.Lock()
	defer mf.mu.Unlock()

	targets := mf.finished[clientID]
	if targets[syncResults] && targets[syncImages] {
		delete(mf.finished, clientID)
	}
}
