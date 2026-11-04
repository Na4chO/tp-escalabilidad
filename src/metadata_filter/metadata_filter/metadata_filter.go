package metadata_filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
	"github.com/Na4chO/tp-escalabilidad/metadata_filter/config"
)

type sectionInfo struct {
	Name string `json:"name"`
}

type MetadataFilter struct {
	metadataQueue     *common.PacketQueue[inner.Metadata]
	imageCandidatesEx middleware.Router
	resultQueue       *common.PacketQueue[inner.QueryResult]
	running           atomic.Bool
	resultsSync       *synchronizer.Synchronizer
	imageSync         *synchronizer.Synchronizer
	mu                sync.Mutex
	finished          map[clientid.ID]map[syncTarget]bool
	imgFetcherCount   int
	sentByPartition   map[clientid.ID]map[int]uint32
}

func NewMetadataFilter(cfg *config.Config) (*MetadataFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	metadataQueue, err := common.NewPacketQueue[inner.Metadata]("metadata", connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("queryResult", connSettings)
	if err != nil {
		metadataQueue.Close()
		return nil, err
	}

	imageCandidatesEx, err := middleware.CreateExchangeMiddleware(common.ExchangeImageCandidates, nil, connSettings)
	if err != nil {
		metadataQueue.Close()
		resultQueue.Close()
		return nil, err
	}

	mf := &MetadataFilter{
		metadataQueue:     metadataQueue,
		imageCandidatesEx: imageCandidatesEx,
		resultQueue:       resultQueue,
		finished:          make(map[clientid.ID]map[syncTarget]bool),
		imgFetcherCount:   cfg.ImageFetcherCount,
		sentByPartition:   make(map[clientid.ID]map[int]uint32),
	}
	mf.running.Store(true)

	mf.resultsSync, err = mf.createSynchronizer(cfg.InstanceID, resultsSyncExchange, syncResults, connSettings)
	if err != nil {
		mf.closeMiddlewares()
		return nil, err
	}
	mf.imageSync, err = mf.createSynchronizer(cfg.InstanceID, imageSyncExchange, syncImages, connSettings)
	if err != nil {
		mf.closeMiddlewares()
		mf.resultsSync.Close()
		return nil, err
	}
	return mf, nil
}

func (mf *MetadataFilter) createSynchronizer(instanceID, exchangeName string, target syncTarget, connSettings middleware.ConnSettings) (*synchronizer.Synchronizer, error) {
	return synchronizer.NewSynchronizer(
		instanceID,
		exchangeName,
		connSettings,
		func(clientID clientid.ID) { mf.onSynced(clientID, target) },
		func(clientID clientid.ID, total uint32) { mf.onFinished(clientID, target, total) },
	)
}

func (mf *MetadataFilter) Run() error {
	go mf.handleSignals()

	err := errors.Join(
		mf.metadataQueue.StartConsuming(func(packet external.Packet[inner.Metadata], ack, nack func()) {
			mf.handleMetadataPacket(packet, ack, nack)
		}),
		mf.imageSync.StartConsuming(),
		mf.resultsSync.StartConsuming(),
		mf.closeMiddlewares(),
	)

	if mf.running.Load() {
		return err
	}

	return nil
}

func (mf *MetadataFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	mf.running.Store(false)
	_ = mf.metadataQueue.StopConsuming()
}

func (mf *MetadataFilter) closeMiddlewares() error {
	return errors.Join(
		mf.metadataQueue.StopConsuming(),
		mf.metadataQueue.Close(),
		mf.imageCandidatesEx.Close(),
		mf.resultQueue.Close(),
		mf.resultsSync.Close(),
		mf.imageSync.Close(),
	)
}

func (mf *MetadataFilter) handleMetadataPacket(packet external.Packet[inner.Metadata], ack func(), nack func()) {
	if packet.IsEof {
		if err := mf.handleEOFPacket(packet); err != nil {
			slog.Error("While handling EOF packet from Metadata Filter", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := mf.handleMetadata(packet.ClientID, packet.Payload); err != nil {
		slog.Error("While handling metadata", "err", err)
		nack()
		return
	}
	ack()
}

func (mf *MetadataFilter) handleMetadata(clientID clientid.ID, metadata inner.Metadata) error {
	mf.markProcessed(clientID)
	date, ok := parseDateModified(metadata.DateModified)
	if !ok {
		slog.Warn("Invalid date modified", "date", metadata.DateModified)
		return nil
	}
	if isAfter2020(date) {
		return nil
	}
	if metadata.InLanguage == "en" {
		result := inner.QueryResult{
			QueryType:    1,
			Url:          metadata.Url,
			DateModified: date.Format(time.RFC3339),
		}
		if err := mf.resultQueue.SendData(clientID, result); err != nil {
			return err
		}
		mf.resultsSync.Sent(clientID)
	}
	if hasBiographySection(metadata.Sections) {
		part := mf.imagePartition(metadata.ImageUrl)
		key := fmt.Sprintf("img-%d", part)

		packet := external.Packet[inner.Metadata]{
			ClientID: clientID,
			Payload:  metadata,
		}
		data, err := packet.Encode()
		if err != nil {
			return err
		}
		if err := mf.imageCandidatesEx.SendTo(key, middleware.Message{Body: string(data)}); err != nil {
			return err
		}
		mf.registerSentToPartition(clientID, part)
		mf.imageSync.Sent(clientID)
	}
	return nil
}

func (mf *MetadataFilter) handleEOFPacket(packet external.Packet[inner.Metadata]) error {
	return errors.Join(
		mf.resultsSync.EndOfData(packet.ClientID, packet.Total),
		mf.imageSync.EndOfData(packet.ClientID, packet.Total),
	)
}

func (mf *MetadataFilter) imagePartition(url string) int {
	if mf.imgFetcherCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(url))
	return int(h.Sum32()) % mf.imgFetcherCount
}

func (mf *MetadataFilter) registerSentToPartition(clientID clientid.ID, p int) {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	if mf.sentByPartition[clientID] == nil {
		mf.sentByPartition[clientID] = make(map[int]uint32)
	}
	mf.sentByPartition[clientID][p]++
}

func parseDateModified(value string) (time.Time, bool) {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}

func isAfter2020(date time.Time) bool {
	return date.Year() > 2020
}

func hasBiographySection(rawSections string) bool {
	if rawSections == "" {
		return false
	}
	var sections []sectionInfo
	if err := json.Unmarshal([]byte(rawSections), &sections); err != nil {
		return false
	}
	for _, section := range sections {
		name := strings.ToLower(strings.TrimSpace(section.Name))
		if name == "biography" || name == "biographie" {
			return true
		}
	}
	return false
}
