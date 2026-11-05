package metadatafilter

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

	"github.com/Na4chO/tp-escalabilidad/article_processing_system/metadata_filter/config"
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
)

type sectionInfo struct {
	Name string `json:"name"`
}

type MetadataFilter struct {
	metadataQueue     *common.PacketQueue[inner.Metadata]
	imageCandidatesEx middleware.Router
	imageCandidates   *common.Batcher[inner.ImageCandidateMetadata]
	resultQueue       *common.PacketQueue[inner.QueryResult]
	running           atomic.Bool
	resultsSync       *synchronizer.Synchronizer
	imageSync         *synchronizer.Synchronizer
	mu                sync.Mutex
	finished          map[clientid.ID]map[syncTarget]bool
	imgFetcherCount   int
}

func NewMetadataFilter(cfg *config.Config) (*MetadataFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	metadataQueue, err := common.NewPacketQueue[inner.Metadata]("metadata", connSettings, cfg.BatchSize)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("queryResult", connSettings, common.ResultsBatchSize)
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
		imageCandidates:   common.NewBatcher(cfg.BatchSize, common.ExchangeSender[inner.ImageCandidateMetadata](imageCandidatesEx)),
		resultQueue:       resultQueue,
		finished:          make(map[clientid.ID]map[syncTarget]bool),
		imgFetcherCount:   cfg.ImageFetcherCount,
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

	errs := make(chan error, 3)
	go func() {
		errs <- mf.metadataQueue.StartConsuming(func(packet external.Packet[inner.Metadata], ack, nack func()) {
			mf.handleMetadataPacket(packet, ack, nack)
		})
	}()
	go func() { errs <- mf.imageSync.StartConsuming() }()
	go func() { errs <- mf.resultsSync.StartConsuming() }()

	var firstErr error
	for range 3 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
		mf.stop()
	}
	err := errors.Join(firstErr, mf.closeMiddlewares())

	if mf.running.Load() {
		return err
	}

	return nil
}

func (mf *MetadataFilter) stop() {
	mf.metadataQueue.StopConsuming()
	mf.imageSync.StopConsuming()
	mf.resultsSync.StopConsuming()
}

func (mf *MetadataFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	mf.running.Store(false)
	mf.stop()
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

	if err := mf.handleMetadata(packet.ClientID, packet.Items); err != nil {
		slog.Error("While handling metadata", "err", err)
		nack()
		return
	}
	ack()
}

func (mf *MetadataFilter) handleMetadata(clientID clientid.ID, batch []inner.Metadata) (err error) {
	defer func() { err = errors.Join(err, mf.markProcessed(clientID, uint32(len(batch)))) }()
	for _, metadata := range batch {
		err = errors.Join(err, mf.filterMetadata(clientID, metadata))
	}
	return err
}

func (mf *MetadataFilter) filterMetadata(clientID clientid.ID, metadata inner.Metadata) error {
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
			QueryType:    inner.QueryMetadata,
			Url:          metadata.Url,
			DateModified: date.Format(time.RFC3339),
		}
		if err := mf.resultQueue.SendData(clientID, result); err != nil {
			return err
		}
		mf.resultsSync.Sent(clientID)
	}
	if hasBiographySection(metadata.Sections) {
		candidate := inner.ImageCandidateMetadata{
			Url:      metadata.Url,
			Name:     metadata.Name,
			ImageUrl: metadata.ImageUrl,
		}
		if err := mf.imageCandidates.Add(clientID, imageKey(mf.imagePartition(metadata.ImageUrl)), candidate); err != nil {
			return err
		}
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

func (mf *MetadataFilter) sendImageCandidatesEOF(clientID clientid.ID, total uint32) error {
	data, err := external.Packet[inner.ImageCandidateMetadata]{ClientID: clientID, IsEof: true, Total: total}.Encode()
	if err != nil {
		return err
	}
	key := imageKey(common.ClientPartition(clientID, mf.imgFetcherCount))
	return mf.imageCandidatesEx.SendTo(key, middleware.Message{Body: string(data)})
}

func imageKey(partition int) string {
	return fmt.Sprintf("img-%d", partition)
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
