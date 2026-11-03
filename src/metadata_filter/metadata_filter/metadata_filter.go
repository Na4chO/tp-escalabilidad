package metadata_filter

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/metadata_filter/config"
)

type sectionInfo struct {
	Name string `json:"name"`
}

type MetadataFilter struct {
	metadataQueue        *common.PacketQueue[inner.Metadata]
	imageCandidatesQueue *common.PacketQueue[inner.Metadata]
	resultQueue          *common.PacketQueue[inner.QueryResult]
	running              atomic.Bool
}

func NewMetadataFilter(cfg *config.Config) (*MetadataFilter, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	metadataQueue, err := common.NewPacketQueue[inner.Metadata]("metadata", connSettings)
	if err != nil {
		return nil, err
	}

	imageCandidatesQueue, err := common.NewPacketQueue[inner.Metadata]("imageCandidates", connSettings)
	if err != nil {
		metadataQueue.Close()
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("queryResult", connSettings)
	if err != nil {
		metadataQueue.Close()
		imageCandidatesQueue.Close()
		return nil, err
	}

	mf := &MetadataFilter{
		metadataQueue:        metadataQueue,
		imageCandidatesQueue: imageCandidatesQueue,
		resultQueue:          resultQueue,
	}
	mf.running.Store(true)
	return mf, nil
}

func (mf *MetadataFilter) Run() error {
	go mf.handleSignals()

	err := errors.Join(
		mf.metadataQueue.StartConsuming(func(packet external.Packet[inner.Metadata], ack, nack func()) {
			mf.handleMetadataPacket(packet, ack, nack)
		}),
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
		mf.imageCandidatesQueue.Close(),
		mf.resultQueue.Close(),
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

func (mf *MetadataFilter) handleMetadata(clientID uint32, metadata inner.Metadata) error {
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
	}
	if hasBiographySection(metadata.Sections) {
		if err := mf.imageCandidatesQueue.SendData(clientID, metadata); err != nil {
			return err
		}
	}
	return nil
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
