package image_fetcher

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/image_fetcher/config"
)

type ImageFetcher struct {
	resultQueue     *common.PacketQueue[inner.QueryResult]
	candidatesQueue *common.PacketQueue[inner.Metadata]
	imageCache      *ImageCache
	running         atomic.Bool
	userAgent       string
	httpClient      *http.Client
}

func NewImageFetcher(cfg *config.Config) (*ImageFetcher, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	candidatesQueue, err := common.NewPacketQueue[inner.Metadata]("imageCandidates", connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult]("queryResult", connSettings)
	if err != nil {
		candidatesQueue.Close()
		return nil, err
	}

	f := &ImageFetcher{
		resultQueue:     resultQueue,
		candidatesQueue: candidatesQueue,
		imageCache:      NewImageCache(cfg.ImageCacheSize),
		userAgent:       cfg.UserAgent,
		httpClient:      http.DefaultClient,
	}
	f.running.Store(true)
	return f, nil
}

func (f *ImageFetcher) Run() error {
	go f.handleSignals()

	err := errors.Join(
		f.candidatesQueue.StartConsuming(func(packet external.Packet[inner.Metadata], ack, nack func()) {
			f.handleCandidatePacket(packet, ack, nack)
		}),
		f.closeMiddlewares(),
	)

	if f.running.Load() {
		return err
	}

	return nil
}

func (f *ImageFetcher) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	f.running.Store(false)
	_ = f.candidatesQueue.StopConsuming()
}

func (f *ImageFetcher) closeMiddlewares() error {
	return errors.Join(
		f.candidatesQueue.StopConsuming(),
		f.candidatesQueue.Close(),
		f.resultQueue.Close(),
	)
}

func (f *ImageFetcher) handleCandidatePacket(packet external.Packet[inner.Metadata], ack func(), nack func()) {
	if packet.IsEof {
		if err := f.handleEOFPacket(packet); err != nil {
			slog.Error("While handling EOF packet from Image Fetcher", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := f.handleCandidate(packet.ClientID, packet.Payload); err != nil {
		slog.Error("While handling image candidate", "err", err)
		nack()
		return
	}
	ack()
}

func (f *ImageFetcher) handleCandidate(clientID clientid.ID, metadata inner.Metadata) error {
	if metadata.ImageUrl == "" {
		slog.Debug("Ignoring article because image url is empty")
		return nil
	}
	if image, ok := f.imageCache.Get(metadata.ImageUrl); ok {
		return f.sendImageResult(clientID, metadata, image)
	}
	image, err := f.fetchImage(metadata.ImageUrl)
	if err != nil {
		return err
	}
	f.imageCache.Put(metadata.ImageUrl, image)
	return f.resultQueue.SendData(clientID, inner.QueryResult{
		QueryType: 2,
		Name:      metadata.Name,
		Url:       metadata.Url,
		Image:     image,
	})
}

func (f *ImageFetcher) sendImageResult(clientID clientid.ID, metadata inner.Metadata, image []byte) error {
	return f.resultQueue.SendData(clientID, inner.QueryResult{
		QueryType: 2,
		Name:      metadata.Name,
		Url:       metadata.Url,
		Image:     image,
	})
}

func (f *ImageFetcher) fetchImage(url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "image/*")
	response, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code from image: %d", response.StatusCode)
	}
	imgBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read image body: %w", err)
	}
	return imgBytes, nil
}
