package image_fetcher

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
	"github.com/Na4chO/tp-escalabilidad/image_fetcher/config"
)

const (
	syncExchange      = "image_fetcher_sync"
	fetchAttempts     = 5
	fetchRetryBackoff = time.Second
	maxRetryWait      = time.Minute
)

type ImageFetcher struct {
	resultQueue       *common.PacketQueue[inner.QueryResult]
	imageCandidatesEx middleware.Router
	synchronizer      *synchronizer.Synchronizer
	imageCache        *ImageCache
	running           atomic.Bool
	userAgent         string
	httpClient        *http.Client
	downloadRateMbps  float64
}

func NewImageFetcher(cfg *config.Config) (*ImageFetcher, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	imageCandidatesEx, err := middleware.CreateExchangeMiddleware(common.ExchangeImageCandidates, []string{fmt.Sprintf("img-%d", cfg.Partition)}, connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings, common.ResultsBatchSize)
	if err != nil {
		imageCandidatesEx.Close()
		return nil, err
	}

	f := &ImageFetcher{
		resultQueue:       resultQueue,
		imageCandidatesEx: imageCandidatesEx,
		imageCache:        NewImageCache(cfg.ImageCacheSize),
		userAgent:         cfg.UserAgent,
		downloadRateMbps:  cfg.DownloadRateMbps,
		httpClient:        &http.Client{Timeout: 30 * time.Second},
	}

	f.synchronizer, err = synchronizer.NewSynchronizer(strconv.Itoa(cfg.Partition), syncExchange, connSettings, f.onSynced, f.onFinished)
	if err != nil {
		imageCandidatesEx.Close()
		resultQueue.Close()
		return nil, err
	}

	f.running.Store(true)
	return f, nil
}

func (f *ImageFetcher) Run() error {
	go f.handleSignals()

	errs := make(chan error, 2)
	go func() { errs <- f.imageCandidatesEx.StartConsuming(f.handleImageCandidatesMessage) }()
	go func() { errs <- f.synchronizer.StartConsuming() }()

	var firstErr error
	for range 2 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
		f.stop()
	}
	err := errors.Join(firstErr, f.closeMiddlewares())

	if f.running.Load() {
		return err
	}
	return nil
}

func (f *ImageFetcher) stop() {
	f.imageCandidatesEx.StopConsuming()
	f.synchronizer.StopConsuming()
}

func (f *ImageFetcher) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	f.running.Store(false)
	f.stop()
}

func (f *ImageFetcher) closeMiddlewares() error {
	return errors.Join(
		f.imageCandidatesEx.Close(),
		f.synchronizer.Close(),
		f.resultQueue.Close(),
	)
}

func (f *ImageFetcher) handleImageCandidatesMessage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.ImageCandidateMetadata]([]byte(msg.Body))
	if err != nil {
		slog.Error("While decoding image candidate packet", "err", err)
		nack()
		return
	}

	if packet.IsEof {
		err = f.synchronizer.EndOfData(packet.ClientID, packet.Total)
	} else {
		err = f.handleCandidates(packet.ClientID, packet.Items)
	}
	if err != nil {
		slog.Error("While handling image candidate", "client", packet.ClientID, "isEOF", packet.IsEof, "err", err)
		nack()
		return
	}
	ack()
}

func (f *ImageFetcher) handleCandidates(clientID clientid.ID, batch []inner.ImageCandidateMetadata) (err error) {
	defer func() { err = errors.Join(err, f.synchronizer.Processed(clientID, uint32(len(batch)))) }()
	for _, candidate := range batch {
		err = errors.Join(err, f.handleCandidate(clientID, candidate))
	}
	return err
}

func (f *ImageFetcher) handleCandidate(clientID clientid.ID, metadata inner.ImageCandidateMetadata) (err error) {
	if metadata.ImageUrl == "" {
		return nil
	}

	image, ok := f.imageCache.Get(metadata.ImageUrl)
	if !ok {
		if image, err = f.fetchImage(metadata.ImageUrl); err != nil {
			slog.Warn("Skipping article, could not fetch image", "url", metadata.Url, "image", metadata.ImageUrl, "err", err)
			return nil
		}
		f.imageCache.Put(metadata.ImageUrl, image)
	}

	return f.sendResult(clientID, metadata, image)
}

func (f *ImageFetcher) sendResult(clientID clientid.ID, metadata inner.ImageCandidateMetadata, image []byte) error {
	if err := f.resultQueue.SendData(clientID, inner.QueryResult{
		QueryType: inner.QueryImage,
		Name:      metadata.Name,
		Url:       metadata.Url,
		Image:     image,
	}); err != nil {
		return err
	}
	f.synchronizer.Sent(clientID)
	return nil
}

func (f *ImageFetcher) onSynced(clientID clientid.ID) {
	if err := f.resultQueue.Flush(clientID); err != nil {
		slog.Error("While flushing results", "client", clientID, "err", err)
	}
	if err := f.synchronizer.Finished(clientID); err != nil {
		slog.Error("While sending finished", "client", clientID, "err", err)
	}
}

func (f *ImageFetcher) onFinished(clientID clientid.ID, total uint32) {
	err := f.resultQueue.Send(external.Packet[inner.QueryResult]{
		ClientID: clientID,
		IsEof:    true,
		Total:    total,
		Payload:  inner.QueryResult{QueryType: inner.QueryImage},
	})
	if err != nil {
		slog.Error("While sending results EOF", "client", clientID, "err", err)
		return
	}
	slog.Info("Client finished", "client", clientID, "total", total)
}

func (f *ImageFetcher) fetchImage(url string) ([]byte, error) {
	backoff := fetchRetryBackoff
	for attempt := 1; ; attempt++ {
		image, retryAfter, err := f.requestImage(url)
		if err == nil || retryAfter < 0 || attempt == fetchAttempts {
			return image, err
		}
		if retryAfter == 0 {
			retryAfter = backoff
			backoff *= 2
		}
		if retryAfter > maxRetryWait {
			return nil, fmt.Errorf("%w, retry after %s", err, retryAfter)
		}
		time.Sleep(retryAfter)
	}
}

func (f *ImageFetcher) requestImage(url string) ([]byte, time.Duration, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, -1, err
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "image/*")
	response, err := f.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests {
		seconds, _ := strconv.Atoi(response.Header.Get("Retry-After"))
		return nil, time.Duration(seconds) * time.Second, fmt.Errorf("rate limited fetching image")
	}
	if response.StatusCode != http.StatusOK {
		return nil, -1, fmt.Errorf("unexpected status code from image: %d", response.StatusCode)
	}
	imgBytes, err := io.ReadAll(newThrottledReader(response.Body, f.downloadRateMbps))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read image body: %w", err)
	}
	return imgBytes, -1, nil
}
