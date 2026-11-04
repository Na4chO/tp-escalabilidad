package image_fetcher

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
	"github.com/Na4chO/tp-escalabilidad/image_fetcher/config"
)

type clientState struct {
	receivedCount uint32
	expectedTotal uint32
	hasEOF        bool
}

type ImageFetcher struct {
	resultQueue       *common.PacketQueue[inner.QueryResult]
	imageCandidatesEx middleware.Router
	imageCache        *ImageCache
	running           atomic.Bool
	userAgent         string
	httpClient        *http.Client
	mu                sync.Mutex
	clientStates      map[clientid.ID]*clientState
	partition         int
}

func NewImageFetcher(cfg *config.Config) (*ImageFetcher, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}

	imageCandidatesEx, err := middleware.CreateExchangeMiddleware(common.ExchangeImageCandidates, []string{fmt.Sprintf("img-%d", cfg.Partition)}, connSettings)
	if err != nil {
		return nil, err
	}

	resultQueue, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		imageCandidatesEx.Close()
		return nil, err
	}

	f := &ImageFetcher{
		resultQueue:       resultQueue,
		imageCandidatesEx: imageCandidatesEx,
		imageCache:        NewImageCache(cfg.ImageCacheSize),
		userAgent:         cfg.UserAgent,
		httpClient:        http.DefaultClient,
		clientStates:      make(map[clientid.ID]*clientState),
		partition:         cfg.Partition,
	}
	f.running.Store(true)
	return f, nil
}

func (f *ImageFetcher) Run() error {
	go f.handleSignals()

	err := errors.Join(
		f.imageCandidatesEx.StartConsuming(f.handleImageCandidatesMessage),
		f.closeMiddlewares(),
	)

	if f.running.Load() {
		return err
	}
	return nil
}

func (f *ImageFetcher) handleImageCandidatesMessage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.Metadata]([]byte(msg.Body))
	if err != nil {
		slog.Error("While decoding image candidate packet", "err", err)
		nack()
		return
	}
	f.handleCandidatePacket(packet, ack, nack)
}

func (f *ImageFetcher) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	f.running.Store(false)
	_ = f.imageCandidatesEx.StopConsuming()
}

func (f *ImageFetcher) closeMiddlewares() error {
	return errors.Join(
		f.imageCandidatesEx.StopConsuming(),
		f.imageCandidatesEx.Close(),
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
		f.mu.Lock()
		state := f.clientStateFor(clientID)
		state.receivedCount++
		f.mu.Unlock()
		return f.checkAndFinalize(clientID)
	}

	var image []byte
	var err error
	if cached, ok := f.imageCache.Get(metadata.ImageUrl); ok {
		image = cached
	} else {
		image, err = f.fetchImage(metadata.ImageUrl)
		if err != nil {
			f.mu.Lock()
			state := f.clientStateFor(clientID)
			state.receivedCount++
			f.mu.Unlock()
			return f.checkAndFinalize(clientID)
		}
		f.imageCache.Put(metadata.ImageUrl, image)
	}
	if err := f.resultQueue.SendData(clientID, inner.QueryResult{
		QueryType: 2,
		Name:      metadata.Name,
		Url:       metadata.Url,
		Image:     image,
	}); err != nil {
		return err
	}
	f.mu.Lock()
	state := f.clientStateFor(clientID)
	state.receivedCount++
	f.mu.Unlock()
	return f.checkAndFinalize(clientID)
}

func (f *ImageFetcher) checkAndFinalize(clientID clientid.ID) error {
	f.mu.Lock()
	state := f.clientStates[clientID]
	if state == nil || !state.hasEOF || state.receivedCount < state.expectedTotal {
		f.mu.Unlock()
		return nil
	}
	delete(f.clientStates, clientID)
	f.mu.Unlock()

	return f.resultQueue.Send(external.Packet[inner.QueryResult]{
		ClientID: clientID,
		IsEof:    true,
		Total:    state.receivedCount,
		Payload:  inner.QueryResult{QueryType: 2},
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

func (f *ImageFetcher) handleEOFPacket(packet external.Packet[inner.Metadata]) error {
	f.mu.Lock()
	state := f.clientStateFor(packet.ClientID)
	state.hasEOF = true
	state.expectedTotal = packet.Total
	f.mu.Unlock()

	return f.checkAndFinalize(packet.ClientID)
}

func (f *ImageFetcher) clientStateFor(clientID clientid.ID) *clientState {
	state, ok := f.clientStates[clientID]
	if !ok {
		state = &clientState{}
		f.clientStates[clientID] = state
	}
	return state
}
