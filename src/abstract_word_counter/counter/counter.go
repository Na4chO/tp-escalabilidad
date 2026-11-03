package counter

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

const countsDir = "counts"

type Counter struct {
	instanceID string
	input      *common.PacketQueue[inner.AbstractWordCount]
	sync       middleware.Router
	averages   middleware.Router
	results    *common.PacketQueue[inner.QueryResult]
	countsDir  string

	mu         sync.Mutex
	consumed   *sync.Cond
	stopping   bool
	closing    map[uint32]bool
	totals     map[uint32]uint32
	peerCounts map[uint32]map[string]uint32
}

func NewCounter(connSettings middleware.ConnSettings) (*Counter, error) {
	instanceID, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("getting instance id: %w", err)
	}

	// TODO Check if necessary, maybe dir can be created in compose
	if err := os.MkdirAll(countsDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating counts directory: %w", err)
	}

	input, err := common.NewPacketQueue[inner.AbstractWordCount](inner.AbstractsFrom2021Queue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	syncExchange, err := middleware.CreateExchangeMiddleware(inner.WordCounterSyncExchange, []string{inner.WordCounterSyncKey}, connSettings)
	if err != nil {
		input.Close()
		return nil, fmt.Errorf("creating sync exchange: %w", err)
	}

	averages, err := middleware.CreateExchangeMiddleware(inner.AveragesExchange, []string{inner.AveragesKey}, connSettings)
	if err != nil {
		input.Close()
		syncExchange.Close()
		return nil, fmt.Errorf("creating averages exchange: %w", err)
	}

	results, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		input.Close()
		syncExchange.Close()
		averages.Close()
		return nil, fmt.Errorf("creating results queue: %w", err)
	}

	c := &Counter{
		instanceID: instanceID,
		input:      input,
		sync:       syncExchange,
		averages:   averages,
		results:    results,
		countsDir:  countsDir,
		closing:    map[uint32]bool{},
		totals:     map[uint32]uint32{},
		peerCounts: map[uint32]map[string]uint32{},
	}
	c.consumed = sync.NewCond(&c.mu)
	return c, nil
}

func (c *Counter) Run() error {
	errs := make(chan error, 3)
	go func() { errs <- c.sync.StartConsuming(c.handleSync) }()
	go func() { errs <- c.averages.StartConsuming(c.handleAverage) }()
	go func() { errs <- c.input.StartConsuming(c.handleInput) }()

	var firstErr error
	for range 3 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
			c.Stop()
		}
	}
	return firstErr
}

func (c *Counter) Stop() {
	c.mu.Lock()
	c.stopping = true
	c.consumed.Broadcast()
	c.mu.Unlock()

	c.input.StopConsuming()
	c.sync.StopConsuming()
	c.averages.StopConsuming()
}

func (c *Counter) Close() {
	c.input.Close()
	c.sync.Close()
	c.averages.Close()
	c.results.Close()
}

func (c *Counter) handleInput(packet external.Packet[inner.AbstractWordCount], ack func(), nack func()) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if packet.IsEof {
		err = c.handleEof(packet.ClientID, packet.Total)
	} else {
		err = c.handleWordCount(packet.ClientID, packet.Payload)
	}
	if err != nil {
		slog.Error("While handling message", "client", packet.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (c *Counter) handleWordCount(clientID uint32, wordCount inner.AbstractWordCount) error {
	if err := appendCount(c.countsDir, clientID, wordCount.Url, wordCount.Count); err != nil {
		return fmt.Errorf("storing word count: %w", err)
	}
	c.peerCount(clientID)[c.instanceID]++

	if !c.closing[clientID] {
		return nil
	}
	c.notifyIfConsumed(clientID)
	return c.sendCount(clientID)
}

func (c *Counter) handleEof(clientID uint32, total uint32) error {
	c.closing[clientID] = true
	c.totals[clientID] = total
	c.notifyIfConsumed(clientID)
	return c.sendCount(clientID)
}

func (c *Counter) handleSync(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.PeerCount]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding sync message", "err", err)
		nack()
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if packet.Payload.Instance == c.instanceID {
		ack()
		return
	}

	clientID := packet.ClientID
	if !c.closing[clientID] {
		c.closing[clientID] = true
		c.totals[clientID] = packet.Total
		if c.peerCount(clientID)[c.instanceID] > 0 {
			if err := c.sendCount(clientID); err != nil {
				slog.Error("While sending count", "client", clientID, "err", err)
			}
		}
	}
	c.peerCount(clientID)[packet.Payload.Instance] = packet.Payload.Count

	c.notifyIfConsumed(clientID)
	ack()
}

func (c *Counter) handleAverage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.Average]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding average message", "err", err)
		nack()
		return
	}

	clientID := packet.ClientID
	c.mu.Lock()
	for !c.isConsumed(clientID) {
		if c.stopping {
			c.mu.Unlock()
			return
		}
		c.consumed.Wait()
	}
	c.mu.Unlock()

	// Ya no llegan más datos de este cliente: se puede leer el archivo sin el lock.
	err = c.sendResults(clientID, packet.Payload.Average)
	if err != nil {
		slog.Error("While sending results", "client", clientID, "err", err)
	}

	c.mu.Lock()
	delete(c.closing, clientID)
	delete(c.totals, clientID)
	delete(c.peerCounts, clientID)
	c.mu.Unlock()

	if err := removeCounts(c.countsDir, clientID); err != nil {
		slog.Error("While removing counts file", "client", clientID, "err", err)
	}

	if err != nil {
		nack()
		return
	}
	ack()
}

func (c *Counter) sendResults(clientID uint32, average float64) error {
	articles, passed := 0, 0
	err := scanCounts(c.countsDir, clientID, func(url string, count uint32) error {
		articles++
		if float64(count) <= average {
			return nil
		}
		passed++
		return c.sendResult(clientID, url, count)
	})
	if err != nil {
		return err
	}
	slog.Info("Client filtered", "client", clientID, "average", average, "articles", articles, "passed", passed)
	return nil
}

func (c *Counter) sendResult(clientID uint32, url string, count uint32) error {
	return c.results.SendData(clientID, inner.QueryResult{
		QueryType: inner.QueryAbstractAboveAverage,
		Url:       url,
		WordCount: count,
	})
}

func (c *Counter) sendCount(clientID uint32) error {
	data, err := external.Packet[inner.PeerCount]{
		ClientID: clientID,
		Total:    c.totals[clientID],
		Payload: inner.PeerCount{
			Instance: c.instanceID,
			Count:    c.peerCount(clientID)[c.instanceID],
		},
	}.Encode()
	if err != nil {
		return err
	}
	return c.sync.Send(middleware.Message{Body: string(data)})
}

func (c *Counter) isConsumed(clientID uint32) bool {
	if !c.closing[clientID] {
		return false
	}
	var consumed uint32
	for _, count := range c.peerCounts[clientID] {
		consumed += count
	}
	return consumed == c.totals[clientID]
}

func (c *Counter) notifyIfConsumed(clientID uint32) {
	if c.isConsumed(clientID) {
		c.consumed.Broadcast()
	}
}

func (c *Counter) peerCount(clientID uint32) map[string]uint32 {
	if c.peerCounts[clientID] == nil {
		c.peerCounts[clientID] = map[string]uint32{}
	}
	return c.peerCounts[clientID]
}
