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
	"github.com/Na4chO/tp-escalabilidad/common/synchronizer"
)

const countsDir = "counts"

type pending struct {
	synced bool
	avg    *float64
}

type Counter struct {
	input        *common.PacketQueue[inner.AbstractWordCount]
	synchronizer *synchronizer.Synchronizer
	averages     middleware.Router
	results      *common.PacketQueue[inner.QueryResult]
	countsDir    string

	mu      sync.Mutex
	pending map[uint32]*pending
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

	c := &Counter{countsDir: countsDir, pending: map[uint32]*pending{}}

	input, err := common.NewPacketQueue[inner.AbstractWordCount]("abstracts_from_2021", connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	syncer, err := synchronizer.NewSynchronizer(instanceID, "abstract_word_counter_sync", connSettings, c.onSynced, c.onFinished)
	if err != nil {
		input.Close()
		return nil, fmt.Errorf("creating synchronizer: %w", err)
	}

	averages, err := middleware.CreateExchangeMiddleware("averages", []string{"average"}, connSettings)
	if err != nil {
		input.Close()
		syncer.Close()
		return nil, fmt.Errorf("creating averages exchange: %w", err)
	}

	results, err := common.NewPacketQueue[inner.QueryResult](common.QueueQueryResult, connSettings)
	if err != nil {
		input.Close()
		syncer.Close()
		averages.Close()
		return nil, fmt.Errorf("creating results queue: %w", err)
	}

	c.input = input
	c.synchronizer = syncer
	c.averages = averages
	c.results = results
	return c, nil
}

func (c *Counter) Run() error {
	errs := make(chan error, 3)
	go func() { errs <- c.synchronizer.StartConsuming() }()
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
	c.input.StopConsuming()
	c.synchronizer.StopConsuming()
	c.averages.StopConsuming()
}

func (c *Counter) Close() {
	c.input.Close()
	c.synchronizer.Close()
	c.averages.Close()
	c.results.Close()
}

func (c *Counter) handleInput(packet external.Packet[inner.AbstractWordCount], ack func(), nack func()) {
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
	return c.synchronizer.Processed(clientID)
}

func (c *Counter) handleEof(clientID, total uint32) error {
	return c.synchronizer.EndOfData(clientID, total)
}

func (c *Counter) handleAverage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.Average]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding average message", "err", err)
		nack()
		return
	}

	avg := packet.Payload.Average
	c.filterIfReady(packet.ClientID, func(p *pending) { p.avg = &avg })
	ack()
}

func (c *Counter) onSynced(clientID uint32) {
	c.filterIfReady(clientID, func(p *pending) { p.synced = true })
}

func (c *Counter) onFinished(clientID uint32, total uint32) {
	err := c.results.Send(external.Packet[inner.QueryResult]{
		ClientID: clientID,
		IsEof:    true,
		Total:    total,
		Payload:  inner.QueryResult{QueryType: inner.QueryAbstractAboveAverage},
	})
	if err != nil {
		slog.Error("While sending results EOF", "client", clientID, "err", err)
		return
	}
	slog.Info("Client finished", "client", clientID, "total", total)
}

func (c *Counter) filterIfReady(clientID uint32, update func(*pending)) {
	c.mu.Lock()
	p := c.pending[clientID]
	if p == nil {
		p = &pending{}
		c.pending[clientID] = p
	}
	update(p)
	ready := p.synced && p.avg != nil
	if ready {
		delete(c.pending, clientID)
	}
	c.mu.Unlock()

	if ready {
		go c.filter(clientID, *p.avg)
	}
}

func (c *Counter) filter(clientID uint32, average float64) {
	if err := c.sendResults(clientID, average); err != nil {
		slog.Error("While sending results", "client", clientID, "err", err)
	}
	if err := removeCounts(c.countsDir, clientID); err != nil {
		slog.Error("While removing counts file", "client", clientID, "err", err)
	}
	if err := c.synchronizer.Finished(clientID); err != nil {
		slog.Error("While sending finished", "client", clientID, "err", err)
	}
}

func (c *Counter) sendResults(clientID uint32, average float64) error {
	articles, passed := 0, 0
	err := scanCounts(c.countsDir, clientID, func(url string, count uint32) error {
		articles++
		if float64(count) <= average {
			return nil
		}
		if err := c.sendResult(clientID, url, count); err != nil {
			return err
		}
		passed++
		c.synchronizer.Sent(clientID)
		return nil
	})
	if err != nil {
		return err
	}
	slog.Info("Client filtered", "client", clientID, "average", average, "articles", articles, "passed", passed)
	return nil
}

func (c *Counter) sendResult(clientID uint32, url string, count uint32) error {
	return c.results.Send(external.Packet[inner.QueryResult]{
		ClientID: clientID,
		Payload: inner.QueryResult{
			QueryType: inner.QueryAbstractAboveAverage,
			Url:       url,
			WordCount: count,
		},
	})
}
