package calculator

import (
	"fmt"
	"log/slog"

	"github.com/Na4chO/tp-escalabilidad/article_processing_system/average_calculator/config"
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

type clientStats struct {
	words     uint64
	abstracts uint64
	expected  uint64
	hasEOF    bool
}

type Calculator struct {
	input    middleware.Router
	averages middleware.Router
	clients  map[clientid.ID]*clientStats
}

func NewCalculator(cfg *config.Config) (*Calculator, error) {
	connSettings := middleware.ConnSettings{Hostname: cfg.MomHost, Port: cfg.MomPort}
	inputKey := common.PartitionKey(common.ExchangeAbstractsUntil2020, cfg.ID)
	input, err := middleware.CreateExchangeMiddleware(common.ExchangeAbstractsUntil2020, []string{inputKey}, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
	}

	averages, err := middleware.CreateExchangeMiddleware("averages", nil, connSettings)
	if err != nil {
		input.Close()
		return nil, fmt.Errorf("creating averages exchange: %w", err)
	}

	return &Calculator{input: input, averages: averages, clients: map[clientid.ID]*clientStats{}}, nil
}

func (c *Calculator) Run() error {
	return c.input.StartConsuming(c.handleMessage)
}

func (c *Calculator) Stop() error {
	return c.input.StopConsuming()
}

func (c *Calculator) Close() {
	c.input.Close()
	c.averages.Close()
}

func (c *Calculator) handleMessage(msg middleware.Message, ack func(), nack func()) {
	packet, err := external.DecodePacket[inner.AbstractWordCount]([]byte(msg.Body))
	if err != nil {
		slog.Error("Discarding message", "err", err)
		nack()
		return
	}

	if packet.IsEof {
		err = c.handleEof(packet.ClientID, packet.Total)
	} else {
		err = c.handleWordCounts(packet.ClientID, packet.Items)
	}
	if err != nil {
		slog.Error("While handling message", "client", packet.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (c *Calculator) handleWordCounts(clientID clientid.ID, batch []inner.AbstractWordCount) error {
	stats := c.statsFor(clientID)
	for _, wordCount := range batch {
		stats.words += uint64(wordCount.Count)
		stats.abstracts++
	}
	return c.sendAverageIfDone(clientID, stats)
}

func (c *Calculator) handleEof(clientID clientid.ID, total uint32) error {
	stats := c.statsFor(clientID)
	stats.expected = uint64(total)
	stats.hasEOF = true
	return c.sendAverageIfDone(clientID, stats)
}

func (c *Calculator) sendAverageIfDone(clientID clientid.ID, stats *clientStats) error {
	if !stats.hasEOF || stats.abstracts < stats.expected {
		return nil
	}

	var average float64
	if stats.abstracts > 0 {
		average = float64(stats.words) / float64(stats.abstracts)
	}
	delete(c.clients, clientID)

	data, err := external.Packet[inner.Average]{ClientID: clientID, Payload: inner.Average{Average: average}}.Encode()
	if err != nil {
		return err
	}
	slog.Info("Average calculated", "client", clientID, "average", average)
	return c.averages.SendTo("average", middleware.Message{Body: string(data)})
}

func (c *Calculator) statsFor(clientID clientid.ID) *clientStats {
	stats, ok := c.clients[clientID]
	if !ok {
		stats = &clientStats{}
		c.clients[clientID] = stats
	}
	return stats
}
