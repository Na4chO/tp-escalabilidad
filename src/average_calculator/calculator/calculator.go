package calculator

import (
	"fmt"
	"log/slog"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

type clientStats struct {
	words     uint64
	abstracts uint64
}

// TODO setear en compose
const instanceKey = "0"

type Calculator struct {
	input    middleware.Router
	averages middleware.Router
	clients  map[clientid.ID]*clientStats
}

func NewCalculator(connSettings middleware.ConnSettings) (*Calculator, error) {
	input, err := middleware.CreateExchangeMiddleware("abstracts_until_2020", []string{instanceKey}, connSettings)
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
		err = c.handleEof(packet.ClientID)
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

func (c *Calculator) handleWordCount(clientID clientid.ID, wordCount inner.AbstractWordCount) error {
	stats, ok := c.clients[clientID]
	if !ok {
		stats = &clientStats{}
		c.clients[clientID] = stats
	}
	stats.words += uint64(wordCount.Count)
	stats.abstracts++
	return nil
}

func (c *Calculator) handleEof(clientID clientid.ID) error {
	var average float64
	if stats, ok := c.clients[clientID]; ok && stats.abstracts > 0 {
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
