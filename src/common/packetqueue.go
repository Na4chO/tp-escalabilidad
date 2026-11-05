package common

import (
	"sync"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

type PacketQueue[T any] struct {
	mw              middleware.Middleware
	batcher         *Batcher[T]
	clientTotalSent map[clientid.ID]uint32
	countersMutex   sync.Mutex
}

func NewPacketQueue[T any](queueName string, settings middleware.ConnSettings, batchSize int) (*PacketQueue[T], error) {
	queue, err := middleware.CreateQueueMiddleware(queueName, settings)
	if err != nil {
		return nil, err
	}
	q := &PacketQueue[T]{mw: queue, clientTotalSent: make(map[clientid.ID]uint32)}
	q.batcher = NewBatcher(batchSize, func(_ string, packet external.Packet[T]) error { return q.Send(packet) })
	return q, nil
}

func (q *PacketQueue[T]) Send(packet external.Packet[T]) error {
	data, err := packet.Encode()
	if err != nil {
		return err
	}
	return q.mw.Send(middleware.Message{Body: string(data)})
}

func (q *PacketQueue[T]) SendData(clientID clientid.ID, item T) error {
	if err := q.batcher.Add(clientID, "", item); err != nil {
		return err
	}

	q.countersMutex.Lock()
	q.clientTotalSent[clientID]++
	q.countersMutex.Unlock()
	return nil
}

func (q *PacketQueue[T]) Flush(clientID clientid.ID) error {
	return q.batcher.Flush(clientID)
}

func (q *PacketQueue[T]) SendEOF(clientID clientid.ID) error {
	var payload T
	return q.SendEOFWithPayload(clientID, payload)
}

func (q *PacketQueue[T]) SendEOFWithPayload(clientID clientid.ID, payload T) error {
	if err := q.Flush(clientID); err != nil {
		return err
	}

	q.countersMutex.Lock()
	total := q.clientTotalSent[clientID]
	q.countersMutex.Unlock()

	if err := q.Send(external.Packet[T]{
		ClientID: clientID,
		IsEof:    true,
		Total:    total,
		Payload:  payload,
	}); err != nil {
		return err
	}

	q.countersMutex.Lock()
	delete(q.clientTotalSent, clientID)
	q.countersMutex.Unlock()
	return nil
}

func (q *PacketQueue[T]) StartConsuming(callback func(packet external.Packet[T], ack func(), nack func())) error {
	return q.mw.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		packet, err := external.DecodePacket[T]([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		callback(packet, ack, nack)
	})
}

func (q *PacketQueue[T]) StopConsuming() error {
	return q.mw.StopConsuming()
}

func (q *PacketQueue[T]) Close() error {
	return q.mw.Close()
}
