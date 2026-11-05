package common

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/middleware"
)

type Batcher[T any] struct {
	size    int
	send    func(key string, packet external.Packet[T]) error
	mu      sync.Mutex
	pending map[clientid.ID]map[string][]T
}

func NewBatcher[T any](size int, send func(key string, packet external.Packet[T]) error) *Batcher[T] {
	return &Batcher[T]{size: max(size, 1), send: send, pending: map[clientid.ID]map[string][]T{}}
}

func (b *Batcher[T]) Add(clientID clientid.ID, key string, item T) error {
	b.mu.Lock()
	if b.pending[clientID] == nil {
		b.pending[clientID] = map[string][]T{}
	}
	b.pending[clientID][key] = append(b.pending[clientID][key], item)
	var full []T
	if len(b.pending[clientID][key]) >= b.size {
		full = b.pending[clientID][key]
		delete(b.pending[clientID], key)
	}
	b.mu.Unlock()

	return b.sendBatch(clientID, key, full)
}

func (b *Batcher[T]) Flush(clientID clientid.ID) error {
	b.mu.Lock()
	batches := b.pending[clientID]
	delete(b.pending, clientID)
	b.mu.Unlock()

	var err error
	for key, items := range batches {
		err = errors.Join(err, b.sendBatch(clientID, key, items))
	}
	return err
}

func (b *Batcher[T]) sendBatch(clientID clientid.ID, key string, items []T) error {
	if len(items) == 0 {
		return nil
	}
	return b.send(key, external.Packet[T]{ClientID: clientID, Count: uint32(len(items)), Items: items})
}

func ExchangeSender[T any](exchange middleware.Router) func(key string, packet external.Packet[T]) error {
	return func(key string, packet external.Packet[T]) error {
		data, err := packet.Encode()
		if err != nil {
			return err
		}
		return exchange.SendTo(key, middleware.Message{Body: string(data)})
	}
}

func LoadBatchSize() (int, error) {
	value := os.Getenv("BATCH_SIZE")
	if value == "" {
		return 1, nil
	}
	size, err := strconv.Atoi(value)
	if err != nil || size < 1 {
		return 0, fmt.Errorf("BATCH_SIZE must be a positive number, got %q", value)
	}
	return size, nil
}
