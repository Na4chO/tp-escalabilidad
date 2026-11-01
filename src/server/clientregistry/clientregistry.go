package clientregistry

import (
	"net"
	"sync"
)

type ClientState struct {
	Conn net.Conn
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[uint32]ClientState
	nextID  uint32
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[uint32]ClientState)}
}

func (registry *ClientRegistry) Add(client ClientState) uint32 {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.nextID++
	id := registry.nextID
	registry.clients[id] = client
	return id
}

func (registry *ClientRegistry) Remove(id uint32) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.clients, id)
}

func (registry *ClientRegistry) Get(id uint32) (ClientState, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	client, ok := registry.clients[id]
	return client, ok
}

func (registry *ClientRegistry) WithLock(action func(clients map[uint32]ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
