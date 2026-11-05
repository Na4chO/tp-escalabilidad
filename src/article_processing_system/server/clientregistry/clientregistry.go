package clientregistry

import (
	"net"
	"sync"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
)

type ClientState struct {
	Conn net.Conn
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[clientid.ID]ClientState
	nextID  uint32
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[clientid.ID]ClientState)}
}

func (registry *ClientRegistry) Add(serverID string, client ClientState) clientid.ID {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.nextID++
	id := clientid.ID{Server: serverID, Seq: registry.nextID}
	registry.clients[id] = client
	return id
}

func (registry *ClientRegistry) Remove(id clientid.ID) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.clients, id)
}

func (registry *ClientRegistry) Get(id clientid.ID) (ClientState, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	client, ok := registry.clients[id]
	return client, ok
}

func (registry *ClientRegistry) WithLock(action func(clients map[clientid.ID]ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
