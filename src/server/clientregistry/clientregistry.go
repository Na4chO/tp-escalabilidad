package clientregistry

import (
	"net"
	"sync"

	articleHandler "github.com/Na4chO/tp-escalabilidad/server/articlehandler"
)

type ClientState struct {
	Conn    net.Conn
	Handler *articleHandler.ArticleHandler
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients []ClientState
}

func (registry *ClientRegistry) Add(client ClientState) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients = append(registry.clients, client)
}

func (registry *ClientRegistry) Remove(i int) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients = append(registry.clients[:i], registry.clients[i+1:]...)
}

func (registry *ClientRegistry) WithLock(action func([]ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
