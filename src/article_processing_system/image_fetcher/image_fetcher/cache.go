package imagefetcher

import (
	"container/list"
	"sync"
)

type ImageCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	lru      *list.List
}

type cacheEntry struct {
	key   string
	value []byte
}

func NewImageCache(capacity int) *ImageCache {
	return &ImageCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}
func (c *ImageCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(elem)
	entry := elem.Value.(*cacheEntry)
	return entry.value, true
}

func (c *ImageCache) Put(key string, value []byte) {
	if c.capacity <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		elem.Value.(*cacheEntry).value = value
		c.lru.MoveToFront(elem)
		return
	}
	entry := &cacheEntry{key: key, value: value}
	elem := c.lru.PushFront(entry)
	c.items[key] = elem

	if c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(*cacheEntry)
			c.lru.Remove(oldest)
			delete(c.items, oldEntry.key)
		}
	}
}
