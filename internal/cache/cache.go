package cache

import "sync"

type Item struct {
	Value     any
	ExpiresAt int64
}

type Cache struct {
	data map[string]Item
	mu   sync.RWMutex
}

func New() *Cache {
	return &Cache{
		data: make(map[string]Item),
	}
}

func (c *Cache) Set(key string, value any) {

	if key == "" {
		return
	}

	item := Item{
		Value: value,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = item

}

func (c *Cache) Get(key string) (any, bool) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data[key]

	if ok {
		return item.Value, ok
	}
	return nil, ok
}

func (c *Cache) Delete(key string) {

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
}
