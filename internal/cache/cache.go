package cache

import (
	"sync"
	"time"
)

type Item struct {
	Value     any
	ExpiresAt int64
}

type Cache struct {
	data map[string]Item
	mu   sync.RWMutex
}

func New() *Cache {
	c := Cache{
		data: make(map[string]Item),
	}

	go c.startExpirationWorker()

	return &c

}

func (c *Cache) Set(key string, value any) {

	if key == "" {
		return
	}

	item := Item{
		Value:     value,
		ExpiresAt: -1,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = item

}

func (c *Cache) Get(key string) (any, bool) {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if ok {
		if item.ExpiresAt == -1 {
			return item.Value, ok
		}
		now := time.Now().Unix()
		if item.ExpiresAt < now {
			delete(c.data, key)
			return nil, false
		}
		return item.Value, ok
	}
	return nil, false
}

func (c *Cache) SetWithTTL(key string, value any, ttl time.Duration) {

	if key == "" {
		return
	}

	item := Item{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl).Unix(),
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = item
}

func (c *Cache) Delete(key string) {

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
}

func (c *Cache) startExpirationWorker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {

		now := time.Now().Unix()

		c.mu.Lock()

		for key, item := range c.data {

			if item.ExpiresAt == -1 {
				continue
			}

			if item.ExpiresAt < now {
				delete(c.data, key)
			}
		}

		c.mu.Unlock()
	}
}
