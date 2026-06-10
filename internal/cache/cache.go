package cache

import (
	"errors"
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
	c := &Cache{
		data: make(map[string]Item),
	}

	go c.startExpirationWorker()

	return c

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

func (c *Cache) TTLleft(key string) int64 {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if !ok {
		return -2
	}

	if item.ExpiresAt == -1 {
		return -1
	}

	now := time.Now().Unix()
	timeleft := item.ExpiresAt - now

	if timeleft <= 0 {
		delete(c.data, key)
		return -2
	}

	return timeleft
}

func (c *Cache) LPush(key string, value string) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if !ok {
		c.data[key] = Item{
			Value:     []string{value},
			ExpiresAt: -1,
		}
		return nil
	}

	list, ok := item.Value.([]string)
	if !ok {
		return errors.New("WRONGTYPE")
	}

	list = append([]string{value}, list...)

	item.Value = list
	c.data[key] = item

	return nil
}

func (c *Cache) LRange(key string) ([]string, error) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data[key]
	if !ok {
		return nil, errors.New("NOTFOUND")
	}

	list, ok := item.Value.([]string)
	if !ok {
		return nil, errors.New("WRONGTYPE")
	}

	return list, nil
}

func (c *Cache) RPush(key string, value string) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if !ok {
		c.data[key] = Item{
			Value:     []string{value},
			ExpiresAt: -1,
		}
		return nil
	}

	list, ok := item.Value.([]string)
	if !ok {
		return errors.New("WRONGTYPE")
	}

	list = append(list, value)

	item.Value = list
	c.data[key] = item

	return nil
}

func (c *Cache) LPop(key string) (string, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if !ok {
		return "", errors.New("NOTFOUND")
	}

	list, ok := item.Value.([]string)

	if !ok {
		return "", errors.New("WRONGTYPE")
	}

	if len(list) == 0 {
		return "", errors.New("EMPTYLIST")
	}

	value := list[0]

	list = list[1:]

	if len(list) == 0 {
		delete(c.data, key)
	} else {
		item.Value = list
		c.data[key] = item
	}

	return value, nil
}

func (c *Cache) RPop(key string) (string, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data[key]

	if !ok {
		return "", errors.New("NOTFOUND")
	}

	list, ok := item.Value.([]string)

	if !ok {
		return "", errors.New("WRONGTYPE")
	}

	if len(list) == 0 {
		return "", errors.New("EMPTYLIST")
	}

	value := list[len(list)-1]

	list = list[:len(list)-1]

	if len(list) == 0 {
		delete(c.data, key)
	} else {
		item.Value = list
		c.data[key] = item
	}

	return value, nil
}
