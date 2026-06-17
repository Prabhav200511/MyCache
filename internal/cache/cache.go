package cache

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
	HashType
)

type Item struct {
	Value     any
	Type      ValueType
	ExpiresAt int64
}

type Cache struct {
	data map[string]Item
	mu   sync.RWMutex

	maxKeys int

	lruList *list.List
	lruMap  map[string]*list.Element
}

type LRUEntry struct {
	Key string
}

var (
	ErrNotFound  = errors.New("NOTFOUND")
	ErrWrongType = errors.New("WRONGTYPE")
	ErrEmptyList = errors.New("EMPTYLIST")
	ErrNoField   = errors.New("NOFIELD")
)

func New(_maxKeys int) *Cache {
	c := &Cache{
		data:    make(map[string]Item),
		maxKeys: _maxKeys,
		lruList: list.New(),
		lruMap:  make(map[string]*list.Element),
	}
	go c.startExpirationWorker()
	return c
}

func (c *Cache) Set(key string, value string) {
	if key == "" {
		return
	}
	item := Item{
		Value:     value,
		Type:      StringType,
		ExpiresAt: -1,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = item
}

func (c *Cache) Get(key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return "", err
	}
	if item.Type != StringType {
		return "", ErrWrongType
	}
	return item.Value.(string), nil
}

func (c *Cache) SetWithTTL(key string, value string, ttl time.Duration) {
	if key == "" {
		return
	}
	item := Item{
		Value:     value,
		Type:      StringType,
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
			if item.ExpiresAt <= now {
				delete(c.data, key)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Cache) TTLleft(key string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return -2
	}
	if item.ExpiresAt == -1 {
		return -1
	}
	return item.ExpiresAt - time.Now().Unix()
}

func (c *Cache) LPush(key string, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if !ok {
		c.data[key] = Item{
			Value:     []string{value},
			Type:      ListType,
			ExpiresAt: -1,
		}
		return nil
	}
	if item.Type != ListType {
		return ErrWrongType
	}
	list := item.Value.([]string)
	list = append([]string{value}, list...)
	item.Value = list
	c.data[key] = item
	return nil
}

func (c *Cache) LRange(key string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return nil, err
	}
	if item.Type != ListType {
		return nil, ErrWrongType
	}
	list := item.Value.([]string)
	return list, nil
}

func (c *Cache) RPush(key string, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if !ok {
		c.data[key] = Item{
			Value:     []string{value},
			Type:      ListType,
			ExpiresAt: -1,
		}
		return nil
	}
	if item.Type != ListType {
		return ErrWrongType
	}
	list := item.Value.([]string)
	list = append(list, value)
	item.Value = list
	c.data[key] = item
	return nil
}

func (c *Cache) LPop(key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return "", err
	}
	if item.Type != ListType {
		return "", ErrWrongType
	}
	list := item.Value.([]string)
	if len(list) == 0 {
		return "", ErrEmptyList
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
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return "", err
	}
	if item.Type != ListType {
		return "", ErrWrongType
	}
	list := item.Value.([]string)
	if len(list) == 0 {
		return "", ErrEmptyList
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

// getValidItemLocked expects the caller to already hold c.mu.Lock()
func (c *Cache) getValidItemLocked(key string) (Item, error) {
	item, ok := c.data[key]
	if !ok {
		return Item{}, ErrNotFound
	}
	if item.ExpiresAt != -1 {
		now := time.Now().Unix()
		if item.ExpiresAt <= now {
			delete(c.data, key)
			return Item{}, ErrNotFound
		}
	}
	return item, nil
}

func (c *Cache) HSet(key, field, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if !ok {
		c.data[key] = Item{
			Type: HashType,
			Value: map[string]string{
				field: value,
			},
			ExpiresAt: -1,
		}
		return nil
	}
	if item.Type != HashType {
		return ErrWrongType
	}
	hash := item.Value.(map[string]string)
	hash[field] = value
	item.Value = hash
	c.data[key] = item
	return nil
}

func (c *Cache) HGet(key, field string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return "", err
	}
	if item.Type != HashType {
		return "", ErrWrongType
	}
	hash := item.Value.(map[string]string)
	value, ok := hash[field]
	if !ok {
		return "", ErrNoField
	}
	return value, nil
}

func (c *Cache) HDel(key, field string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return err
	}
	if item.Type != HashType {
		return ErrWrongType
	}
	hash := item.Value.(map[string]string)
	if _, ok := hash[field]; !ok {
		return ErrNoField
	}
	delete(hash, field)
	if len(hash) == 0 {
		delete(c.data, key)
		return nil
	}
	item.Value = hash
	c.data[key] = item
	return nil
}

func (c *Cache) HGetAll(key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return nil, err
	}
	if item.Type != HashType {
		return nil, ErrWrongType
	}
	hash := item.Value.(map[string]string)
	result := make(map[string]string)
	for field, value := range hash {
		result[field] = value
	}
	return result, nil
}

func (c *Cache) Exists(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.getValidItemLocked(key)
	return err == nil
}

func (c *Cache) Type(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return "none"
	}
	switch item.Type {
	case StringType:
		return "string"
	case ListType:
		return "list"
	case HashType:
		return "hash"
	}
	return "none"
}

func (c *Cache) LLen(key string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return 0, err
	}
	if item.Type != ListType {
		return 0, ErrWrongType
	}
	list := item.Value.([]string)
	return len(list), nil
}

func (c *Cache) HLen(key string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.getValidItemLocked(key)
	if err != nil {
		return 0, err
	}
	if item.Type != HashType {
		return 0, ErrWrongType
	}
	hash := item.Value.(map[string]string)
	return len(hash), nil
}
