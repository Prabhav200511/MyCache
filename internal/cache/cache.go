package cache

import (
	"container/list"
	"errors"
	"fmt"
	"net"
	"path/filepath"
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

	channels map[string]map[net.Conn]struct{}
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
		data:     make(map[string]Item),
		maxKeys:  _maxKeys,
		lruList:  list.New(),
		lruMap:   make(map[string]*list.Element),
		channels: make(map[string]map[net.Conn]struct{}),
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
	c.touch(key)
	if len(c.data) > c.maxKeys {
		c.evict()
	}
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
	c.touch(key)
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
	c.touch(key)
	if len(c.data) > c.maxKeys {
		c.evict()
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	c.removeFromLRU(key)
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
				c.removeFromLRU(key)
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
		c.touch(key)
		if len(c.data) > c.maxKeys {
			c.evict()
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
	c.touch(key)
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
	c.touch(key)
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
		c.touch(key)
		if len(c.data) > c.maxKeys {
			c.evict()
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
	c.touch(key)
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
		c.removeFromLRU(key)
	} else {
		item.Value = list
		c.data[key] = item
		c.touch(key)
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
		c.removeFromLRU(key)
	} else {
		item.Value = list
		c.data[key] = item
		c.touch(key)
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
			c.removeFromLRU(key)
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

		c.touch(key)
		if len(c.data) > c.maxKeys {
			c.evict()
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
	c.touch(key)
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
	c.touch(key)
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
		c.removeFromLRU(key)
		return nil
	}
	item.Value = hash
	c.data[key] = item
	c.touch(key)
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
	c.touch(key)
	return result, nil
}

func (c *Cache) Exists(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check existence and expiration, but do NOT call c.touch(key)
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
	c.touch(key)
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
	c.touch(key)
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
	c.touch(key)
	return len(hash), nil
}

func (c *Cache) touch(key string) {

	if elem, ok := c.lruMap[key]; ok {

		c.lruList.MoveToFront(elem)

		return
	}

	elem := c.lruList.PushFront(
		&LRUEntry{
			Key: key,
		},
	)

	c.lruMap[key] = elem
}

func (c *Cache) evict() {

	back := c.lruList.Back()

	if back == nil {
		return
	}

	entry := back.Value.(*LRUEntry)

	delete(c.data, entry.Key)

	delete(c.lruMap, entry.Key)

	c.lruList.Remove(back)
}

func (c *Cache) removeFromLRU(key string) {

	elem, ok := c.lruMap[key]

	if !ok {
		return
	}

	c.lruList.Remove(elem)

	delete(c.lruMap, key)
}

func (c *Cache) Subscribe(channel string, conn net.Conn) {

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.channels[channel]
	if !ok {
		c.channels[channel] = make(map[net.Conn]struct{})
	}

	c.channels[channel][conn] = struct{}{}
}

func (c *Cache) Unsubscribe(channel string, conn net.Conn) {

	c.mu.Lock()
	defer c.mu.Unlock()

	subs, ok := c.channels[channel]

	if !ok {
		return
	}

	delete(subs, conn)

	if len(subs) == 0 {
		delete(c.channels, channel)
	}
}

func (c *Cache) Publish(channel string, msg string) {
	c.mu.RLock()

	subs, ok := c.channels[channel]
	if !ok {
		c.mu.RUnlock()
		return
	}

	connections := make([]net.Conn, 0, len(subs))
	for conn := range subs {
		connections = append(connections, conn)
	}
	c.mu.RUnlock()

	// Construct the strict Redis Pub/Sub array: *3\r\n$7\r\nmessage\r\n$<len>\r\n<chan>\r\n$<len>\r\n<msg>\r\n
	respMsg := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(msg), msg)

	for _, conn := range connections {
		fmt.Fprint(conn, respMsg)
	}
}

func (c *Cache) RemoveConnection(conn net.Conn) {

	c.mu.Lock()
	defer c.mu.Unlock()

	for channel, subscribers := range c.channels {

		delete(subscribers, conn)

		if len(subscribers) == 0 {
			delete(c.channels, channel)
		}
	}
}

func (c *Cache) SubscriberCount(channel string) int {

	c.mu.RLock()
	defer c.mu.RUnlock()

	subscribers, ok := c.channels[channel]

	if !ok {
		return 0
	}

	return len(subscribers)
}

func (c *Cache) Channels() []string {

	c.mu.RLock()
	defer c.mu.RUnlock()

	channels := make([]string, 0, len(c.channels))

	for channel := range c.channels {
		channels = append(channels, channel)
	}

	return channels
}

func (c *Cache) Snapshot() map[string]Item {

	c.mu.RLock()
	defer c.mu.RUnlock()

	snapshot := make(map[string]Item, len(c.data))

	for key, item := range c.data {

		copiedItem := Item{
			Type:      item.Type,
			ExpiresAt: item.ExpiresAt,
		}

		switch item.Type {

		case StringType:
			copiedItem.Value = item.Value.(string)

		case ListType:

			list := item.Value.([]string)

			listCopy := make([]string, len(list))

			copy(listCopy, list)

			copiedItem.Value = listCopy

		case HashType:

			hash := item.Value.(map[string]string)

			hashCopy := make(map[string]string, len(hash))

			for field, value := range hash {
				hashCopy[field] = value
			}

			copiedItem.Value = hashCopy
		}

		snapshot[key] = copiedItem
	}

	return snapshot
}

func (c *Cache) Expire(key string, ttl time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, err := c.getValidItemLocked(key)
	if err != nil {
		return 0
	}

	item.ExpiresAt = time.Now().Add(ttl).Unix()
	c.data[key] = item
	c.touch(key)

	return 1
}

func (c *Cache) Persist(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, err := c.getValidItemLocked(key)
	if err != nil {
		return 0
	}

	if item.ExpiresAt == -1 {
		return 0
	}

	item.ExpiresAt = -1
	c.data[key] = item
	c.touch(key)

	return 1
}

func (c *Cache) DBSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	now := time.Now().Unix()

	for _, item := range c.data {
		if item.ExpiresAt != -1 && item.ExpiresAt <= now {
			continue
		}
		count++
	}

	return count
}

func (c *Cache) Keys(pattern string) []string {
	// Switched to RLock to allow concurrent reads
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []string
	now := time.Now().Unix()

	for key, item := range c.data {
		if item.ExpiresAt != -1 && item.ExpiresAt <= now {
			continue
		}

		if pattern == "*" {
			result = append(result, key)
			continue
		}

		matched, err := filepath.Match(pattern, key)
		if err == nil && matched {
			result = append(result, key)
		}
	}

	if result == nil {
		return []string{}
	}

	return result
}
