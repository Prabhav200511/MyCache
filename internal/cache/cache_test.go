package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	cache := New()

	cache.Set("name", "Krishna")

	value, ok := cache.Get("name")

	if !ok {
		t.Fatal("expected key to exist")
	}

	str, ok := value.(string)
	if !ok {
		t.Fatal("expected value to be a string")
	}

	if str != "Krishna" {
		t.Fatalf("expected Krishna, got %s", str)
	}
}

func TestDelete(t *testing.T) {

	cache := New()

	cache.Set("rollNo", "23UCS640")

	cache.Delete("rollNo")

	_, ok := cache.Get("rollNo")

	if ok {
		t.Fatal("Key not deleted")
	}
}

func CallerSet(c *Cache, key string, value any, wg *sync.WaitGroup) {
	defer wg.Done()

	c.Set(key, value)
}

func TestConcurrent(t *testing.T) {
	cache := New()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		key := fmt.Sprintf("key%d", i)

		go CallerSet(cache, key, i, &wg)
	}

	wg.Wait()
}
