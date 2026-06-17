package cache

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	cache := New(10000)
	cache.Set("name", "Krishna")
	value, err := cache.Get("name")
	if err != nil {
		t.Fatal(err)
	}
	if value != "Krishna" {
		t.Fatalf("expected Krishna, got %s", value)
	}
}

func TestDelete(t *testing.T) {
	cache := New(10000)
	cache.Set("rollNo", "23UCS640")
	cache.Delete("rollNo")
	_, err := cache.Get("rollNo")
	if err == nil {
		t.Fatal("Key not deleted")
	}
}

func CallerSet(c *Cache, key string, value string, wg *sync.WaitGroup) {
	defer wg.Done()
	c.Set(key, value)
}

func TestConcurrent(t *testing.T) {
	cache := New(10000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("%d", i)
		go CallerSet(cache, key, val, &wg)
	}
	wg.Wait()
}

func TestTTLExpiration(t *testing.T) {
	cache := New(10000)
	cache.SetWithTTL("name", "Krishna", 2*time.Second)
	time.Sleep(1 * time.Second)
	_, err := cache.Get("name")
	if err != nil {
		t.Fatal("Disappeared before TTL")
	}
	time.Sleep(2 * time.Second)
	_, err = cache.Get("name")
	if err == nil {
		t.Fatal("Still did not disappear")
	}
}

func TestWrongTypeGet(t *testing.T) {
	cache := New(10000)
	err := cache.LPush("msgs", "hello")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cache.Get("msgs")
	if err == nil {
		t.Fatal("Expected WRONGTYPE")
	}
	if err.Error() != "WRONGTYPE" {
		t.Fatalf("expected WRONGTYPE, got %s", err.Error())
	}
}

func TestHashSetAndGet(t *testing.T) {
	cache := New(10000)
	err := cache.HSet("user:1", "name", "Krishna")
	if err != nil {
		t.Fatal(err)
	}
	err = cache.HSet("user:1", "role", "host")
	if err != nil {
		t.Fatal(err)
	}
	name, err := cache.HGet("user:1", "name")
	if err != nil {
		t.Fatal(err)
	}
	if name != "Krishna" {
		t.Fatalf("expected Krishna, got %s", name)
	}
	role, err := cache.HGet("user:1", "role")
	if err != nil {
		t.Fatal(err)
	}
	if role != "host" {
		t.Fatalf("expected host, got %s", role)
	}
}

func TestHashWrongType(t *testing.T) {
	cache := New(10000)
	cache.Set("user", "Krishna")
	err := cache.HSet("user", "role", "host")
	if err == nil {
		t.Fatal("expected WRONGTYPE")
	}
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected WRONGTYPE, got %v", err)
	}
}

func TestHashMissingField(t *testing.T) {
	cache := New(10000)
	err := cache.HSet("user:1", "name", "Krishna")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cache.HGet("user:1", "age")
	if err == nil {
		t.Fatal("expected NOFIELD")
	}
	if !errors.Is(err, ErrNoField) {
		t.Fatalf("expected NOFIELD, got %v", err)
	}
}

func TestHashDeleteLastField(t *testing.T) {
	cache := New(10000)
	cache.HSet("user:1", "name", "Krishna")
	cache.HDel("user:1", "name")
	_, err := cache.HGet("user:1", "name")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListDeleteLastElement(t *testing.T) {
	cache := New(10000)
	cache.LPush("list:1", "element")
	cache.LPop("list:1")
	_, err := cache.LRange("list:1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestExists(t *testing.T) {
	cache := New(10000)
	if cache.Exists("missing") {
		t.Fatal("expected false for missing key")
	}
	cache.Set("found", "value")
	if !cache.Exists("found") {
		t.Fatal("expected true for existing key")
	}
}

func TestType(t *testing.T) {
	cache := New(10000)
	cache.Set("strKey", "val")
	cache.LPush("listKey", "val")
	cache.HSet("hashKey", "field", "val")

	if cache.Type("strKey") != "string" {
		t.Fatalf("expected string, got %s", cache.Type("strKey"))
	}
	if cache.Type("listKey") != "list" {
		t.Fatalf("expected list, got %s", cache.Type("listKey"))
	}
	if cache.Type("hashKey") != "hash" {
		t.Fatalf("expected hash, got %s", cache.Type("hashKey"))
	}
	if cache.Type("missing") != "none" {
		t.Fatalf("expected none, got %s", cache.Type("missing"))
	}
}

func TestLLen(t *testing.T) {
	cache := New(10000)
	cache.LPush("list", "one")
	cache.LPush("list", "two")

	l, err := cache.LLen("list")
	if err != nil || l != 2 {
		t.Fatalf("expected length 2, got %d", l)
	}
}

func TestHLen(t *testing.T) {
	cache := New(10000)
	cache.HSet("hash", "f1", "v1")
	cache.HSet("hash", "f2", "v2")
	cache.HSet("hash", "f3", "v3")

	l, err := cache.HLen("hash")
	if err != nil || l != 3 {
		t.Fatalf("expected length 3, got %d", l)
	}
}

func TestExistsExpiredKey(t *testing.T) {
	cache := New(10000)

	cache.SetWithTTL("temp", "x", 1*time.Second)

	time.Sleep(2 * time.Second)

	if cache.Exists("temp") {
		t.Fatal("expired key should not exist")
	}
}

func TestLLenWrongType(t *testing.T) {
	cache := New(10000)

	cache.Set("name", "Krishna")

	_, err := cache.LLen("name")

	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected WRONGTYPE, got %v", err)
	}
}

func TestHLenWrongType(t *testing.T) {
	cache := New(10000)

	cache.Set("name", "Krishna")

	_, err := cache.HLen("name")

	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("expected WRONGTYPE, got %v", err)
	}
}
