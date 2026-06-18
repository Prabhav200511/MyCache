package persistence

import (
	"fmt"
	"mycache/internal/cache"
	"os"
	"sync"
	"testing"
)

func TestAOF_RewriteConcurrency(t *testing.T) {
	c := cache.New(10000)
	c.Set("A", "1")

	os.Remove("appendonly.aof")
	aof, err := NewAOF("appendonly.aof")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = aof.Append(fmt.Sprintf("SET K%d V", i))
		}
	}()

	err = aof.Rewrite(c)
	if err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	wg.Wait()
	aof.Close()
	defer os.Remove("appendonly.aof")

	c2 := cache.New(10000)
	aof2, err := NewAOF("appendonly.aof")
	if err != nil {
		t.Fatal(err)
	}

	err = aof2.Replay(c2)
	if err != nil {
		t.Fatal(err)
	}
	aof2.Close()

	val, err := c2.Get("A")
	if err != nil || val != "1" {
		t.Fatal("Initial key A is missing or wrong")
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("K%d", i)
		val, err := c2.Get(key)
		if err != nil || val != "V" {
			t.Fatalf("Missing concurrent key %s - Data loss detected!", key)
		}
	}
}
