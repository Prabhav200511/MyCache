package main

import (
	"mycache/internal/cache"
	"mycache/internal/network"
)

func main() {
	c := cache.New()
	network.Start(":6380", c)
}
