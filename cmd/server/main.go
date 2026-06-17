package main

import (
	"log"
	"mycache/internal/cache"
	"mycache/internal/network"
	"mycache/internal/persistence"
)

func main() {

	c := cache.New(10000)

	aof, err := persistence.NewAOF("appendonly.aof")
	if err != nil {
		log.Fatal(err)
	}

	err = aof.Replay(c)
	if err != nil {
		log.Fatal(err)
	}

	network.Start(":6380", c, aof)
}
