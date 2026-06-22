package main

import (
	"log"
	"mycache/internal/cache"
	"mycache/internal/config"
	"mycache/internal/network"
	"mycache/internal/persistence"
)

func main() {

	cfg := config.Load()

	c := cache.New(cfg.MaxKeys)

	aof, err := persistence.NewAOF(cfg.AOFFile)

	if err != nil {
		log.Fatal(err)
	}

	err = aof.Replay(c)
	if err != nil {
		log.Fatal(err)
	}

	network.Start(
		":"+cfg.Port,
		c,
		aof,
		cfg,
	)

	log.Println("========== MyCache ==========")
	log.Println("Port:", cfg.Port)
	log.Println("MaxKeys:", cfg.MaxKeys)
	log.Println("AOF:", cfg.AOFFile)
	log.Println("AppendOnly:", cfg.AppendOnly)
	log.Println("=============================")
}
