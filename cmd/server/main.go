package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"mycache/internal/cache"
	"mycache/internal/config"
	"mycache/internal/network"
	"mycache/internal/persistence"
)

func main() {

	// CLI Flags
	port := flag.String(
		"port",
		"",
		"Server Port",
	)

	password := flag.String(
		"password",
		"",
		"Server Password",
	)

	maxKeys := flag.Int(
		"maxkeys",
		0,
		"Maximum number of keys",
	)

	aofFile := flag.String(
		"aof",
		"",
		"AOF file path",
	)

	flag.Parse()

	// CLI flags override environment variables

	if *port != "" {
		os.Setenv(
			"MYCACHE_PORT",
			*port,
		)
	}

	if *password != "" {
		os.Setenv(
			"MYCACHE_PASSWORD",
			*password,
		)
	}

	if *maxKeys > 0 {
		os.Setenv(
			"MYCACHE_MAX_KEYS",
			strconv.Itoa(*maxKeys),
		)
	}

	if *aofFile != "" {
		os.Setenv(
			"MYCACHE_AOF_FILE",
			*aofFile,
		)
	}

	cfg := config.Load()

	err := os.MkdirAll("./data", 0755)

	if cfg.Password == "" {
		log.Fatal("password is required, use --password or MYCACHE_PASSWORD")
	}

	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"Starting MyCache on port %s",
		cfg.Port,
	)

	c := cache.New(
		cfg.MaxKeys,
	)

	aof, err := persistence.NewAOF(
		cfg.AOFFile,
	)

	if err != nil {
		log.Fatal(err)
	}

	if cfg.AppendOnly {

		err = aof.Replay(c)

		if err != nil {
			log.Fatal(err)
		}
	}

	network.Start(
		":"+cfg.Port,
		c,
		aof,
		cfg,
	)
}
