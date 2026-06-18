package config

import "os"

func Password() string {
	return os.Getenv("MYCACHE_PASSWORD")
}
