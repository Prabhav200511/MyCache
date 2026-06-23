package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port       string
	Password   string
	MaxKeys    int
	AOFFile    string
	HTTPPort   string
	AppendOnly bool
}

func Load() *Config {

	maxKeys := 10000

	if value := os.Getenv("MYCACHE_MAX_KEYS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			maxKeys = parsed
		}
	}

	appendOnly := true

	if value := os.Getenv("MYCACHE_APPENDONLY"); value == "false" {
		appendOnly = false
	}

	return &Config{
		Port:     getEnv("MYCACHE_PORT", "6380"),
		Password: getEnv("MYCACHE_PASSWORD", ""),
		MaxKeys:  maxKeys,
		AOFFile: getEnv(
			"MYCACHE_AOF_FILE",
			"./data/appendonly.aof",
		),
		HTTPPort:   getEnv("MYCACHE_HTTP_PORT", "8080"),
		AppendOnly: appendOnly,
	}
}

func getEnv(key string, fallback string) string {

	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}
