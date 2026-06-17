package persistence

import (
	"bufio"
	"mycache/internal/cache"
	"os"
	"strings"
	"sync"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

func NewAOF(path string) (*AOF, error) {

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &AOF{
		file: file,
	}, nil
}

func (a *AOF) Append(command string) error {

	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.WriteString(command + "\n")

	return a.file.Sync()

	return err
}

func (a *AOF) Replay(c *cache.Cache) error {

	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Seek(0, 0)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(a.file)

	for scanner.Scan() {

		line := scanner.Text()

		parts := strings.Fields(line)

		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {

		case "SET":
			if len(parts) == 3 {
				c.Set(parts[1], parts[2])
			}

		case "DEL":
			if len(parts) == 2 {
				c.Delete(parts[1])
			}

		case "LPUSH":
			if len(parts) == 3 {
				_ = c.LPush(parts[1], parts[2])
			}

		case "RPUSH":
			if len(parts) == 3 {
				_ = c.RPush(parts[1], parts[2])
			}

		case "LPOP":
			if len(parts) == 2 {
				_, _ = c.LPop(parts[1])
			}

		case "RPOP":
			if len(parts) == 2 {
				_, _ = c.RPop(parts[1])
			}

		case "HSET":
			if len(parts) == 4 {
				_ = c.HSet(parts[1], parts[2], parts[3])
			}

		case "HDEL":
			if len(parts) == 3 {
				_ = c.HDel(parts[1], parts[2])
			}
		}
	}

	return scanner.Err()
}

func (a *AOF) Close() error {

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.file.Close()
}
