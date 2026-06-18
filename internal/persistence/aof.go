package persistence

import (
	"bufio"
	"mycache/internal/cache"
	"os"
	"strings"
	"sync"
)

type AOF struct {
	file          *os.File
	mu            sync.Mutex
	rewriting     bool
	rewriteBuffer []string
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

	if a.rewriting {
		a.rewriteBuffer = append(a.rewriteBuffer, command)
	}

	_, err := a.file.WriteString(command + "\n")
	if err != nil {
		return err
	}
	return a.file.Sync()
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

func (a *AOF) Rewrite(c *cache.Cache) error {
	a.mu.Lock()
	a.rewriting = true
	a.rewriteBuffer = nil
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.rewriting = false
		a.rewriteBuffer = nil
		a.mu.Unlock()
	}()

	snapshot := c.Snapshot()

	tempFile, err := os.Create("temp.aof")
	if err != nil {
		return err
	}
	// We will manually close tempFile before rename to satisfy Windows locks

	writer := bufio.NewWriter(tempFile)

	for key, item := range snapshot {
		switch item.Type {
		case cache.StringType:
			value := item.Value.(string)
			if _, err := writer.WriteString("SET " + key + " " + value + "\n"); err != nil {
				tempFile.Close()
				return err
			}
		case cache.ListType:
			list := item.Value.([]string)
			for _, value := range list {
				if _, err := writer.WriteString("RPUSH " + key + " " + value + "\n"); err != nil {
					tempFile.Close()
					return err
				}
			}
		case cache.HashType:
			hash := item.Value.(map[string]string)
			for field, value := range hash {
				if _, err := writer.WriteString("HSET " + key + " " + field + " " + value + "\n"); err != nil {
					tempFile.Close()
					return err
				}
			}
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, cmd := range a.rewriteBuffer {
		if _, err := writer.WriteString(cmd + "\n"); err != nil {
			tempFile.Close()
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		tempFile.Close()
		return err
	}

	// Close the file so Windows releases the lock allowing us to rename it
	tempFile.Close()

	if err := a.file.Close(); err != nil {
		return err
	}

	if err := os.Rename("temp.aof", "appendonly.aof"); err != nil {
		return err
	}

	file, err := os.OpenFile(
		"appendonly.aof",
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return err
	}

	a.file = file

	return nil
}
