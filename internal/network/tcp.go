package network

import (
	"bufio"
	"fmt"
	"log"
	"mycache/internal/cache"
	"mycache/internal/config"
	"mycache/internal/persistence"
	"net"
	"strconv"
	"strings"
	"time"
)

func handleConnection(conn net.Conn, cache *cache.Cache, aof *persistence.AOF, cfg *config.Config) {
	authenticated := false

	reader := bufio.NewReader(conn)

	defer func() {
		cache.RemoveConnection(conn)
		conn.Close()
	}()

	for {
		var (
			parts  []string
			err    error
			isRESP bool
		)

		b, err := reader.Peek(1)
		if err != nil {
			return
		}

		if b[0] == '*' {
			isRESP = true
			parts, err = ParseRESP(reader)
			if err != nil {
				WriteError(conn, "invalid RESP")
				continue
			}
		} else {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts = strings.Fields(line)
		}

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		if cmd != "AUTH" && cmd != "QUIT" && cmd != "ECHO" && cmd != "INFO" && cmd != "PING" && cmd != "HELLO" && cmd != "CLIENT" && cmd != "COMMAND" {
			if !authenticated {
				if isRESP {
					WriteError(conn, "NOAUTH Authentication required")
				} else {
					fmt.Fprintln(conn, "-NOAUTH Authentication required")
				}
				continue
			}
		}

		switch cmd {
		case "AUTH":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			if parts[1] != cfg.Password {
				if isRESP {
					WriteError(conn, "ERR Invalid Password")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Password")
				}
				continue
			}

			authenticated = true

			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "GET":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			value, err := cache.Get(key)

			if err != nil {
				if isRESP {
					WriteNull(conn)
				} else {
					fmt.Fprintln(conn, "NULL")
				}
				continue
			}

			if isRESP {
				WriteBulkString(conn, value)
			} else {
				fmt.Fprintln(conn, value)
			}

		case "SET":
			if len(parts) != 3 && len(parts) != 5 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			} else if len(parts) == 3 {
				key := parts[1]
				value := parts[2]
				cache.Set(key, value)
				err := aof.Append("SET " + key + " " + value)

				if err != nil {
					if isRESP {
						WriteError(conn, "ERR Persistence Failure")
					} else {
						fmt.Fprintln(conn, "ERR Persistence Failure")
					}
					continue
				}

				if isRESP {
					WriteSimpleString(conn, "OK")
				} else {
					fmt.Fprintln(conn, "+OK")
				}

			} else if len(parts) == 5 {
				key := parts[1]
				value := parts[2]
				if strings.ToUpper(parts[3]) == "EX" {
					ttl, err := strconv.Atoi(parts[4])
					if err != nil || ttl <= 0 {
						if isRESP {
							WriteError(conn, "ERR Invalid Command")
						} else {
							fmt.Fprintln(conn, "ERR Invalid Command")
						}
						continue
					}

					cache.SetWithTTL(key, value, time.Duration(ttl)*time.Second)
					_ = aof.Append("SET " + key + " " + value + " EX " + strconv.Itoa(ttl))

					if isRESP {
						WriteSimpleString(conn, "OK")
					} else {
						fmt.Fprintln(conn, "+OK")
					}
				} else {
					if isRESP {
						WriteError(conn, "ERR Invalid Command")
					} else {
						fmt.Fprintln(conn, "ERR Invalid Command")
					}
					continue
				}
			}

		case "EXPIRE":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			ttl, err := strconv.Atoi(parts[2])
			if err != nil {
				if isRESP {
					WriteError(conn, "ERR value is not an integer or out of range")
				} else {
					fmt.Fprintln(conn, "ERR value is not an integer")
				}
				continue
			}
			// Assuming cache.Expire returns 1 if timeout was set, 0 if key doesn't exist
			res := cache.Expire(key, time.Duration(ttl)*time.Second)
			_ = aof.Append("EXPIRE " + key + " " + parts[2])
			if isRESP {
				WriteInteger(conn, res)
			} else {
				fmt.Fprintln(conn, res)
			}

		case "PERSIST":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			// Assuming cache.Persist returns 1 if timeout was removed, 0 if key doesn't exist or has no timeout
			res := cache.Persist(key)
			_ = aof.Append("PERSIST " + key)
			if isRESP {
				WriteInteger(conn, res)
			} else {
				fmt.Fprintln(conn, res)
			}

		case "DBSIZE":
			if len(parts) != 1 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			size := cache.DBSize()
			if isRESP {
				WriteInteger(conn, size)
			} else {
				fmt.Fprintln(conn, size)
			}

		case "KEYS":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			keys := cache.Keys(parts[1])
			if isRESP {
				WriteArray(conn, keys)
			} else {
				for _, k := range keys {
					fmt.Fprintln(conn, k)
				}
			}

		case "SELECT":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "OK")
			}

		case "DEL":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			deleted := 0
			if cache.Exists(key) {
				deleted = 1
			}

			cache.Delete(key)
			_ = aof.Append("DEL " + key)

			if isRESP {
				WriteInteger(conn, deleted)
			} else {
				fmt.Fprintln(conn, deleted)
			}

		case "TTL":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			ttl := cache.TTLleft(key)
			if isRESP {
				WriteInteger(conn, int(ttl))
			} else {
				fmt.Fprintln(conn, ttl)
			}

		case "LPUSH":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			value := parts[2]
			err := cache.LPush(key, value)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			_ = aof.Append("LPUSH " + key + " " + value)
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "LRANGE":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			list, err := cache.LRange(key)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			if isRESP {
				WriteArray(conn, list)
			} else {
				for _, value := range list {
					fmt.Fprintln(conn, value)
				}
			}

		case "RPUSH":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			value := parts[2]
			err := cache.RPush(key, value)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			_ = aof.Append("RPUSH " + key + " " + value)
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "LPOP":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			value, err := cache.LPop(key)
			if err != nil {
				if isRESP {
					WriteNull(conn)
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}

			_ = aof.Append("LPOP " + key)

			if isRESP {
				WriteBulkString(conn, value)
			} else {
				fmt.Fprintln(conn, value)
			}

		case "RPOP":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			value, err := cache.RPop(key)
			if err != nil {
				if isRESP {
					WriteNull(conn)
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}

			_ = aof.Append("RPOP " + key)

			if isRESP {
				WriteBulkString(conn, value)
			} else {
				fmt.Fprintln(conn, value)
			}

		case "HSET":
			if len(parts) != 4 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			field := parts[2]
			value := parts[3]
			err := cache.HSet(key, field, value)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			_ = aof.Append("HSET " + key + " " + field + " " + value)
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "HGET":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			field := parts[2]
			value, err := cache.HGet(key, field)
			if err != nil {
				if isRESP {
					WriteNull(conn)
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			if isRESP {
				WriteBulkString(conn, value)
			} else {
				fmt.Fprintln(conn, value)
			}

		case "HDEL":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			field := parts[2]
			err := cache.HDel(key, field)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}
			_ = aof.Append("HDEL " + key + " " + field)
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "HGETALL":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			key := parts[1]
			hash, err := cache.HGetAll(key)
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}

			if isRESP {
				var arr []string
				for field, value := range hash {
					arr = append(arr, field, value)
				}
				WriteArray(conn, arr)
			} else {
				for field, value := range hash {
					fmt.Fprintln(conn, field, value)
				}
			}

		case "EXISTS":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			exists := cache.Exists(parts[1])
			if isRESP {
				if exists {
					WriteInteger(conn, 1)
				} else {
					WriteInteger(conn, 0)
				}
			} else {
				if exists {
					fmt.Fprintln(conn, "1")
				} else {
					fmt.Fprintln(conn, "0")
				}
			}

		case "TYPE":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			if isRESP {
				WriteBulkString(conn, cache.Type(parts[1]))
			} else {
				fmt.Fprintln(conn, cache.Type(parts[1]))
			}

		case "LLEN":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			l, err := cache.LLen(parts[1])
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
			} else {
				if isRESP {
					WriteInteger(conn, int(l))
				} else {
					fmt.Fprintln(conn, l)
				}
			}

		case "HLEN":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}
			l, err := cache.HLen(parts[1])
			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
			} else {
				if isRESP {
					WriteInteger(conn, int(l))
				} else {
					fmt.Fprintln(conn, l)
				}
			}

		case "HELP":
			helpText := `GET key
SET key value
SET key value EX seconds
DEL key
TTL key
LPUSH key value
RPUSH key value
LRANGE key
LPOP key
RPOP key
HSET key field value
HGET key field
HDEL key field
HGETALL key
EXISTS key
TYPE key
LLEN key
HLEN key`
			if isRESP {
				WriteBulkString(conn, helpText)
			} else {
				fmt.Fprintln(conn, helpText)
			}

		case "SUBSCRIBE":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			channel := parts[1]
			cache.Subscribe(channel, conn)

			if isRESP {
				fmt.Fprintf(conn, "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
			} else {
				fmt.Fprintln(conn, "SUBSCRIBED", channel)
			}

		case "UNSUBSCRIBE":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			channel := parts[1]
			cache.Unsubscribe(channel, conn)

			if isRESP {
				fmt.Fprintf(conn, "*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
			} else {
				fmt.Fprintln(conn, "UNSUBSCRIBED", channel)
			}

		case "PUBLISH":
			if len(parts) != 3 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			channel := parts[1]
			message := parts[2]
			cache.Publish(channel, message)

			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "SUBCOUNT":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			channel := parts[1]
			if isRESP {
				WriteInteger(conn, int(cache.SubscriberCount(channel)))
			} else {
				fmt.Fprintln(conn, cache.SubscriberCount(channel))
			}

		case "CHANNELS":
			if len(parts) != 1 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			channels := cache.Channels()
			if isRESP {
				WriteArray(conn, channels)
			} else {
				for _, channel := range channels {
					fmt.Fprintln(conn, channel)
				}
			}

		case "REWRITEAOF":
			if len(parts) != 1 {
				if isRESP {
					WriteError(conn, "ERR Invalid Command")
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
				}
				continue
			}

			err := aof.Rewrite(cache)

			if err != nil {
				if isRESP {
					WriteError(conn, err.Error())
				} else {
					fmt.Fprintln(conn, err.Error())
				}
				continue
			}

			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "+OK")
			}

		case "PING":
			if isRESP {
				WriteSimpleString(conn, "PONG")
			} else {
				fmt.Fprintln(conn, "PONG")
			}

		case "HELLO":
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "OK")
			}

		case "CLIENT":
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "OK")
			}

		case "COMMAND":
			WriteNull(conn)

		case "QUIT":
			if isRESP {
				WriteSimpleString(conn, "OK")
			} else {
				fmt.Fprintln(conn, "OK")
			}
			return

		case "ECHO":
			if len(parts) != 2 {
				if isRESP {
					WriteError(conn, "ERR wrong number of arguments")
				} else {
					fmt.Fprintln(conn, "ERR wrong number of arguments")
				}
				continue
			}
			if isRESP {
				WriteBulkString(conn, parts[1])
			} else {
				fmt.Fprintln(conn, parts[1])
			}

		case "INFO":
			if isRESP {
				WriteBulkString(conn, "redis_version:mycache-1.0\r\n")
			} else {
				fmt.Fprintln(conn, "redis_version:mycache-1.0")
			}
		case "VERSION":

			if isRESP {
				WriteBulkString(
					conn,
					"mycache-1.0.0",
				)
			} else {
				fmt.Fprintln(
					conn,
					"mycache-1.0.0",
				)
			}
		case "SAVE":

			err := aof.Rewrite(cache)

			if err != nil {

				WriteError(
					conn,
					err.Error(),
				)

				continue
			}

			WriteSimpleString(
				conn,
				"OK",
			)

		default:
			if isRESP {
				WriteError(conn, "ERR unknown command")
			} else {
				fmt.Fprintln(conn, "ERR Invalid Command")
			}
		}
	}
}

func Start(port string, cache *cache.Cache, aof *persistence.AOF, cfg *config.Config) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Server listening on", port)
		fmt.Println(cfg.Password)
		fmt.Println("Check Check")
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
			} else {
				go handleConnection(conn, cache, aof, cfg)
			}
		}
	}
}
