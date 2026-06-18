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

func handleConnection(conn net.Conn, cache *cache.Cache, aof *persistence.AOF) {
	authenticated := false

	scanner := bufio.NewScanner(conn)
	defer func() {
		cache.RemoveConnection(conn)
		conn.Close()
	}()

	for scanner.Scan() {
		command := scanner.Text()
		parts := strings.Fields(command)

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		if cmd != "AUTH" {

			if !authenticated {

				fmt.Fprintln(conn, "NOAUTH Authentication required")

				continue
			}
		}

		switch cmd {
		case "AUTH":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				if parts[1] != config.Password() {
					fmt.Fprintln(conn, "ERR Invalid Password")
					continue
				}

				authenticated = true

				fmt.Fprintln(conn, "+OK")
			}
		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			value, err := cache.Get(key)
			if err == nil {
				fmt.Fprintln(conn, value)
			} else {
				fmt.Fprintln(conn, err.Error())
			}

		case "SET":
			if len(parts) != 3 && len(parts) != 5 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			} else if len(parts) == 3 {
				key := parts[1]
				value := parts[2]
				cache.Set(key, value)
				err := aof.Append(
					"SET " + key + " " + value,
				)

				if err != nil {
					fmt.Fprintln(conn, "ERR Persistence Failure")
					continue
				}
				fmt.Fprintln(conn, "+OK")
			} else if len(parts) == 5 {
				key := parts[1]
				value := parts[2]
				if strings.ToUpper(parts[3]) == "EX" {
					ttl, err := strconv.Atoi(parts[4])
					if err != nil {
						fmt.Fprintln(conn, "ERR Invalid Command")
						continue
					} else {
						if ttl <= 0 {
							fmt.Fprintln(conn, "ERR Invalid Command")
							continue
						}
						cache.SetWithTTL(key, value, time.Duration(ttl)*time.Second)
						fmt.Fprintln(conn, "+OK")
					}
				} else {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}
			}

		case "DEL":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			cache.Delete(key)
			_ = aof.Append("DEL " + key)
			fmt.Fprintln(conn, "+OK")

		case "TTL":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			fmt.Fprintln(conn, cache.TTLleft(key))

		case "LPUSH":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			value := parts[2]
			err := cache.LPush(key, value)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			_ = aof.Append(
				"LPUSH " + key + " " + value,
			)
			fmt.Fprintln(conn, "+OK")

		case "LRANGE":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			list, err := cache.LRange(key)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			for _, value := range list {
				fmt.Fprintln(conn, value)
			}

		case "RPUSH":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			value := parts[2]
			err := cache.RPush(key, value)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			_ = aof.Append(
				"RPUSH " + key + " " + value,
			)
			fmt.Fprintln(conn, "+OK")

		case "LPOP":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			value, err := cache.LPop(key)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			fmt.Fprintln(conn, value)

		case "RPOP":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			value, err := cache.RPop(key)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			fmt.Fprintln(conn, value)

		case "HSET":
			if len(parts) != 4 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			field := parts[2]
			value := parts[3]
			err := cache.HSet(key, field, value)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			_ = aof.Append(
				"HSET " + key +
					" " + field +
					" " + value,
			)
			fmt.Fprintln(conn, "+OK")

		case "HGET":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			field := parts[2]
			value, err := cache.HGet(key, field)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			fmt.Fprintln(conn, value)

		case "HDEL":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			field := parts[2]
			err := cache.HDel(key, field)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			_ = aof.Append(
				"HDEL " + key +
					" " + field,
			)
			fmt.Fprintln(conn, "+OK")

		case "HGETALL":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			key := parts[1]
			hash, err := cache.HGetAll(key)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
				continue
			}
			for field, value := range hash {
				fmt.Fprintln(conn, field, value)
			}

		case "EXISTS":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			if cache.Exists(parts[1]) {
				fmt.Fprintln(conn, "1")
			} else {
				fmt.Fprintln(conn, "0")
			}

		case "TYPE":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			fmt.Fprintln(conn, cache.Type(parts[1]))

		case "LLEN":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			l, err := cache.LLen(parts[1])
			if err != nil {
				fmt.Fprintln(conn, err.Error())
			} else {
				fmt.Fprintln(conn, l)
			}

		case "HLEN":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR Invalid Command")
				continue
			}
			l, err := cache.HLen(parts[1])
			if err != nil {
				fmt.Fprintln(conn, err.Error())
			} else {
				fmt.Fprintln(conn, l)
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
			fmt.Fprintln(conn, helpText)
		case "SUBSCRIBE":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				channel := parts[1]

				cache.Subscribe(channel, conn)

				fmt.Fprintln(conn, "SUBSCRIBED", channel)
			}
		case "UNSUBSCRIBE":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				channel := parts[1]

				cache.Unsubscribe(channel, conn)

				fmt.Fprintln(conn, "UNSUBSCRIBED", channel)
			}
		case "PUBLISH":
			{
				if len(parts) != 3 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				channel := parts[1]
				message := parts[2]

				cache.Publish(channel, message)

				fmt.Fprintln(conn, "+OK")
			}
		case "SUBCOUNT":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				channel := parts[1]

				fmt.Fprintln(conn, cache.SubscriberCount(channel))
			}
		case "CHANNELS":
			{
				if len(parts) != 1 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				channels := cache.Channels()

				for _, channel := range channels {
					fmt.Fprintln(conn, channel)
				}
			}
		case "REWRITEAOF":
			{
				if len(parts) != 1 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				err := aof.Rewrite(cache)

				if err != nil {
					fmt.Fprintln(conn, err.Error())
					continue
				}

				fmt.Fprintln(conn, "+OK")
			}
		default:
			fmt.Fprintln(conn, "ERR Invalid Command")
		}
	}
}

func Start(port string, cache *cache.Cache, aof *persistence.AOF) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Server listening on", port)
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
			} else {
				go handleConnection(conn, cache, aof)
			}
		}
	}
}
