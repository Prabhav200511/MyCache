package network

import (
	"bufio"
	"fmt"
	"log"
	"mycache/internal/cache"
	"net"
	"strconv"
	"strings"
	"time"
)

func handleConnection(conn net.Conn, cache *cache.Cache) {

	scanner := bufio.NewScanner(conn)
	defer conn.Close()

	for scanner.Scan() {
		command := scanner.Text()

		parts := strings.Fields(command)

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "GET":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}
				key := parts[1]

				value, ok := cache.Get(key)

				if ok {
					fmt.Fprintln(conn, value)
				} else {
					fmt.Fprintln(conn, "NULL")
				}

			}
		case "SET":
			{
				if len(parts) != 3 && len(parts) != 5 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				} else if len(parts) == 3 {
					key := parts[1]
					value := parts[2]

					cache.Set(key, value)
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
			}
		case "DEL":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				key := parts[1]

				cache.Delete(key)
				fmt.Fprintln(conn, "+OK")

			}
		case "TTL":
			{
				if len(parts) != 2 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}

				key := parts[1]

				fmt.Fprintln(conn, cache.TTLleft(key))
			}
		case "LPUSH":
			{
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

				fmt.Fprintln(conn, "+OK")
			}
		case "LRANGE":
			{
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
			}
		case "RPUSH":
			{
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

				fmt.Fprintln(conn, "+OK")
			}
		case "LPOP":
			{
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
			}
		case "RPOP":
			{
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
			}
		default:
			{
				fmt.Fprintln(conn, "ERR Invalid Command")
			}
		}
	}

}

func Start(port string, cache *cache.Cache) {

	ln, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatal(err)
	} else {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Server listening on", port)
				go handleConnection(conn, cache)
			}
		}
	}

}
