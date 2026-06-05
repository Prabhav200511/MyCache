package network

import (
	"bufio"
	"fmt"
	"log"
	"mycache/internal/cache"
	"net"
	"strings"
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
				if len(parts) != 3 {
					fmt.Fprintln(conn, "ERR Invalid Command")
					continue
				}
				key := parts[1]
				value := parts[2]

				cache.Set(key, value)
				fmt.Fprintln(conn, "+OK")

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
