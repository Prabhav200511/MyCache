package network

import (
	"bufio"
	"fmt"
	"mycache/internal/cache"
	"net"
	"strings"
)

func handleConnection(conn net.Conn, cache *cache.Cache) {

	scanner := bufio.NewScanner(conn)
	defer conn.Close()

	line := ""

	for scanner.Scan() {
		command := scanner.Text()
		line += command

		parts := strings.Fields(line)

		if parts[0] == "GET" {
			if len(parts) != 2 {
				fmt.Println("Incomplete GET statement")
				return
			}
			key := parts[1]

			value, ok := cache.Get(key)

			if ok {
				fmt.Fprintln(conn, value)
			} else {
				fmt.Fprintln(conn, "NULL")
			}

		} else if parts[0] == "SET" {
			if len(parts) != 3 {
				fmt.Println("Incomplete SET statement")
				return
			}
			key := parts[1]
			value := parts[2]

			cache.Set(key, value)
			fmt.Fprintln(conn, "+OK")

		} else if parts[0] == "DELETE" {
			if len(parts) != 2 {
				fmt.Println("Incomplete DELETE statement")
				return
			}

			key := parts[1]

			cache.Delete(key)
			fmt.Fprintln(conn, "+OK")

		} else {
			fmt.Println("Invalid command")
		}
	}

}

func Start(port string, cache *cache.Cache) {

	ln, err := net.Listen("tcp", port)

	if err != nil {
		fmt.Println(err)
	} else {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
			} else {
				go handleConnection(conn, cache)
			}
		}
	}

}
