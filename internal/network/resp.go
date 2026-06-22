package network

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

func ParseRESP(reader *bufio.Reader) ([]string, error) {

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)

	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("invalid array")
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	parts := make([]string, 0, count)

	for i := 0; i < count; i++ {

		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		header = strings.TrimSpace(header)

		if len(header) == 0 || header[0] != '$' {
			return nil, errors.New("invalid bulk string")
		}

		size, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}

		buf := make([]byte, size+2)

		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		parts = append(parts, string(buf[:size]))
	}

	return parts, nil
}

func WriteSimpleString(conn net.Conn, value string) {
	fmt.Fprintf(conn, "+%s\r\n", value)
}

func WriteError(conn net.Conn, msg string) {
	fmt.Fprintf(conn, "-%s\r\n", msg)
}

func WriteBulkString(conn net.Conn, value string) {
	fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)
}

func WriteNull(conn net.Conn) {
	fmt.Fprint(conn, "$-1\r\n")
}

func WriteInteger(conn net.Conn, value int) {
	fmt.Fprintf(conn, ":%d\r\n", value)
}

func WriteArray(conn net.Conn, values []string) {
	fmt.Fprintf(conn, "*%d\r\n", len(values))
	for _, value := range values {
		fmt.Fprintf(
			conn,
			"$%d\r\n%s\r\n",
			len(value),
			value,
		)
	}
}
