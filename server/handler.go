package server

import (
	"bufio"
	"fishRedis/memdb"
	"fmt"
	"net"
)

type Handler struct {
	memdb *memdb.MemDb
}

func NewHandler() *Handler {
	return &Handler{
		memdb: memdb.NewMemdb(),
	}
}

func (h *Handler) handle(conn net.Conn) {
	fmt.Println("handling")
	input := bufio.NewScanner(conn)
	for input.Scan() {
		fmt.Println(input.Text())
	}
}
