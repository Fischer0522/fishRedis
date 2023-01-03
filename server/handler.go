package server

import (
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
}
