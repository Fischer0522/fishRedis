package memdb

import (
	"fishRedis/resp"
	"net"
)

var CmdTable = make(map[string]*CmdExecutor)

type CmdExecutor func(client *RedisClient) resp.RedisData

func RegisterCommand(cmdName string, executor CmdExecutor) {
	CmdTable[cmdName] = &executor
}

type RedisClient struct {
	Args         [][]byte
	Flags        int
	RedisCommand *CmdExecutor
	OutputBuf    resp.RedisData
	Conn         net.Conn
	RedisDb      *MemDb
}

func NewRedisClient() *RedisClient {
	return &RedisClient{
		Args:         nil,
		Flags:        0,
		RedisCommand: nil,
		OutputBuf:    nil,
		Conn:         nil,
		RedisDb:      nil,
	}
}
