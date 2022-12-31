package memdb

import "fishRedis/resp"

var cmdTable = make(map[string]*command)

type cmdExecutor func(m *MemDb, cmd [][]byte) resp.RedisData

type command struct {
	executor cmdExecutor
}

func RegisterCommand(cmdName string, executor cmdExecutor) {
	cmdTable[cmdName] = &command{
		executor: executor,
	}
}
