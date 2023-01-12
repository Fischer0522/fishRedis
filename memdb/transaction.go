package memdb

import "fishRedis/resp"

var TransactionTable = make(map[string]null)

func InitTransactionTable() {
	TransactionTable["multi"] = null{}
	TransactionTable["exec"] = null{}
	TransactionTable["discard"] = null{}
	TransactionTable["watch"] = null{}
}

type MultiCmd struct {
	argv         [][]byte
	redisCommand *CmdExecutor
}

type MultiState struct {
	commands []MultiCmd
	count    int
}

func NewMultiState() MultiState {
	return MultiState{
		commands: make([]MultiCmd, 0),
		count:    0,
	}
}

func (m *MultiState) AddCommandToBuf(cmd [][]byte, executor *CmdExecutor) {
	multiCmd := MultiCmd{
		argv:         cmd,
		redisCommand: executor,
	}
	m.commands = append(m.commands, multiCmd)
	m.count++
}

func (m *MultiState) PopCommandFromHead() MultiCmd {
	command := m.commands[0]
	m.commands = m.commands[1:]
	m.count--
	return command
}

func multiTrans(client *RedisClient) resp.RedisData {
	client.Flags |= REDIS_MULTI
	return resp.MakeStringData("OK")
}

func RegisterTransactionCommand() {
	RegisterCommand("multi", multiTrans)
}
