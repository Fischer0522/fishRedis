package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strings"
)

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
	cmdName := strings.ToLower(string(client.Args[0]))
	if cmdName != "multi" {
		dblog.Logger.Error("multiTrans func: cmdName != multi")
		return resp.MakeErrorData("server error")
	}
	if client.Flags&REDIS_MULTI == REDIS_MULTI {
		return resp.MakeErrorData("MULTI calls can not be nested")
	}
	client.Flags |= REDIS_MULTI
	return resp.MakeStringData("OK")
}

func execTrans(client *RedisClient) resp.RedisData {
	cmdName := strings.ToLower(string(client.Args[0]))
	if cmdName != "exec" {
		dblog.Logger.Error("execTrans func: cmdName != exec")
		return resp.MakeErrorData("server error")
	}
	if client.Flags&REDIS_MULTI != REDIS_MULTI {
		return resp.MakeErrorData("EXEC without MULTI")
	}
	client.Flags &= ^REDIS_MULTI
	if client.Flags&REDIS_DIRTY_CAS == REDIS_DIRTY_CAS {
		// clear all commands and refuse the transaction
		for client.Mstate.count > 0 {
			client.Mstate.PopCommandFromHead()
		}
		return resp.MakeBulkData(nil)
	}

	if client.Mstate.count != 0 {
		resArr := make([]resp.RedisData, 0, client.Mstate.count)
		for client.Mstate.count > 0 {
			command := client.Mstate.PopCommandFromHead()
			cmd := command.argv
			execFunc := *command.redisCommand
			client.Args = cmd
			res := execFunc(client)
			resArr = append(resArr, res)
		}
		client.Mstate.count = 0
		return resp.MakeArrayData(resArr)
	}
	genericUnwatch(client)
	return resp.MakeStringData("(empty list or set)")
}

func discardTrans(client *RedisClient) resp.RedisData {
	cmd := client.Args
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "discard" {
		dblog.Logger.Error("discardTrans func: cmdName != discard")
		return resp.MakeErrorData("server error")
	}

	if client.Flags&REDIS_MULTI != REDIS_MULTI {
		return resp.MakeErrorData("DISCARD without MUlTI")
	}
	client.Flags &= ^REDIS_MULTI
	for client.Mstate.count > 0 {
		client.Mstate.PopCommandFromHead()
	}
	genericUnwatch(client)
	return resp.MakeStringData("OK")
}

func watchTrans(client *RedisClient) resp.RedisData {
	cmdName := strings.ToLower(string(client.Args[0]))
	key := strings.ToLower(string(client.Args[1]))
	if cmdName != "watch" {
		dblog.Logger.Error("watchTrans func: cmdName != watch")
		return resp.MakeErrorData("server error")
	}
	if client.Flags&REDIS_MULTI == REDIS_MULTI {
		return resp.MakeErrorData("WATCH inside MULTI is not allowed")
	}
	watchedKeys := client.RedisDb.watchKeys
	temp, ok := watchedKeys.Get(key)
	if !ok {
		set := make(map[*RedisClient]null)
		set[client] = null{}
		watchedKeys.Set(key, set)
	} else {
		set, typeOK := temp.(map[*RedisClient]null)
		if !typeOK {
			dblog.Logger.Error("wrong type")
			return resp.MakeErrorData("server error")
		}
		set[client] = null{}
	}

	return resp.MakeStringData("OK")
}

// unwatch all keys
func genericUnwatch(client *RedisClient) {
	for _, key := range client.RedisDb.watchKeys.Keys() {
		temp, ok := client.RedisDb.watchKeys.Get(key)
		if !ok {
			return
		}
		set := temp.(map[*RedisClient]null)
		delete(set, client)
	}

}
func unwatchTrans(client *RedisClient) resp.RedisData {
	cmdName := strings.ToLower(string(client.Args[0]))
	if cmdName != "unwatch" {
		dblog.Logger.Error("unwatchTrans func: cmdName != unwatch")
		return resp.MakeErrorData("server error")
	}
	genericUnwatch(client)
	return resp.MakeStringData("OK")

}

func RegisterTransactionCommand() {
	RegisterCommand("multi", multiTrans)
	RegisterCommand("exec", execTrans)
	RegisterCommand("discard", discardTrans)
	RegisterCommand("watch", watchTrans)
}
