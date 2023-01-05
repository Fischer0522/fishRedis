package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strconv"
	"strings"
)

func lIndexList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lindex" {
		dblog.Logger.Error("lIndex func:cmdName != lindex")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'lindex' command")
	}
	key := string(cmd[1])
	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("index is not an integer")
	}
	// not return is also ok
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	listVal, typeOk := val.(List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := listVal.index(index)
	if res == nil {
		return resp.MakeBulkData(nil)
	}
	return resp.MakeBulkData(res)
}

func lLenList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "llen" {
		dblog.Logger.Error("lLenList func :cmdName != llen")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'llen' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	listVal, typeOk := val.(List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.MakeIntData(int64(listVal.Length))

}

func lPushList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lpush" {
		dblog.Logger.Error("lPushList func:cmdName != lpush")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'lpush' command")
	}
	key := string(cmd[1])
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		list := NewList()
		m.db.Set(key, list)
	}
	// if key is not exist,get the empty list
	val, _ = m.db.Get(key)
	listVal, typeOk := val.(List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")
	}
	for i := 2; i < length; i++ {
		listVal.lPush(cmd[i])
	}
	return resp.MakeIntData(int64(listVal.Length))
}

func lPushxList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lpushx" {
		dblog.Logger.Error("lPushxList func : cmdName != lpushx")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'lpushx' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	listVal, typeOk := val.(List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for i := 2; i < length; i++ {
		listVal.lPush(cmd[i])
	}
	return resp.MakeIntData(int64(listVal.Length))
}

func lPopList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lpop" {
		dblog.Logger.Error("lPopList func: cmdName != lpop")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'lpop'")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	listVal, typeOk := val.(List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")
	}
	res := listVal.lPop()
	return resp.MakeBulkData(res)
}