package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strings"
)

func hGetHash(mem *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hget" {
		dblog.Logger.Error("hGetHash func: cmdName != hget")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hget' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	mem.locks.RLock(key)
	defer mem.locks.Unlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val := hash.Get(field)
	return resp.MakeBulkData(val)
}

// set multiple key-val
// use HSET instead of HMSET
func hSetHash(mem *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hset" {
		dblog.Logger.Error("hSetHash func: cmdName != hset")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hset' command")
	}
	key := string(cmd[1])
	mem.CheckTTL(key)
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		newHash := NewHash()
		mem.db.Set(key, newHash)
	}
	temp, _ := mem.db.Get(key)
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := 0
	for i := 2; i < len(cmd); i += 2 {
		field := string(cmd[i])
		val := cmd[i+1]
		num := hash.Set(field, val)
		count += num
	}
	return resp.MakeIntData(int64(count))
}

func hDelHash(mem *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hdel" {
		dblog.Logger.Error("hDelHash func: cmdName != hdel")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hdel' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := 0
	for i := 2; i < len(cmd); i++ {
		keyToDel := string(cmd[i])
		num := hash.Del(keyToDel)
		count += num
	}
	return resp.MakeIntData(int64(count))
}

func hMgetHash(mem *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hmget" {
		dblog.Logger.Error("hMgetHash func: cmdName != hmget")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hmget' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	result := make([]resp.RedisData, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		field := string(cmd[i])
		res := hash.Get(field)
		result = append(result, resp.MakeBulkData(res))
	}

	return resp.MakeArrayData(result)
}
