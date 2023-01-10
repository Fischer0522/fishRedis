package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"github.com/gobwas/glob"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func pingKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "ping" {
		dblog.Logger.Error("pingKey func: cmdName != ping")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) > 2 {
		return resp.MakeErrorData("wrong number of arguments for 'ping' command")
	}
	if len(cmd) < 2 {
		return resp.MakeStringData("PONG")
	}
	return resp.MakeBulkData(cmd[1])
}
func delKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "del" {
		dblog.Logger.Error("delKey func: cmdName != del")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("wrong number of arguments for 'del' command")
	}
	count := 0
	for i := 1; i < len(cmd); i++ {
		key := string(cmd[i])
		mem.locks.Lock(key)
		res := mem.db.Delete(key)
		mem.ttlKeys.Delete(key)
		count += res
	}
	return resp.MakeIntData(int64(count))
}
func existsKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "exists" {
		dblog.Logger.Error("existsKey func: cmdName != exist")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("wrong number of arguments for 'exists' command")
	}
	count := 0
	for i := 1; i < len(cmd); i++ {
		key := strings.ToLower(string(cmd[i]))
		mem.CheckTTL(key)
		mem.locks.RLock(key)
		_, ok := mem.db.Get(key)
		if ok {
			count++
		}
		mem.locks.RUnlock(key)
	}
	return resp.MakeIntData(int64(count))
}
func expireKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "expire" {
		dblog.Logger.Error("expireKey func: cmdName != expire")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 && len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'expire' commands")
	}
	key := string(cmd[1])
	seconds, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	arg := "default"
	if len(cmd) == 4 {
		arg = string(cmd[3])
	}
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	var result int
	ttl := time.Now().Unix() + seconds
	switch arg {
	case "nx":
		_, ok := mem.ttlKeys.Get(key)
		if !ok {
			result = mem.SetTTL(key, ttl)
		}
	case "xx":
		_, ok := mem.ttlKeys.Get(key)
		if ok {
			result = mem.SetTTL(key, ttl)
		}
	case "gt":
		val, ok := mem.ttlKeys.Get(key)
		if ok && ttl > val.(int64) {
			result = mem.SetTTL(key, ttl)
		}
	case "lt":
		val, ok := mem.ttlKeys.Get(key)
		if ok && ttl < val.(int64) {
			result = mem.SetTTL(key, ttl)
		}
	default:
		if arg != "default" {
			return resp.MakeErrorData("Unsupported option " + arg)
		}
		result = mem.SetTTL(key, ttl)
	}
	return resp.MakeIntData(int64(result))
}

// TODO after finish the glob parse
func keysKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "keys" {
		dblog.Logger.Error("keysKey func: cmdName != keys")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'keys' command")
	}
	pattern := string(cmd[1])
	var g glob.Glob
	g = glob.MustCompile(pattern)
	allKeys := mem.db.Keys()
	res := make([]resp.RedisData, 0)
	for _, key := range allKeys {
		if !mem.CheckTTL(key) {
			continue
		}
		if g.Match(key) {
			res = append(res, resp.MakeBulkData([]byte(key)))
		}
	}

	return resp.MakeArrayData(res)
}

func persistKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "persist" {
		dblog.Logger.Error("persistKey func: cmdName != persist")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'persist' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	res := mem.ttlKeys.Delete(key)
	return resp.MakeIntData(int64(res))
}

// maybe not so random...
func randomKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "randomkey" {
		dblog.Logger.Error("raddomKey func: cmdName != randomKey")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 1 {
		return resp.MakeErrorData("wrong number of arguments for 'randomkey' command")
	}
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(mem.db.size)
	for i := r; i < mem.db.size; i++ {
		slot := mem.db.table[i]
		if len(slot.mp) != 0 {
			slot.rwMu.RLock()
			for key := range slot.mp {
				slot.rwMu.RUnlock()
				return resp.MakeBulkData([]byte(key))
			}
			slot.rwMu.RUnlock()
		}
	}
	for i := r; i >= 0; i-- {
		slot := mem.db.table[i]
		if len(slot.mp) != 0 {
			slot.rwMu.RLock()
			for key := range slot.mp {
				slot.rwMu.RUnlock()
				return resp.MakeBulkData([]byte(key))
			}
			slot.rwMu.RUnlock()
		}
	}
	return resp.MakeBulkData(nil)
}

func renameKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "rename" {
		dblog.Logger.Error("renameKey func: cmdName != rename")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'rename' command")
	}
	key := string(cmd[1])
	newKey := string(cmd[2])
	if !mem.CheckTTL(key) {
		return resp.MakeErrorData("no such key")
	}
	keysToLock := []string{key, newKey}
	mem.locks.LockMulti(keysToLock)
	defer mem.locks.UnlockMulti(keysToLock)
	oldVal, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeErrorData("no such key")
	}
	mem.db.Delete(key)
	mem.DeleteTTL(key)
	mem.db.Delete(newKey)
	mem.DeleteTTL(newKey)
	mem.db.Set(newKey, oldVal)
	return resp.MakeStringData("OK")
}

func ttlKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "ttl" {
		dblog.Logger.Error("ttlKey func: cmdName != ttl")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'ttl' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(-2)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(-2)
	}
	ttl, ok := mem.ttlKeys.Get(key)
	if !ok {
		return resp.MakeIntData(-1)
	}
	now := time.Now().Unix()
	return resp.MakeIntData(ttl.(int64) - now)
}

// support string,list,hash,set
func typeKey(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "type" {
		dblog.Logger.Error("typeKey func: cmdName != type")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'type' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeStringData("none")
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	val, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeStringData("none")
	}
	switch val.(type) {
	case []byte:
		return resp.MakeStringData("string")
	case Set:
		return resp.MakeStringData("set")
	case *List:
		return resp.MakeStringData("list")
	case Hash:
		return resp.MakeStringData("Hash")
	default:
		return resp.MakeStringData("only support string,list,set and hash")
	}
}

func RegisterKeyCommand() {
	RegisterCommand("ping", pingKey)
	RegisterCommand("del", delKey)
	RegisterCommand("exists", existsKey)
	RegisterCommand("expire", expireKey)
	RegisterCommand("keys", keysKey)
	RegisterCommand("persist", persistKey)
	RegisterCommand("randomkey", randomKey)
	RegisterCommand("rename", renameKey)
	RegisterCommand("ttl", ttlKey)
	RegisterCommand("type", typeKey)
}
