package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strconv"
	"strings"
)

func hGetHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
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
	defer mem.locks.RUnlock(key)
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
func hSetHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
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

func hDelHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
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
	defer func() {
		if hash.Len() == 0 {
			mem.DeleteTTL(key)
			mem.db.Delete(key)
		}
	}()
	return resp.MakeIntData(int64(count))
}

func hMgetHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
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
func hSetnxHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hsetnx" {
		dblog.Logger.Error("hSetnxHash func: cmdName != hsetnx")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hsetnx' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	value := cmd[3]
	mem.CheckTTL(key)
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		hash := NewHash()
		mem.db.Set(key, hash)
	}
	temp, _ := mem.db.Get(key)
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.Get(field)
	if len(res) != 0 {
		return resp.MakeIntData(0)
	}
	hash.Set(field, value)
	return resp.MakeIntData(1)
}

func hExistsHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hexists" {
		dblog.Logger.Error("hexistHash func: cmdName !=hexists")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hexists' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.Get(field)
	if len(res) == 0 {
		return resp.MakeIntData(0)
	}
	return resp.MakeIntData(1)
}

func hGetAllHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hgetall" {
		dblog.Logger.Error("hGetAllHash func: cmdName != hgetall")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hgetall' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	hash, typeok := temp.(Hash)
	if !typeok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.KeysAndVals()
	if len(res) == 0 {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	result := make([]resp.RedisData, 0, len(res))
	for _, kv := range res {
		result = append(result, resp.MakeBulkData(kv))
	}
	return resp.MakeArrayData(result)
}

func hIncrByHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hincrby" {
		dblog.Logger.Error("hIncrByHash func: cmdName != hincrby")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hincrby' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	increment, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer")
	}
	mem.CheckTTL(key)
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		hash := NewHash()
		mem.db.Set(key, hash)
	}
	temp, _ := mem.db.Get(key)
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res, err := hash.IncrBy(field, increment)
	if err != nil {
		return resp.MakeErrorData("hash value is not an integer")
	}
	return resp.MakeIntData(int64(res))
}

func hIncrByFloatHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hincrbyfloat" {
		dblog.Logger.Error("hIncrByFloatHash func: cmdName != hincrbyfloat")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'hincrbyfloat' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	increment, err := strconv.ParseFloat(string(cmd[3]), 64)
	if err != nil {
		return resp.MakeErrorData("value is not a valid float")
	}
	mem.CheckTTL(key)
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		hash := NewHash()
		mem.db.Set(key, hash)
	}
	temp, _ := mem.db.Get(key)
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res, err := hash.IncrByFloat(field, increment)
	if err != nil {
		return resp.MakeErrorData("hash value is not a float")
	}
	return resp.MakeBulkData([]byte(strconv.FormatFloat(res, 'f', -1, 64)))
}
func hKeysHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hkeys" {
		dblog.Logger.Error("hkeysHash func: cmdName != hkeys")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hkeys' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.KeysAndVals()
	if len(res) == 0 {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	result := make([]resp.RedisData, 0, len(res))
	for i := 0; i < len(res); i += 2 {
		result = append(result, resp.MakeBulkData(res[i]))
	}
	return resp.MakeArrayData(result)
}

func hValsHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hvals" {
		dblog.Logger.Error("hValsHash func: cmdName != hvals")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hvals' command")
	}

	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.KeysAndVals()
	if len(res) == 0 {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	result := make([]resp.RedisData, 0, len(res))
	for i := 1; i < len(res); i += 2 {
		result = append(result, resp.MakeBulkData(res[i]))
	}
	return resp.MakeArrayData(result)
}

func hLenHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hlen" {
		dblog.Logger.Error("hLenHash func: cmdName != hlen")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'hlen' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.MakeIntData(int64(hash.Len()))
}
func hStrLenHash(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "hstrlen" {
		dblog.Logger.Error("hStrLen func: cmdName != hstrlen")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'hstrlen' command")
	}
	key := string(cmd[1])
	field := string(cmd[2])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	hash, typeOk := temp.(Hash)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := hash.Strlen(field)
	return resp.MakeIntData(int64(res))
}

func RegisterHashCommand() {
	RegisterCommand("hdel", hDelHash)
	RegisterCommand("hexists", hExistsHash)
	RegisterCommand("hgetall", hGetAllHash)
	RegisterCommand("hincrby", hIncrByHash)
	RegisterCommand("hincrbyfloat", hIncrByFloatHash)
	RegisterCommand("hkeys", hKeysHash)
	RegisterCommand("hlen", hLenHash)
	RegisterCommand("hmget", hMgetHash)
	RegisterCommand("hset", hSetHash)
	RegisterCommand("hget", hGetHash)
	RegisterCommand("hsetnx", hSetnxHash)
	RegisterCommand("hstrlen", hStrLenHash)
	RegisterCommand("hvals", hValsHash)
}
