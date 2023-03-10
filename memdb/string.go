package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func setString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "set" {
		dblog.Logger.Error("setString func: cmdName != set")

		return resp.MakeErrorData("Server error")

	}
	if len(cmd) < 3 {
		dblog.Logger.Error("invalid length for set command")
		client.OutputBuf = resp.MakeErrorData("error: commands is invalid")
	}
	m.CheckTTL(string(cmd[1]))
	var err error
	var exval int64
	var nx, xx, get, ex, keepttl bool
	key := string(cmd[1])
	val := cmd[2]
	for i := 3; i < len(cmd); i++ {
		parm := strings.ToLower(string(cmd[i]))
		switch parm {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "get":
			get = true
		case "ex":
			ex = true
			i++
			if i >= len(cmd) {
				return resp.MakeErrorData("error:commands is invalid")

			}
			exTime := cmd[i]
			exval, err = strconv.ParseInt(string(cmd[i]), 10, 64)
			if err != nil {
				return resp.MakeErrorData(fmt.Sprintf("error: commands is invalid %s is not an integer", exTime))

			}
		case "keepttl":
			keepttl = true
		default:
			return resp.MakeErrorData("error unsupported option:" + string(cmd[i]))

		}
	}
	if (nx && xx) || (ex && keepttl) {
		return resp.MakeErrorData("error:command is invalid")

	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	oldVal, oldOk := m.db.Get(key)
	var oldvalWithType []byte
	var typeOK bool
	var res resp.RedisData
	if oldOk {
		oldvalWithType, typeOK = oldVal.([]byte)
		if !typeOK {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")

		}
	}
	if nx || xx {
		if nx {
			if !oldOk {
				m.db.Set(key, val)
				res = resp.MakeStringData("OK")
			} else {
				res = resp.MakeBulkData(nil)
			}

		} else if xx {
			if oldOk {
				m.db.Set(key, val)
				res = resp.MakeStringData("OK")
			} else {
				res = resp.MakeBulkData(nil)
			}
		}
	} else {
		m.db.Set(key, val)
		res = resp.MakeStringData("OK")
	}
	if get {
		if !oldOk {
			return resp.MakeBulkData(nil)

		} else {
			return resp.MakeBulkData(oldvalWithType)

		}
	}
	if !keepttl {
		m.DeleteTTL(key)
	}
	if ex {
		ttlTime := time.Now().Unix() + exval
		m.SetTTL(key, ttlTime)
	}
	m.TouchWatchKey(key)
	return res

}

func getString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "get" {
		dblog.Logger.Error("GetString func:cmdName != get")
		return resp.MakeErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("error:commands is invalid")

	}
	key := string(cmd[1])
	// checkTTL first,delete expired key
	if !m.CheckTTL(key) {
		client.OutputBuf = resp.MakeBulkData(nil)
		return resp.MakeBulkData(nil)
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	valWithType, typeOK := val.([]byte)
	if !typeOK {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.MakeBulkData(valWithType)
}

func getRangeString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "getrange" {
		dblog.Logger.Error("getRangeString func:cmdName != getrange")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("error:commands is invalid")
	}
	key := string(cmd[1])
	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("error: command is invalid")
	}
	end, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("error: command is invalid")
	}
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	valWithType, typeOk := val.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if start < 0 {
		start = len(valWithType) + start
	}
	if end < 0 {
		end = len(valWithType) + end
	}
	end = end + 1
	if start > end || start >= len(valWithType) || end < 0 {
		return resp.MakeBulkData(nil)
	}
	if start < 0 {
		start = 0
	}
	if end > len(valWithType) {
		end = len(valWithType)
	}
	return resp.MakeBulkData(valWithType[start:end])

}
func setRangeString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "setrange" {
		dblog.Logger.Error("setRangeString func: cmdName != setrange")
		return resp.MakeErrorData("Server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("error:commands is invalid")
	}
	key := string(cmd[1])
	offset, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("error: command is invalid")
	}
	substr := cmd[3]
	// check ttl first
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	var oldValWithType []byte
	oldVal, ok := m.db.Get(key)
	var newVal []byte
	if !ok {
		oldValWithType = make([]byte, 0)
	} else {
		oldValWithType, ok = oldVal.([]byte)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong value")
		}
	}
	if offset > len(oldValWithType) {
		newVal = oldValWithType
		for i := 0; i < offset-len(oldValWithType); i++ {
			newVal = append(newVal, byte(0))
		}
		newVal = append(newVal, substr...)
	} else {
		newVal = oldValWithType[:offset]
		tailIndex := offset + len(substr)
		newVal = append(newVal, substr...)
		if tailIndex < len(oldValWithType) {
			tail := oldValWithType[offset+len(substr):]
			newVal = append(newVal, tail...)
		}

	}
	m.db.Set(key, newVal)
	m.TouchWatchKey(key)
	return resp.MakeIntData(int64(len(newVal)))
}
func mGetString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "mget" {
		dblog.Logger.Error("mGetString func:cmdName != mget")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("error: commands is invalid")
	}
	length := len(cmd)
	res := make([]resp.RedisData, 0)
	for i := 1; i < length; i++ {
		key := string(cmd[i])
		if !m.CheckTTL(key) {
			res = append(res, resp.MakeBulkData(nil))
			continue
		}
		m.locks.RLock(key)
		val, ok := m.db.Get(key)
		m.locks.RUnlock(key)
		if !ok {
			res = append(res, resp.MakeBulkData(nil))
			continue
		}
		valWithType, typeOk := val.([]byte)
		if !typeOk {
			res = append(res, resp.MakeBulkData(nil))
		} else {
			res = append(res, resp.MakeBulkData(valWithType))
		}

	}
	return resp.MakeArrayData(res)
}

func mSetString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "mset" {
		dblog.Logger.Error("mSetString func:cmdName != mset")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 || length%2 == 0 {
		return resp.MakeErrorData("error: command is invalid")
	}
	keys := make([]string, 0)
	vals := make([][]byte, 0)
	for i := 1; i < length; i += 2 {
		key := string(cmd[i])
		val := cmd[i+1]
		keys = append(keys, key)
		vals = append(vals, val)
	}
	m.locks.LockMulti(keys)
	defer m.locks.UnlockMulti(keys)
	for i := 0; i < len(keys); i++ {
		m.DeleteTTL(keys[i])
		m.db.Set(keys[i], vals[i])
		m.TouchWatchKey(keys[i])
	}
	return resp.MakeStringData("OK")
}

func setExString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "setex" {
		dblog.Logger.Error("setExString func:cmdName != setex")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("error: command is invalid")
	}
	// no need to checkTTL
	key := string(cmd[1])
	val := cmd[3]
	ttlTime, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.MakeErrorData("error: command is invalid")
	}
	newTTL := time.Now().Unix() + ttlTime

	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	m.db.Set(key, val)
	m.ttlKeys.Set(key, newTTL)
	m.TouchWatchKey(key)
	return resp.MakeStringData("OK")
}
func setNxString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "setnx" {
		dblog.Logger.Error("setNxString func:cmdName != setnx")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	val := cmd[2]
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	res := m.db.SetIfNotExist(key, val)
	m.TouchWatchKey(key)
	return resp.MakeIntData(int64(res))

}

func strLenString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "strlen" {
		dblog.Logger.Error("strLenString func :cmdName !=setlen")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	m.locks.RLock(key)
	val, ok := m.db.Get(key)
	m.locks.RUnlock(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	valWithType, typeOk := val.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.MakeIntData(int64(len(valWithType)))

}

func incrString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "incr" {
		dblog.Logger.Error("incrString func :cmdName != incr")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte("1"))
		return resp.MakeIntData(1)
	}
	valWithType, typeOk := val.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err := strconv.ParseInt(string(valWithType), 10, 64)
	if err != nil {
		return resp.MakeErrorData("value is not an integer")
	}
	intVal++
	// no need to deleteTTL
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	m.TouchWatchKey(key)
	return resp.MakeIntData(intVal)

}
func incrByString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "incrby" {
		dblog.Logger.Error("incrByString func: cmdName != incrby")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	increment, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.MakeErrorData("commands invalid: increment value is not an integer")
	}
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)

	var intVal int64
	if !ok {
		intVal = 0
		intVal += increment
		m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
		return resp.MakeIntData(intVal)
	}
	valWithType, typeOK := val.([]byte)
	if !typeOK {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err = strconv.ParseInt(string(valWithType), 10, 64)
	if err != nil {
		return resp.MakeErrorData("value is not an integer")
	}
	intVal += increment
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	m.TouchWatchKey(key)
	return resp.MakeIntData(intVal)
}

func decrString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "decr" {
		dblog.Logger.Error("decrString func:cmdName != decr")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	var intVal int64
	if !ok {
		intVal = 0
		intVal--
		m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
		return resp.MakeIntData(intVal)
	}
	valWithType, typeOk := val.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err := strconv.ParseInt(string(valWithType), 10, 64)
	if err != nil {
		return resp.MakeErrorData("value is not an integer")
	}
	intVal--
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	m.TouchWatchKey(key)
	return resp.MakeIntData(intVal)
}

func decrByString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "decrby" {
		dblog.Logger.Error("decrByString func: cmdName!= decrby")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	decrement, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.MakeErrorData("commands invalid: decrement value is not an integer")
	}
	m.CheckTTL(key)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	var intVal int64
	if !ok {
		intVal = 0
		intVal -= decrement
		m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
		return resp.MakeIntData(intVal)
	}
	valWithType, typeOk := val.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err = strconv.ParseInt(string(valWithType), 10, 64)
	if err != nil {
		return resp.MakeErrorData("value is not an integer")
	}
	intVal -= decrement
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	m.TouchWatchKey(key)
	return resp.MakeIntData(intVal)

}

func incrByFloatString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "incrbyfloat" {
		dblog.Logger.Error("incrByFloatString func: cmdName != incrbyfloat")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("commands is invalid")
	}
	key := string(cmd[1])
	inc, err := strconv.ParseFloat(string(cmd[2]), 64)

	if err != nil {
		return resp.MakeErrorData("commands invalid increment value is not a float")
	}
	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	var floatVal float64
	val, ok := m.db.Get(key)
	if !ok {
		floatVal += inc
		m.db.Set(key, []byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))
		return resp.MakeBulkData([]byte(strconv.FormatFloat(inc, 'f', -1, 64)))
	}
	valWithType, ok := val.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	floatVal, err = strconv.ParseFloat(string(valWithType), 64)
	if err != nil {
		return resp.MakeErrorData("value is not a float")
	}
	floatVal += inc
	m.db.Set(key, []byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))
	m.TouchWatchKey(key)
	return resp.MakeBulkData([]byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))

}
func appendString(client *RedisClient) resp.RedisData {
	cmd := client.Args
	m := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "append" {
		dblog.Logger.Error("appendString func: cmdName != append")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	val := cmd[2]
	m.CheckTTL(key)

	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	oldVal, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, val)
		return resp.MakeIntData(int64(len(val)))
	}
	oldvalWithType, typeOk := oldVal.([]byte)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	newVal := append(oldvalWithType, val...)
	m.db.Set(key, newVal)
	m.TouchWatchKey(key)
	return resp.MakeIntData(int64(len(newVal)))
}

func RegisterStringCommand() {
	RegisterCommand("set", setString)
	RegisterCommand("get", getString)
	RegisterCommand("getrange", getRangeString)
	RegisterCommand("setrange", setRangeString)
	RegisterCommand("mget", mGetString)
	RegisterCommand("mset", mSetString)
	RegisterCommand("setex", setExString)
	RegisterCommand("setnx", setNxString)
	RegisterCommand("strlen", strLenString)
	RegisterCommand("incr", incrString)
	RegisterCommand("incrby", incrByString)
	RegisterCommand("decr", decrString)
	RegisterCommand("decrby", decrByString)
	RegisterCommand("incrbyfloat", incrByFloatString)
	RegisterCommand("append", appendString)
}
