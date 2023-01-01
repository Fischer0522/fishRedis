package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// when calling function about ttl
// should call func about ttl first and then require lock
// because func about already require lock and release lock

func setString(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "set" {
		dblog.Logger.Error("setString func: cmdName != set")
		return resp.MakeErrorData("Server error")
	}
	if len(cmd) < 3 {
		dblog.Logger.Error("invalid length for set command")
		return resp.MakeErrorData("error: commands is invalid")
	}
	m.CheckTTL(string(cmd[1]))
	var err error
	var exval int64
	var nx, xx, get, ex, keepttl bool
	key := strings.ToLower(string(cmd[1]))
	val := string(cmd[2])
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
	return res
}

func getString(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "get" {
		dblog.Logger.Error("GetString func:cmdName != get")
		return resp.MakeErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("error:commands is invalid")
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	// checkTTL first,delete expired key
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
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

func getRangeString(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "getrange" {
		dblog.Logger.Error("getRangeString func:cmdName != getrange")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("error:commands is invalid")
	}
	key := strings.ToLower(string(cmd[1]))
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
	return resp.MakeBulkData(valWithType[start:end])

}
func setRangeString(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "setrange" {
		dblog.Logger.Error("setRangeString func: cmdName != setrange")
		return resp.MakeErrorData("Server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("error:commands is invalid")
	}
	key := strings.ToLower(string(cmd[1]))
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
		newVal = append(newVal, substr...)
	}
	m.db.Set(key, newVal)
	return resp.MakeIntData(int64(len(newVal)))
}
