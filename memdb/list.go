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
	listVal, typeOk := val.(*List)
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
	listVal, typeOk := val.(*List)
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
	listVal, typeOk := val.(*List)
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
	listVal, typeOk := val.(*List)
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
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")
	}
	res := listVal.lPop()
	return resp.MakeBulkData(res)
}

func rPushList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "rpush" {
		dblog.Logger.Error("rPushList func:cmdName != lpush")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'rpush' command")
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
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")
	}
	for i := 2; i < length; i++ {
		listVal.rPush(cmd[i])
	}
	return resp.MakeIntData(int64(listVal.Length))
}
func rPopList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "rpop" {
		dblog.Logger.Error("rPopList func: cmdName != rpop")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'rpop'")
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
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding thr wrong kind of value")
	}
	res := listVal.rPop()
	return resp.MakeBulkData(res)
}

func rPushxList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "rpushx" {
		dblog.Logger.Error("rPushxList func : cmdName != rpushx")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'rpushx' command")
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
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for i := 2; i < length; i++ {
		listVal.rPush(cmd[i])
	}
	return resp.MakeIntData(int64(listVal.Length))
}
func lPosList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lpos" {
		dblog.Logger.Error("lPosList func:cmdName != lpos")
		return resp.MakeErrorData("server error")
	}
	length := len(cmd)
	if length < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'lpos' command")
	}
	key := string(cmd[1])
	element := cmd[2]
	if !m.CheckTTL(key) {
		return resp.MakeBulkData(nil)
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData(nil)
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wtong kind of value")
	}
	// default values
	rank := 1
	var err error
	count := 1
	maxLen := listVal.Length

	// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
	for i := 3; i < length; i++ {
		param := strings.ToLower(string(cmd[i]))
		switch param {
		case "rank":
			i++
			rank, err = strconv.Atoi(string(cmd[i]))
			if err != nil {
				return resp.MakeErrorData("value is not an integer or out of range")
			}
			if rank == 0 {
				return resp.MakeErrorData("ERR RANK can't be zero :use 1 to start from the first match,2 from the second and so on")
			}
		case "count":
			i++
			count, err = strconv.Atoi(string(cmd[i]))
			if err != nil {
				return resp.MakeErrorData("value is not an integer or out of range")
			}
			if count == 0 {
				count = listVal.Length
			}
		case "maxlen":
			i++
			maxLen, err = strconv.Atoi(string(cmd[i]))
			if err != nil {
				return resp.MakeErrorData("value is not an integer or out of range")
			}
		}

	}
	resInt := listVal.lPos(element, rank, count, maxLen)
	if len(resInt) == 0 {
		return resp.MakeBulkData(nil)
	}
	res := make([]resp.RedisData, 0)
	for _, v := range resInt {
		res = append(res, resp.MakeIntData(int64(v)))
	}
	return resp.MakeArrayData(res)

}
func lInsertList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "linsert" {
		dblog.Logger.Error("lInsertList func:cmdName != linsert")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 5 {
		return resp.MakeErrorData("wrong number of arguments for 'linsert' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	var isBefore bool
	if strings.ToLower(string(cmd[2])) == "before" {
		isBefore = true
	} else if strings.ToLower(string(cmd[2])) == "after" {
		isBefore = false
	} else {
		return resp.MakeErrorData("ERR syntax error")
	}
	pivot := cmd[3]
	element := cmd[4]
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := listVal.lInsert(isBefore, pivot, element)
	return resp.MakeIntData(int64(res))
}
func lRangeList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lrange" {
		dblog.Logger.Error("lRangeList func:cmdName != lrange")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'lrange' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeBulkData([]byte("empty list or set"))
	}
	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("empty list or set"))
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := listVal.lRange(start, end)
	if len(res) == 0 {
		return resp.MakeBulkData([]byte("empty list or set"))
	}
	resData := make([]resp.RedisData, 0)
	for _, v := range res {
		resData = append(resData, resp.MakeBulkData(v))
	}
	return resp.MakeArrayData(resData)
}
func lRemList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lrem" {
		dblog.Logger.Error("lRemList func:cmdName != lrem")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'lrem' command")
	}
	key := string(cmd[1])
	count, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	element := cmd[3]
	if !m.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := listVal.deleteByVal(element, count)
	return resp.MakeIntData(int64(res))
}
func lSetList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "lset" {
		dblog.Logger.Error("lSetList func:cmdName != lset")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'lset' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeErrorData("no such key")
	}
	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	element := cmd[3]
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeErrorData("no such key")
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := listVal.set(element, index)
	if res == false {
		return resp.MakeErrorData("index out if range")
	} else {
		return resp.MakeStringData("OK")
	}

}

func lTrimList(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "ltrim" {
		dblog.Logger.Error("lTrimList func:cmdName != ltrim")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'ltrim' command")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.MakeBulkData([]byte("empty list or set"))
	}
	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("value is not an integer or out of range")
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("empty list or set"))
	}
	listVal, typeOk := val.(*List)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	listVal.trim(start, end)
	return resp.MakeStringData("OK")
}

func RegisterListCommands() {
	RegisterCommand("llen", lLenList)
	RegisterCommand("lindex", lIndexList)
	RegisterCommand("lpos", lPosList)
	RegisterCommand("lpop", lPopList)
	RegisterCommand("rpop", rPopList)
	RegisterCommand("lpush", lPushList)
	RegisterCommand("lpushx", lPushxList)
	RegisterCommand("rpush", rPushList)
	RegisterCommand("rpushx", rPushxList)
	RegisterCommand("linsert", lInsertList)
	RegisterCommand("lset", lSetList)
	RegisterCommand("lrem", lRemList)
	RegisterCommand("ltrim", lTrimList)
	RegisterCommand("lrange", lRangeList)
}
