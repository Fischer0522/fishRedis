package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strconv"
	"strings"
)

func sAddSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sadd" {
		dblog.Logger.Error("sAddSet func: cmdName!= sadd")
		return resp.MakeErrorData("server error")
	}
	if len(cmdName) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'sadd' command")
	}
	key := string(cmd[1])
	mem.CheckTTL(key)
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	_, ok := mem.db.Get(key)
	if !ok {
		set := NewSet()
		mem.db.Set(key, set)
	}
	temp, _ := mem.db.Get(key)
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}

	count := 0
	for i := 2; i < len(cmd); i++ {
		res := set.sAdd(string(cmd[i]))
		count += res
	}
	mem.TouchWatchKey(key)
	return resp.MakeIntData(int64(count))
}
func sCardSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "scard" {
		dblog.Logger.Error("aCardSet func: cmdName != scard")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeErrorData("wrong number of arguments for 'scard' command")
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
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	return resp.MakeIntData(int64(set.sLen()))
}
func sPopSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "spop" {
		dblog.Logger.Error("sPopSet func: cmdName != spop")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'spop' command")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	count := 1
	var err error
	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("value is not an integer or out of range")
		}
	}
	mem.locks.Lock(key)
	defer mem.locks.Unlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	randRes := set.sRandom(count)
	result := make([]resp.RedisData, 0, len(randRes))
	for _, setKey := range randRes {
		set.sDelete(setKey)
		result = append(result, resp.MakeBulkData([]byte(setKey)))
	}
	defer func() {
		if set.sLen() == 0 {
			mem.DeleteTTL(key)
			mem.db.Delete(key)
		}
	}()
	mem.TouchWatchKey(key)
	return resp.MakeArrayData(result)
}

// TODO support count < 0
func sRandMemberSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "srandmember" {
		dblog.Logger.Error("sRandMemberSet func: cmdName != srandmember")
	}
	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'srandmember' command")
	}
	key := string(cmd[1])
	count := 1
	var err error
	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("value is not an integer or out of range")
		}
	}

	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	resStr := set.sRandom(count)
	result := make([]resp.RedisData, 0, len(resStr))
	for _, setKey := range resStr {
		result = append(result, resp.MakeBulkData([]byte(setKey)))
	}
	return resp.MakeArrayData(result)
}

func sRemSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "srem" {
		dblog.Logger.Error("sRemset func: cmdName != srem")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'srem' command")
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
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeIntData(0)
	}
	count := 0
	for i := 2; i < len(cmd); i++ {
		res := set.sDelete(string(cmd[i]))
		count += res
	}
	defer func() {
		if set.sLen() == 0 {
			mem.DeleteTTL(key)
			mem.db.Delete(key)
		}
	}()
	mem.TouchWatchKey(key)
	return resp.MakeIntData(int64(count))
}

// if set destination doesn't exist create an empty set first
func sMoveSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "smove" {
		dblog.Logger.Error("sMoveSet func: cmdName != smove")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.MakeErrorData("wrong number of arguments for 'smove' command")
	}
	source := string(cmd[1])
	des := string(cmd[2])
	member := string(cmd[3])
	if !mem.CheckTTL(source) {
		return resp.MakeIntData(0)
	}
	mem.CheckTTL(des)
	mem.locks.Lock(source)
	defer mem.locks.Unlock(source)
	mem.locks.Lock(des)
	defer mem.locks.Unlock(des)

	tempSource, ok := mem.db.Get(source)
	if !ok {
		return resp.MakeIntData(0)
	}
	sourceSet, typeOk := tempSource.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	_, ok = mem.db.Get(des)
	if !ok {
		emptySet := NewSet()
		mem.db.Set(des, emptySet)
	}
	tempDes, _ := mem.db.Get(des)
	desSet, typeOk := tempDes.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	if !sourceSet.sIsContains(member) {
		return resp.MakeIntData(0)
	}
	sourceSet.sDelete(member)
	desSet.sAdd(member)
	defer func() {
		if sourceSet.sLen() == 0 {
			mem.DeleteTTL(source)
			mem.db.Delete(source)
		}
	}()
	mem.TouchWatchKey(source)
	mem.TouchWatchKey(des)
	return resp.MakeIntData(1)
}

func sMembersSet(client *RedisClient) resp.RedisData {

	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "smembers" {
		dblog.Logger.Error("smembersSet func: cmdName != smembers")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.MakeBulkData([]byte("(empty list or set)"))
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
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	resStr := set.sGetAll()
	result := make([]resp.RedisData, 0)
	for _, setKey := range resStr {
		result = append(result, resp.MakeBulkData([]byte(setKey)))
	}
	return resp.MakeArrayData(result)
}

func sGenericIsMember(mem *MemDb, key string, member string) resp.RedisData {

	temp, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeIntData(0)
	}
	set, typeOk := temp.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding a wrong kind of value")
	}
	if set.sIsContains(member) {
		return resp.MakeIntData(1)
	} else {
		return resp.MakeIntData(0)
	}

}
func sIsMemberSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sismember" {
		dblog.Logger.Error("sIsMember func: cmdName != sismember")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.MakeErrorData("wrong number of arguments for 'sismember' command")
	}

	key := string(cmd[1])
	member := string(cmd[2])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	return sGenericIsMember(mem, key, member)
}

func sMIsMemberSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "smismember" {
		dblog.Logger.Error("sMIsMember func: cmdName != smismember")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'smismember'")
	}
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeIntData(0)
	}
	mem.locks.RLock(key)
	defer mem.locks.RUnlock(key)
	resultArr := make([]resp.RedisData, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		member := string(cmd[i])
		res := sGenericIsMember(mem, key, member)
		resultArr = append(resultArr, res)
	}
	return resp.MakeArrayData(resultArr)
}

//func genericDiff(mem *MemDb, targetSet Set, sets []Set, isStore bool, destination string) resp.RedisData {
//
//}
func sDiffSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sdiff" {
		dblog.Logger.Error("sDiffSet func: cmdName != sdiff")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("wrong number of arguments for 'sDiff' command")
	}
	sets := make([]Set, 0, len(cmd)-2)
	keys := make([]string, 0, len(cmd)-1)
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}

	keys = append(keys, key)
	for i := 2; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		mem.CheckTTL(key)
	}
	mem.locks.RLockMulti(keys)
	defer mem.locks.RUnlockMulti(keys)
	tempTarget, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	targetSet, typeOk := tempTarget.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	// ignore the target key
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		// if the key is not exist, just regard it as empty set and ignore it
		if !ok {
			continue
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	diffSet := targetSet.sDiff(sets...).sGetAll()
	result := make([]resp.RedisData, 0, len(diffSet))
	for _, ele := range diffSet {
		result = append(result, resp.MakeBulkData([]byte(ele)))
	}
	return resp.MakeArrayData(result)

}

// the destination mayby a member of sets
// use the LockMulti and lock the destination may lead to a deadlock
// so when writing the destination UnLockMulti first
// it means we can't use defer to unlock
func sDiffStoreSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sdiffstore" {
		dblog.Logger.Error("sDiffSet func: cmdName != sdiffstore")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'sdiffstore' command")
	}
	sets := make([]Set, 0, len(cmd)-3)
	keys := make([]string, 0, len(cmd)-2)
	key := string(cmd[2])
	destination := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	mem.CheckTTL(destination)
	// rewrite the destination regardless the type

	keys = append(keys, key)

	for i := 3; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		mem.CheckTTL(key)
	}

	// call RUnLockMulti before return
	mem.locks.RLockMulti(keys)

	tempTarget, ok := mem.db.Get(key)
	if !ok {
		mem.locks.RUnlockMulti(keys)
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	targetSet, typeOk := tempTarget.(Set)
	if !typeOk {
		mem.locks.RUnlockMulti(keys)
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	// ignore the targetKey
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		// if key is not exist just regard it as empty set and ignore it
		if !ok {
			continue
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			mem.locks.RUnlockMulti(keys)
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	diffSet := targetSet.sDiff(sets...)
	mem.locks.RUnlockMulti(keys)
	mem.locks.Lock(destination)
	defer mem.locks.Unlock(destination)

	mem.db.Set(destination, diffSet)
	mem.TouchWatchKey(destination)

	return resp.MakeIntData(int64(diffSet.sLen()))

}

func sInterSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sinter" {
		dblog.Logger.Error("sDiffSet func: cmdName != sinter")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("wrong number of arguments for 'sinter' command")
	}

	sets := make([]Set, 0, len(cmd)-2)
	keys := make([]string, 0, len(cmd)-1)
	key := string(cmd[1])
	if !mem.CheckTTL(key) {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}

	keys = append(keys, key)
	for i := 2; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		if !mem.CheckTTL(key) {
			return resp.MakeBulkData([]byte("(empty list or set)"))
		}
	}
	mem.locks.RLockMulti(keys)
	defer mem.locks.RUnlockMulti(keys)
	tempTarget, ok := mem.db.Get(key)
	if !ok {
		return resp.MakeBulkData([]byte("(empty list or set)"))
	}
	targetSet, typeOk := tempTarget.(Set)
	if !typeOk {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	// ignore the target key
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		if !ok {
			return resp.MakeBulkData([]byte("(empty list or set)"))
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	diffSet := targetSet.sInter(sets...).sGetAll()
	result := make([]resp.RedisData, 0, len(diffSet))
	for _, ele := range diffSet {
		result = append(result, resp.MakeBulkData([]byte(ele)))
	}
	return resp.MakeArrayData(result)

}
func sInterStoreSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sinterstore" {
		dblog.Logger.Error("sDiffSet func: cmdName != sinterstore")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'sinterstore' command")
	}
	sets := make([]Set, 0, len(cmd)-3)
	keys := make([]string, 0, len(cmd)-2)
	key := string(cmd[2])
	destination := string(cmd[1])
	if !mem.CheckTTL(key) {
		mem.locks.Lock(destination)
		mem.db.Delete(destination)
		mem.locks.Unlock(destination)
		return resp.MakeIntData(0)
	}
	mem.CheckTTL(destination)
	// rewrite the destination regardless the type

	keys = append(keys, key)

	for i := 3; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		mem.CheckTTL(key)
	}

	// call RUnLockMulti before return
	mem.locks.RLockMulti(keys)

	tempTarget, ok := mem.db.Get(key)
	if !ok {
		mem.locks.RUnlockMulti(keys)
		mem.locks.Lock(destination)
		mem.db.Delete(destination)
		mem.locks.Unlock(destination)
		return resp.MakeIntData(0)
	}
	targetSet, typeOk := tempTarget.(Set)
	if !typeOk {
		mem.locks.RUnlockMulti(keys)
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	// ignore the targetKey
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		// if not exists generate an empty list
		if !ok {
			mem.locks.RUnlockMulti(keys)
			mem.locks.Lock(destination)
			mem.db.Delete(destination)
			mem.locks.Unlock(destination)
			return resp.MakeIntData(0)
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			mem.locks.RUnlockMulti(keys)
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	interSet := targetSet.sInter(sets...)
	mem.locks.RUnlockMulti(keys)
	mem.locks.Lock(destination)
	defer mem.locks.Unlock(destination)

	mem.db.Set(destination, interSet)
	mem.TouchWatchKey(destination)
	return resp.MakeIntData(int64(interSet.sLen()))

}
func sUnionSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sunion" {
		dblog.Logger.Error("sDiffSet func: cmdName != sunion")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.MakeErrorData("wrong number of arguments for 'sunion' command")
	}
	sets := make([]Set, 0, len(cmd)-2)
	keys := make([]string, 0, len(cmd)-1)
	key := string(cmd[1])

	mem.CheckTTL(key)

	keys = append(keys, key)
	for i := 2; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		mem.CheckTTL(key)
	}
	mem.locks.RLockMulti(keys)
	defer mem.locks.RUnlockMulti(keys)
	tempTarget, ok := mem.db.Get(key)
	var targetSet Set
	if !ok {
		targetSet = NewSet()
	} else {
		targetSet, ok = tempTarget.(Set)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	// ignore the target key
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		// if the key is not exist, just regard it as empty set and ignore it
		if !ok {
			continue
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	unionSet := targetSet.sUnion(sets...).sGetAll()
	result := make([]resp.RedisData, 0, len(unionSet))
	for _, ele := range unionSet {
		result = append(result, resp.MakeBulkData([]byte(ele)))
	}
	return resp.MakeArrayData(result)

}
func sUnionStoreSet(client *RedisClient) resp.RedisData {
	cmd := client.Args
	mem := client.RedisDb
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sunionstore" {
		dblog.Logger.Error("sDiffSet func: cmdName != sunionstore")
		return resp.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.MakeErrorData("wrong number of arguments for 'unionstore' command")
	}
	sets := make([]Set, 0, len(cmd)-3)
	keys := make([]string, 0, len(cmd)-2)
	key := string(cmd[2])
	destination := string(cmd[1])
	if !mem.CheckTTL(key) {
		mem.locks.Lock(destination)
		mem.db.Delete(destination)
		mem.locks.Unlock(destination)
		return resp.MakeIntData(0)
	}
	mem.CheckTTL(destination)
	// rewrite the destination regardless the type

	keys = append(keys, key)

	for i := 3; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}
	for _, key := range keys {
		mem.CheckTTL(key)
	}

	// call RUnLockMulti before return
	mem.locks.RLockMulti(keys)

	tempTarget, ok := mem.db.Get(key)
	var targetSet Set
	if !ok {
		targetSet = NewSet()
	} else {
		targetSet, ok = tempTarget.(Set)
		if !ok {
			mem.locks.RUnlockMulti(keys)
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	// ignore the targetKey
	for _, key := range keys[1:] {
		temp, ok := mem.db.Get(key)
		// if not exists ,regard it as empty set ignore it
		if !ok {
			continue
		}
		set, typeOk := temp.(Set)
		if !typeOk {
			mem.locks.RUnlockMulti(keys)
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)

	}

	unionSet := targetSet.sUnion(sets...)
	mem.locks.RUnlockMulti(keys)
	mem.locks.Lock(destination)
	defer mem.locks.Unlock(destination)

	mem.db.Set(destination, unionSet)
	mem.TouchWatchKey(destination)

	return resp.MakeIntData(int64(unionSet.sLen()))
}

func RegisterSetCommands() {
	RegisterCommand("sadd", sAddSet)
	RegisterCommand("scard", sCardSet)
	RegisterCommand("sdiff", sDiffSet)
	RegisterCommand("sdiffstore", sDiffStoreSet)
	RegisterCommand("sinter", sInterSet)
	RegisterCommand("sinterstore", sInterStoreSet)
	RegisterCommand("sismember", sIsMemberSet)
	RegisterCommand("smismember", sMIsMemberSet)
	RegisterCommand("smembers", sMembersSet)
	RegisterCommand("smove", sMoveSet)
	RegisterCommand("spop", sPopSet)
	RegisterCommand("srandmember", sRandMemberSet)
	RegisterCommand("srem", sRemSet)
	RegisterCommand("sunion", sUnionSet)
	RegisterCommand("sunionstore", sUnionStoreSet)

}
