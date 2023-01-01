package memdb

import (
	"fishRedis/dblog"
	"fishRedis/resp"
	"strings"
	"time"
)

type MemDb struct {
	db      *ConcurrentMap
	ttlKeys *ConcurrentMap
	locks   *Locks
}

func NewMemdb() *MemDb {
	return &MemDb{
		db:      NewConcurrentMap(DEFAULT_SIZE),
		ttlKeys: NewConcurrentMap(DEFAULT_SIZE),
		locks:   NewLocks(DEFAULT_SIZE * 2),
	}
}

func (m *MemDb) ExecCommand(cmd [][]byte) resp.RedisData {
	if len(cmd) == 0 {
		return nil
	}
	cmdName := strings.ToLower(string(cmd[0]))
	command, ok := cmdTable[cmdName]
	if !ok {
		return resp.MakeErrorData("error: unsupported command")
	}
	execFunc := command.executor

	return execFunc(m, cmd)

}

// all lock and unlock action about ttl are here
// when calling function about ttl shouldn't lock or unlock

// CheckTTL check ttlkeys and delete expired keys
// if the key doesn't exist or not is expired return true
// if the key is expired,return false
func (m *MemDb) CheckTTL(key string) bool {
	ttl, ok := m.ttlKeys.Get(key)
	if !ok {
		return true
	}
	ttlTime := ttl.(int64)
	now := time.Now().Unix()
	if ttlTime > now {
		return true
	}
	// expired now delete it
	m.locks.Lock(key)
	defer m.locks.UnLock(key)
	m.ttlKeys.Delete(key)
	m.db.Delete(key)
	return false

}

func (m *MemDb) SetTTL(key string, value int64) int {
	_, ok := m.db.Get(key)
	if !ok {
		dblog.Logger.Debugf("SetTTL key not exist, key = %s", key)
		return 0
	}
	// the result should be 1
	m.locks.Lock(key)
	defer m.locks.UnLock(key)
	return m.ttlKeys.Set(key, value)

}
func (m *MemDb) DeleteTTL(key string) int {
	_, ok := m.db.Get(key)
	if !ok {
		dblog.Logger.Debugf("DeleteTTL key not exist key = %s,maybe is expired", key)
	}
	m.locks.Lock(key)
	defer m.locks.UnLock(key)
	return m.ttlKeys.Delete(key)

}