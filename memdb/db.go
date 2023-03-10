package memdb

import (
	"fishRedis/dblog"
	"time"
)

type MemDb struct {
	db        *ConcurrentMap
	ttlKeys   *ConcurrentMap
	watchKeys *ConcurrentMap
	locks     *Locks
}

func NewMemdb() *MemDb {
	return &MemDb{
		db:        NewConcurrentMap(DEFAULT_SIZE),
		ttlKeys:   NewConcurrentMap(DEFAULT_SIZE),
		watchKeys: NewConcurrentMap(DEFAULT_SIZE),
		locks:     NewLocks(DEFAULT_SIZE * 2),
	}
}

// CheckTTL check ttlkeys and delete expired keys
// if the key doesn't exist or is not expired return true
// if the key is expired,return false
func (m *MemDb) CheckTTL(key string) bool {
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
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

	m.ttlKeys.Delete(key)
	m.db.Delete(key)
	return false

}

// SetTTL shouldn't acquire the lock
// the SETEX is an atomic command,so we acquire lock in setExString
func (m *MemDb) SetTTL(key string, value int64) int {
	_, ok := m.db.Get(key)
	if !ok {
		dblog.Logger.Debugf("SetTTL key not exist, key = %s", key)
		return 0
	}
	// the result should be 1
	return m.ttlKeys.Set(key, value)

}
func (m *MemDb) DeleteTTL(key string) int {
	_, ok := m.db.Get(key)
	if !ok {
		dblog.Logger.Debugf("DeleteTTL key not exist key = %s,maybe is expired", key)
	}
	return m.ttlKeys.Delete(key)

}

func (m *MemDb) TouchWatchKey(key string) {
	temp, ok := m.watchKeys.Get(key)
	if ok {
		// if the key is being watched
		set := temp.(map[*RedisClient]null)
		// mark all clients that watch the key dirty
		for client := range set {
			client.Flags |= REDIS_DIRTY_CAS
		}

	}

}
