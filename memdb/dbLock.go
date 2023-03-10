package memdb

import (
	"fishRedis/dblog"
	"fishRedis/util"
	"sort"
	"sync"
)

// lock keys logically

type Locks struct {
	locks []*sync.RWMutex
}

func NewLocks(size int) *Locks {
	locks := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		locks[i] = &sync.RWMutex{}
	}
	return &Locks{
		locks: locks,
	}
}

func (lock *Locks) GetKeyPos(key string) int {
	pos := util.HashKey(key)
	return pos % len(lock.locks)
}

func (lock *Locks) Lock(key string) {
	pos := lock.GetKeyPos(key)

	if pos == -1 {
		dblog.Logger.Errorf("Locks Lock key %s error: pos == -1", key)
	}
	lock.locks[pos].Lock()
}

func (lock *Locks) Unlock(key string) {
	pos := lock.GetKeyPos(key)
	if pos == -1 {
		dblog.Logger.Errorf("Locks unlock key %s error : pos == -1", key)
	}
	lock.locks[pos].Unlock()

}

func (lock *Locks) RLock(key string) {
	pos := lock.GetKeyPos(key)
	if pos == -1 {
		dblog.Logger.Errorf("Locks RLock key %s error : pos == -1", key)
	}
	lock.locks[pos].RLock()
}

func (lock *Locks) RUnlock(key string) {
	pos := lock.GetKeyPos(key)
	if pos == -1 {
		dblog.Logger.Errorf("Lock RUnlock key %s error : pos == -1", key)
	}
	lock.locks[pos].RUnlock()
}

// when call LockMulti,call sortedLockPoses first sort the keys
// to prevent the deadLock,lock keys from front to the end
// use map to ensure only lock or unlock a slot once
func (lock *Locks) sortedLockPoses(keys []string) []int {
	set := make(map[int]struct{})
	for _, key := range keys {
		pos := lock.GetKeyPos(key)
		if pos == -1 {
			dblog.Logger.Errorf("Locks Lock key %s error: pos == -1", key)
			return nil
		}
		set[pos] = struct{}{}

	}

	poses := make([]int, len(set))
	i := 0
	for index := range set {
		poses[i] = index
		i++
	}
	sort.Ints(poses)

	return poses
}
func (lock *Locks) LockMulti(keys []string) {
	poses := lock.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		lock.locks[pos].Lock()
	}
}
func (lock *Locks) UnlockMulti(keys []string) {
	poses := lock.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		lock.locks[pos].Unlock()
	}
}
func (lock *Locks) RLockMulti(keys []string) {
	poses := lock.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		lock.locks[pos].RLock()
	}
}
func (lock *Locks) RUnlockMulti(keys []string) {
	poses := lock.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		lock.locks[pos].RUnlock()
	}
}

// just use for test
func (lock *Locks) tryToGetLock(key string) bool {
	pos := lock.GetKeyPos(key)
	return lock.locks[pos].TryLock()
}
func (lock *Locks) tryToGetRLock(key string) bool {
	pos := lock.GetKeyPos(key)
	return lock.locks[pos].TryRLock()
}
func (lock *Locks) getLockCount() int {
	result := 0
	for _, l := range lock.locks {
		if !l.TryLock() {
			result++
		}
	}
	return result
}
func (lock *Locks) getRLockCount() int {
	result := 0
	for _, l := range lock.locks {
		if !l.TryRLock() {
			result++
		}
	}
	return result
}
