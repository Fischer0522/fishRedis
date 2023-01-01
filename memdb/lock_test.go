package memdb

import (
	"fmt"
	"log"
	"strconv"
	"testing"
)

func TestGetLock(t *testing.T) {
	db := NewMemdb()
	length := 10000
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		db.db.Set(key, val)
		db.locks.Lock(key)
		ok := db.locks.tryToGetLock(key)
		if ok {
			t.Errorf("lock key %s failed", key)
		}
		db.locks.Unlock(key)
	}

	for i := 0; i < 10; i++ {
		log.Println("-----------------------------------", i, "---------------------------------------")
		set := make(map[int]string)
		for j := 0; j < 125; j++ {
			key := "key" + strconv.Itoa(i) + strconv.Itoa(j)
			val := "val" + strconv.Itoa(i) + strconv.Itoa(j)
			db.db.Set(key, val)
			pos := db.db.GetKeyPos(key)
			_, ok := set[pos]
			if !ok {
				set[pos] = key
				db.locks.Lock(key)
			} else {
				// already lock the slot,just skip this round
				log.Println(" hash conflict,required failed")
				log.Println("now set", set)
			}
		}
		log.Println(set)
		for _, key := range set {
			ok := db.locks.tryToGetLock(key)
			if ok {
				t.Errorf("try get lock failed key == %s", key)
			} else {
				db.locks.Unlock(key)
			}
		}
	}

}

func TestReadWriteLock(t *testing.T) {
	db := NewMemdb()
	length := 10000
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		db.db.Set(key, val)
		db.locks.Lock(key)
		ok := db.locks.tryToGetRLock(key)
		if ok {
			t.Errorf("lock key %s failed", key)
		}
		db.locks.Unlock(key)
	}

	for i := 0; i < 10; i++ {
		log.Println("-----------------------------------", i, "---------------------------------------")
		set := make(map[int]string)
		for j := 0; j < 125; j++ {
			key := "key" + strconv.Itoa(i) + strconv.Itoa(j)
			val := "val" + strconv.Itoa(i) + strconv.Itoa(j)
			db.db.Set(key, val)
			pos := db.db.GetKeyPos(key)
			_, ok := set[pos]
			if !ok {
				set[pos] = key
				db.locks.Lock(key)
			} else {
				// already lock the slot,just skip this round
				log.Println(" hash conflict,required failed")
				log.Println("now set", set)
			}
		}
		log.Println(set)
		for _, key := range set {
			ok := db.locks.tryToGetRLock(key)
			if ok {
				t.Errorf("try get lock failed key == %s", key)
			} else {
				db.locks.Unlock(key)
			}
		}
	}
}

func TestReadReadLock(t *testing.T) {
	db := NewMemdb()
	length := 10000
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		db.db.Set(key, val)
		db.locks.RLock(key)
		ok := db.locks.tryToGetRLock(key)
		if !ok {
			t.Errorf("lock key %s failed", key)
		}
		db.locks.RUnlock(key)
	}

	for i := 0; i < 10; i++ {
		log.Println("-----------------------------------", i, "---------------------------------------")
		set := make(map[int]string)
		for j := 0; j < 125; j++ {
			key := "key" + strconv.Itoa(i) + strconv.Itoa(j)
			val := "val" + strconv.Itoa(i) + strconv.Itoa(j)
			db.db.Set(key, val)
			pos := db.db.GetKeyPos(key)
			_, ok := set[pos]
			if !ok {
				set[pos] = key
				db.locks.RLock(key)
			} else {
				// hash conflict,but you can rlock it once again
				//db.locks.RLock(key)
				log.Println(" hash conflict,required failed")
				log.Println("now set", set)
			}
		}
		log.Println(set)
		for _, key := range set {
			ok := db.locks.tryToGetRLock(key)
			if !ok {
				t.Errorf("try get lock failed key == %s", key)
			} else {
				db.locks.RUnlock(key)
			}
		}
	}
}

// if some keys are hash into the same slot,
// the lock of this slot will be required more than once ,lead to the deadlock
// should ensure only lock the slot one time
func TestLockMulti(t *testing.T) {
	length := 1000
	db := NewMemdb()
	keys := make([]string, 0, length)
	mp := make(map[int]struct{})
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		pos := db.locks.GetKeyPos(key)
		mp[pos] = struct{}{}
		db.db.Set(key, val)
		keys = append(keys, key)
	}
	db.locks.LockMulti(keys)
	count := db.locks.getLockCount()
	fmt.Println(count)
	if len(mp) != count {
		t.Error("missing some locks", len(mp), " ", count)
	}
	db.locks.UnlockMulti(keys)
	db.locks.LockMulti(keys)
	db.locks.UnlockMulti(keys)
	//count = db.locks.getLockCount()
	//fmt.Println(count)
	//if count != 0 {
	//	t.Error("some locks can't be released", count)
	//}
}

func TestString(t *testing.T) {

	test1 := "\u0000"
	fmt.Println(test1)

}
