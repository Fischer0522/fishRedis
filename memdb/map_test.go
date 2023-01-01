package memdb

import (
	"strconv"
	"testing"
)

func TestSetGet(t *testing.T) {
	length := 10000
	con := NewConcurrentMap(1024)
	for i := 0; i < length; i++ {
		con.Set("key"+strconv.Itoa(i), i)
	}
	if con.Len() != length {
		t.Error("the number of key is wrong")
	}
	for i := 0; i < length; i++ {
		_, ok := con.Get("key" + strconv.Itoa(i))
		if !ok {
			t.Errorf("can not get key%d", i)
		}
	}
	for i := 0; i < length; i++ {
		con.Delete("key" + strconv.Itoa(i))
	}
	if con.Len() != 0 {
		t.Error("delete failed,some keys are still in the map")
	}
	for i := 0; i < length; i++ {
		con.Set("key"+strconv.Itoa(i), i)
	}
	con.Clear()
	if con.Len() != 0 {
		t.Error("clear failed,some keys are still in the map")
	}
}

func TestConcurrentMap_Keys(t *testing.T) {
	length := 10000
	con := NewConcurrentMap(1024)
	originKey := make(map[string]struct{})
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		originKey[key] = struct{}{}
		con.Set(key, i)
	}
	result := con.Keys()
	for _, ele := range result {
		_, ok := originKey[ele]
		if !ok {
			t.Error("missing key")
		}
	}

	for i := 0; i < length/4; i++ {
		key := "key" + strconv.Itoa(i)
		con.Delete(key)
		delete(originKey, key)
	}
	result = con.Keys()
	for _, ele := range result {
		_, ok := originKey[ele]
		if !ok {
			t.Error("missing key")
		}
	}

	for i := 0; i < length/4; i++ {
		key := "key" + strconv.Itoa(i)
		con.Delete(key)
		delete(originKey, key)
	}
	result = con.Keys()
	for _, ele := range result {
		_, ok := originKey[ele]
		if !ok {
			t.Error("missing key")
		}
	}

}

func TestExist(t *testing.T) {
	mp := NewConcurrentMap(DEFAULT_SIZE)
	length := 10000
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		oldVal := "old" + strconv.Itoa(i)
		mp.Set(key, oldVal)
	}
	// try setIfExist
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		newVal := "new" + strconv.Itoa(i)
		result := mp.SetIfExist(key, newVal)
		if result != 1 {
			t.Error("set if Exist failed")
		}
	}
	for i := 0; i < length; i++ {
		key := "key" + strconv.Itoa(i)
		val, ok := mp.Get(key)
		if !ok {
			t.Error("can not find the key")
		}
		if val != "new"+strconv.Itoa(i) {
			t.Error("the val is wrong")
		}
	}

	mp.Clear()

	for i := 0; i < length; i++ {
		key := "oldKey" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		mp.Set(key, val)
	}

	for i := 0; i < length; i++ {
		key := "oldKey" + strconv.Itoa(i)
		val := "temp"
		ok := mp.SetIfNotExist(key, val)
		if ok == 1 {
			t.Error("should not set the exist key")
		}
	}

	for i := 0; i < length; i++ {
		key := "newKey" + strconv.Itoa(i)
		val := "temp"
		ok := mp.SetIfNotExist(key, val)
		if ok != 1 {
			t.Error("set the key that not exist failed")
		}
	}
}
