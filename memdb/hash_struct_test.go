package memdb

import "testing"

func TestSetAndGet(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("test"))
	res := hash.Get("test")
	if string(res) != "test" {
		t.Errorf("hset hget failed")
	}

}

func TestDel(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("test"))
	res := hash.Del("test")
	if res != 1 {
		t.Errorf("hdel failed")
	}
	res = hash.Del("test")
	if res != 0 {
		t.Errorf("hdel failed")
	}
}

func TestKeysAndVals(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("test"))
	hash.Set("test1", []byte("test1"))
	res := hash.KeysAndVals()
	if len(res) != 4 {
		t.Errorf("hkeys hvals failed")
	}
	for index, ele := range res {
		if index%2 == 0 {
			if string(ele) != "test" && string(ele) != "test1" {
				t.Errorf("hkeys failed")
			}
		} else {
			if string(ele) != "test" && string(ele) != "test1" {
				t.Errorf("hvals failed")
			}
		}
	}
}

func TestLen(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("test"))
	hash.Set("test1", []byte("test1"))
	res := hash.Len()
	if res != 2 {
		t.Errorf("hlen failed")
	}
}

func TestStrlen(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("test"))
	res := hash.Strlen("test")
	if res != 4 {
		t.Errorf("hstrlen failed")
	}
}

func TestHash_IncrBy(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("1"))
	res, err := hash.IncrBy("test", 1)
	if err != nil {
		t.Errorf("hincrby failed err:%v", err)
	}
	if res != 2 {
		t.Errorf("hincrby failed")
	}

	res, err = hash.IncrBy("test1", 23)
	if err != nil {
		t.Errorf("hincrby failed err:%v", err)
	}
	if res != 23 {
		t.Errorf("hincrby failed")
	}
	res, err = hash.IncrBy("test1", -23)
	if err != nil {
		t.Errorf("hincrby failed err:%v", err)
	}
	res, err = hash.IncrBy("test2", -23)
	if err != nil {
		t.Errorf("hincrby failed err:%v", err)
	}
	if res != -23 {
		t.Errorf("hincrby failed")
	}

}
func TestIncrBYFloat(t *testing.T) {
	hash := NewHash()
	hash.Set("test", []byte("1.1"))
	res, err := hash.IncrByFloat("test", 1.1)
	if err != nil {
		t.Errorf("hincrbyfloat failed err:%v", err)
	}
	if res != 2.2 {
		t.Errorf("hincrbyfloat failed")
	}

	res, err = hash.IncrByFloat("test1", 23.23)
	if err != nil {
		t.Errorf("hincrbyfloat failed err:%v", err)
	}
	if res != 23.23 {
		t.Errorf("hincrbyfloat failed")
	}
	res, err = hash.IncrByFloat("test1", -23.23)
	if err != nil {
		t.Errorf("hincrbyfloat failed err:%v", err)
	}
	res, err = hash.IncrByFloat("test2", -23.23)
	if err != nil {
		t.Errorf("hincrbyfloat failed err:%v", err)
	}
	if res != -23.23 {
		t.Errorf("hincrbyfloat failed")
	}

}
