package memdb

import (
	"bytes"
	"fishRedis/dblog"
	"fishRedis/resp"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSetGetString(t *testing.T) {
	mem := NewMemdb()
	round := 10000
	length := 10

	client := &RedisClient{
		RedisDb: mem,
	}
	for i := 0; i < round; i++ {
		for j := 0; j < length; j++ {
			key := []byte("key" + strconv.Itoa(i) + strconv.Itoa(j))
			val := []byte("val" + strconv.Itoa(i) + strconv.Itoa(j))
			cmd := [][]byte{[]byte("set"), key, val}
			client.Args = cmd
			res := setString(client)
			if !bytes.Equal(res.ToBytes(), []byte("+OK\r\n")) {
				t.Errorf("set reply error")
			}
			cmd = [][]byte{[]byte("get"), key}
			client.Args = cmd
			ans := resp.MakeBulkData(val)
			resGet := getString(client)

			if !bytes.Equal(ans.ToBytes(), resGet.ToBytes()) {
				t.Errorf("get key : %s failed", key)
			}
		}
	}
	if mem.db.count != round*length {
		t.Error("the count of keys is invalid")
	}
}

func TestSetWithParam(t *testing.T) {
	dblog.InitLogger()
	mem := NewMemdb()
	cmdName := []byte("set")
	nx := []byte("nx")
	xx := []byte("xx")
	client := &RedisClient{
		RedisDb: mem,
	}
	// test set nx

	length := 10000
	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key, val}
		client.Args = cmd
		if i%7 == 0 {
			setString(client)
		}
	}

	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		cmd := [][]byte{[]byte("get"), key}
		client.Args = cmd
		if i%7 == 0 {
			res := getString(client)
			if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(val).ToBytes()) {
				t.Error("set failed")
			}
		}
	}

	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("newVal" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key, val, nx}
		client.Args = cmd
		setRes := setString(client)
		if i%7 == 0 {
			if !bytes.Equal(setRes.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
				t.Errorf("set nx failed,i = %d key = %s", i, string(key))
			}
		} else {
			if !bytes.Equal(setRes.ToBytes(), []byte("+OK\r\n")) {
				t.Errorf("set normal key failed,i = %d, key = %s", i, string(key))
			}
		}
	}
	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		newVal := []byte("newVal" + strconv.Itoa(i))
		cmd := [][]byte{[]byte("get"), key}
		client.Args = cmd
		res := getString(client)
		if i%7 == 0 {
			if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(val).ToBytes()) {
				t.Errorf("get nx key %s failed", key)
			}

		} else {
			if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(newVal).ToBytes()) {
				t.Errorf("get normal key %s failed", key)
			}
		}
	}
	// test xx
	mem.db.Clear()
	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key, val}
		client.Args = cmd
		if i%7 == 0 {
			setString(client)
		}
	}
	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("newVal" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key, val, xx}
		client.Args = cmd
		setRes := setString(client)
		if i%7 != 0 {
			if !bytes.Equal(setRes.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
				t.Errorf("set nx failed,i = %d key = %s", i, string(key))
			}
		} else {
			if !bytes.Equal(setRes.ToBytes(), []byte("+OK\r\n")) {
				t.Errorf("set normal key failed,i = %d, key = %s", i, string(key))
			}
		}
	}

	for i := 0; i < length; i++ {
		key := []byte("key" + strconv.Itoa(i))
		newVal := []byte("newVal" + strconv.Itoa(i))
		cmd := [][]byte{[]byte("get"), key}
		client.Args = cmd
		res := getString(client)
		if i%7 == 0 {
			if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(newVal).ToBytes()) {
				t.Errorf("get xx key %s failed", key)
			}

		} else {
			if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
				t.Errorf("get nil key %s failed %v", key, string(res.ToBytes()))
			}
		}
	}
}

func TestSetEx(t *testing.T) {
	dblog.InitLogger()
	mem := NewMemdb()
	length := 10000
	client := &RedisClient{
		RedisDb: mem,
	}
	for i := 0; i < length; i++ {
		cmdName := []byte("set")
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		ex := []byte("ex")
		ttl := []byte("3")
		cmd := [][]byte{cmdName, key, val, ex, ttl}
		client.Args = cmd
		res := setString(client)
		if !bytes.Equal(res.ToBytes(), []byte("+OK\r\n")) {
			t.Error("set ex failed")
		}
	}
	for i := 0; i < length; i++ {
		cmdName := []byte("get")
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key}
		client.Args = cmd
		res := getString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(val).ToBytes()) {
			t.Error("get before expired failed")
		}
	}
	time.Sleep(time.Second * 5)
	for i := 0; i < length; i++ {
		cmdName := []byte("get")
		key := []byte("key" + strconv.Itoa(i))
		cmd := [][]byte{cmdName, key}
		client.Args = cmd
		res := getString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
			t.Error("get after expired failed")
		}
	}
	if mem.db.count != 0 {
		t.Error("delete expired key failed")
	}
}

func TestKeepTTL(t *testing.T) {
	mem := NewMemdb()
	dblog.InitLogger()
	client := &RedisClient{
		RedisDb: mem,
	}
	cmdName := []byte("set")
	key := []byte("key1")
	val := []byte("val1")
	keepttl := []byte("keepttl")
	ex := []byte("ex")
	ttlTime := []byte("3")
	cmdex := [][]byte{cmdName, key, val, ex, ttlTime}
	client.Args = cmdex
	res := setString(client)
	if !bytes.Equal(res.ToBytes(), []byte("+OK\r\n")) {
		t.Error("set ex failed")
	}
	if !mem.CheckTTL("key1") {
		t.Error("can not find ttl")
	}
	cmdGet := [][]byte{[]byte("get"), key}
	client.Args = cmdGet
	res = getString(client)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(val).ToBytes()) {
		t.Error("get old val failed")
	}
	newVal := []byte("val2")

	cmdKeepTTL := [][]byte{cmdName, key, newVal, keepttl}
	client.Args = cmdKeepTTL
	res = setString(client)
	if !bytes.Equal(res.ToBytes(), []byte("+OK\r\n")) {
		t.Error("set keepttl val failed")
		fmt.Println(string(res.ToBytes()))
	}
	if !mem.CheckTTL("key1") {
		t.Error("keepttl failed")
	}
	time.Sleep(4 * time.Second)
	if mem.CheckTTL("key1") {
		t.Error("key should by expired")
	}

}

func TestMSetGet(t *testing.T) {
	mem := NewMemdb()
	dblog.InitLogger()
	client := &RedisClient{
		RedisDb: mem,
	}
	keyAndVal := [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"), []byte("k3"), []byte("v3")}
	cmdName := []byte("mset")
	cmdSet := [][]byte{cmdName}
	cmdSet = append(cmdSet, keyAndVal...)
	client.Args = cmdSet
	res := mSetString(client)
	if !bytes.Equal(res.ToBytes(), []byte("+OK\r\n")) {
		t.Error("set failed")
	}
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}

	vals := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	getResult := make([]resp.RedisData, 0)
	for _, val := range vals {
		getResult = append(getResult, resp.MakeBulkData(val))
	}
	ans := resp.MakeArrayData(getResult)
	cmdName = []byte("mget")
	cmdGet := [][]byte{cmdName}
	cmdGet = append(cmdGet, keys...)
	client.Args = cmdGet
	mgetResult := mGetString(client)
	if !bytes.Equal(mgetResult.ToBytes(), ans.ToBytes()) {
		t.Error("mget failed")
		fmt.Println(string(mgetResult.ToBytes()))
		fmt.Println("-------------")
		fmt.Println(string(ans.ToBytes()))
	}

}

func TestGetRange(t *testing.T) {
	mem := NewMemdb()
	dblog.InitLogger()
	client := &RedisClient{
		RedisDb: mem,
	}
	cmdName := []byte("getrange")
	indexs := [][]int{{1, 4}, {3, 5}, {3, 12}, {2, 8}, {12, 4}, {-3, -1}}
	strs := []string{"hello", "hello redis", "postgresql", "he12345", "helloworld", "session"}
	ans := [][]byte{[]byte("ello"), []byte("lo "), []byte("tgresql"), []byte("12345"), nil, []byte("ion")}
	for i, str := range strs {
		cmd := [][]byte{[]byte("set"), []byte(strconv.Itoa(i)), []byte(str)}
		client.Args = cmd
		setString(client)
	}
	for i, index := range indexs {
		start := index[0]
		end := index[1]

		cmd := [][]byte{cmdName, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(start)), []byte(strconv.Itoa(end))}
		client.Args = cmd
		res := getRangeString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(ans[i]).ToBytes()) {
			t.Error("failed")
			fmt.Printf("1 : %s 2 : %s\n", string(res.ToBytes()), string(resp.MakeBulkData(ans[i]).ToBytes()))

		} else {
			fmt.Printf("success,%s\n", ans[i])
		}

	}
}

func TestSetRange(t *testing.T) {
	mem := NewMemdb()
	dblog.InitLogger()
	client := &RedisClient{
		RedisDb: mem,
	}
	indexs := []int{1, 4, 8, 10, 5, 0}
	newsubStr := "test"
	length := []int{5, 11, 12, 14, 10, 7}
	strs := []string{"hello", "hello redis", "postgresql", "he12345", "helloworld", "session"}
	ans := []string{"htest", "helltestdis", "postgrestest", "he12345\x00\x00\x00test", "hellotestd", "testion"}
	for i, str := range strs {
		cmd := [][]byte{[]byte("set"), []byte(strconv.Itoa(i)), []byte(str)}
		client.Args = cmd
		setString(client)
	}
	for i := 0; i < len(indexs); i++ {
		fmt.Println("round", i)
		cmd := [][]byte{[]byte("setrange"), []byte(strconv.Itoa(i)), []byte(strconv.Itoa(indexs[i])), []byte(newsubStr)}
		client.Args = cmd
		res := setRangeString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeIntData(int64(length[i])).ToBytes()) {
			t.Errorf("setrange failed, i = %d  ", i)
			fmt.Println(string(res.ToBytes()), length[i])
		}
	}

	for i := 0; i < len(indexs); i++ {
		cmd := [][]byte{[]byte("get"), []byte(strconv.Itoa(i))}
		client.Args = cmd
		res := getString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte(ans[i])).ToBytes()) {

			t.Errorf("expect %s result %s", resp.MakeBulkData([]byte(ans[i])).ToBytes(), string(res.ToBytes()))
		}

	}
}

func TestStrLen(t *testing.T) {
	mem := NewMemdb()
	dblog.InitLogger()
	/*set a key first*/
	client := &RedisClient{
		RedisDb: mem,
	}
	key := []byte("k1")
	val := []byte("7231789237128")
	cmd := [][]byte{[]byte("set"), key, val}
	client.Args = cmd
	setString(client)

	cmd = [][]byte{[]byte("strlen"), []byte("k1")}
	res := strLenString(client)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(int64(len(val))).ToBytes()) {
		t.Error("wrong length")
	}
}

func TestIncrDecr(t *testing.T) {
	mem := NewMemdb()
	client := &RedisClient{
		RedisDb: mem,
	}
	dblog.InitLogger()
	keyVals := []string{"k1", "1", "k2", "2", "k3", "0"}
	for i := 0; i < len(keyVals); i += 2 {
		key := []byte(keyVals[i])
		val := []byte(keyVals[i+1])
		valInt, _ := strconv.ParseInt(keyVals[i+1], 10, 64)
		valInt++
		cmd := [][]byte{[]byte("set"), key, val}
		client.Args = cmd
		setString(client)
		cmdIncr := [][]byte{[]byte("incr"), key}
		client.Args = cmdIncr
		res := incrString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeIntData(valInt).ToBytes()) {
			t.Error("incr failed")
		}
		incrby := 5
		valInt += int64(incrby)
		cmdIncrBy := [][]byte{[]byte("incrby"), key, []byte(strconv.Itoa(incrby))}
		client.Args = cmdIncrBy
		res = incrByString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeIntData(valInt).ToBytes()) {
			t.Error("incrby failed")
		}
		decrby := 5
		valInt -= int64(decrby)
		cmdDecrBy := [][]byte{[]byte("decrby"), key, []byte(strconv.Itoa(incrby))}
		client.Args = cmdDecrBy
		res = decrByString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeIntData(valInt).ToBytes()) {
			t.Error("decrby failed")
		}
		valInt--

		cmdDecr := [][]byte{[]byte("decr"), key}
		client.Args = cmdDecr
		res = decrString(client)
		if !bytes.Equal(res.ToBytes(), resp.MakeIntData(valInt).ToBytes()) {
			t.Error("decr failed")
		}

	}
	for i := 0; i < 100; i++ {
		key := []byte("key" + strconv.Itoa(i))
		var cmdName []byte
		if i%4 == 0 {
			cmdName = []byte("incr")
			cmd := [][]byte{cmdName, key}
			client.Args = cmd
			res := incrString(client)
			if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
				t.Error("incr keyNotExist error")
			}
		} else if i%4 == 1 {
			cmdName = []byte("decr")
			cmd := [][]byte{cmdName, key}
			client.Args = cmd
			res := decrString(client)
			if !bytes.Equal(res.ToBytes(), resp.MakeIntData(-1).ToBytes()) {
				t.Error("decr keyNotExist error")
			}
		} else if i%4 == 2 {
			cmdName = []byte("incrby")
			incrBy := []byte(strconv.FormatInt(int64(i), 10))
			cmd := [][]byte{cmdName, key, incrBy}
			client.Args = cmd
			res := incrByString(client)
			if !bytes.Equal(res.ToBytes(), resp.MakeIntData(int64(i)).ToBytes()) {
				t.Error("decr keyNotExist error")
			}
		} else if i%4 == 3 {
			cmdName = []byte("decrby")
			decrBy := []byte(strconv.FormatInt(int64(i), 10))

			cmd := [][]byte{cmdName, key, decrBy}
			client.Args = cmd
			res := decrByString(client)
			if !bytes.Equal(res.ToBytes(), resp.MakeIntData(int64(-i)).ToBytes()) {
				t.Error("decr keyNotExist error")
			}
		}
	}

}
