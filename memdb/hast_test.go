package memdb

import (
	"bytes"
	"fishRedis/dblog"
	"fishRedis/resp"
	"testing"
)

func init() {
	dblog.InitLogger()
}

func TestSetAndGetHash(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if res == nil {
		t.Error("hset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hget", "myhash", "field1")
	res = hGetHash(mem, cmd)
	if res == nil {
		t.Error("hget failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("value1")).ToBytes()) {
		t.Errorf("hget failed,result:%s,expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("value1")).ToBytes())
	}
	// test set multi field
	cmd = NewCommand("hset", "myhash", "field2", "value2", "field3", "value3")
	res = hSetHash(mem, cmd)
	if res == nil {
		t.Error("hmset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(2).ToBytes()) {
		t.Errorf("hmset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(2).ToBytes())
	}
	cmd = NewCommand("hmget", "myhash", "field1", "field2", "field3")
	res = hMgetHash(mem, cmd)
	if res == nil {
		t.Error("hmget failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeArrayData([]resp.RedisData{
		resp.MakeBulkData([]byte("value1")),
		resp.MakeBulkData([]byte("value2")),
		resp.MakeBulkData([]byte("value3")),
	}).ToBytes()) {
		t.Errorf("hmget failed,result:%s,expect:%s", res.ToBytes(), resp.MakeArrayData([]resp.RedisData{
			resp.MakeBulkData([]byte("value1")),
			resp.MakeBulkData([]byte("value2")),
			resp.MakeBulkData([]byte("value3")),
		}).ToBytes())
	}
}
func TestDelHash(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if res == nil {
		t.Error("hset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hdel", "myhash", "field1")
	res = hDelHash(mem, cmd)
	if res == nil {
		t.Error("hdel failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hdel failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hget", "myhash", "field1")
	res = hGetHash(mem, cmd)
	if res == nil {
		t.Error("hget failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
		t.Errorf("hget failed,result:%s,expect:%s", res.ToBytes(), resp.MakeBulkData(nil).ToBytes())
	}

}
func TestHSetNx(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if res == nil {
		t.Error("hset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hsetnx", "myhash", "field1", "value2")
	res = hSetnxHash(mem, cmd)
	if res == nil {
		t.Error("hsetnx failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("hsetnx failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hsetnx", "myhash", "field2", "value2")
	res = hSetnxHash(mem, cmd)
	if res == nil {
		t.Error("hsetnx failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hsetnx failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	cmd = NewCommand("hget", "myhash", "field1")
	res = hGetHash(mem, cmd)
	if res == nil {
		t.Error("hget failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("value1")).ToBytes()) {
		t.Errorf("hget failed,result:%s,expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("value1")).ToBytes())
	}

}
func TestHLen(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if res == nil {
		t.Error("hset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hlen", "myhash")
	res = hLenHash(mem, cmd)
	if res == nil {
		t.Error("hlen failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hlen failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "value2")
	res = hSetHash(mem, cmd)
	if res == nil {
		t.Error("hset failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hlen", "myhash")
	res = hLenHash(mem, cmd)
	if res == nil {
		t.Error("hlen failed")
	}
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(2).ToBytes()) {
		t.Errorf("hlen failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(2).ToBytes())
	}
}
func TestHExists(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hexists", "myhash", "field1")
	res = hExistsHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hexists failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hexists", "myhash", "field2")
	res = hExistsHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("hexists failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}

}
func TestHKeys(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "value2")
	res = hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hkeys", "myhash")
	res = hKeysHash(mem, cmd)
	result := make([]resp.RedisData, 0)
	result = append(result, resp.MakeBulkData([]byte("field1")))
	result = append(result, resp.MakeBulkData([]byte("field2")))
	if !bytes.Equal(res.ToBytes(), resp.MakeArrayData(result).ToBytes()) {
		t.Errorf("hkeys failed,result:%s,expect:%s", res.ToBytes(), resp.MakeArrayData(result).ToBytes())
	}
}
func TestHVals(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "value2")
	res = hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hvals", "myhash")
	res = hValsHash(mem, cmd)
	result := make([]resp.RedisData, 0)
	result = append(result, resp.MakeBulkData([]byte("value1")))
	result = append(result, resp.MakeBulkData([]byte("value2")))
	if !bytes.Equal(res.ToBytes(), resp.MakeArrayData(result).ToBytes()) {
		t.Errorf("hvals failed,result:%s,expect:%s", res.ToBytes(), resp.MakeArrayData(result).ToBytes())
	}
}
func TestHGetAll(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "value2")
	res = hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hgetall", "myhash")
	res = hGetAllHash(mem, cmd)
	result := make([]resp.RedisData, 0)
	result = append(result, resp.MakeBulkData([]byte("field1")))
	result = append(result, resp.MakeBulkData([]byte("value1")))
	result = append(result, resp.MakeBulkData([]byte("field2")))
	result = append(result, resp.MakeBulkData([]byte("value2")))
	if !bytes.Equal(res.ToBytes(), resp.MakeArrayData(result).ToBytes()) {
		t.Errorf("hgetall failed,result:%s,expect:%s", res.ToBytes(), resp.MakeArrayData(result).ToBytes())
	}
}
func TestHIncrBy(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "10")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hincrby", "myhash", "field1", "10")
	res = hIncrByHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(20).ToBytes()) {
		t.Errorf("hincrby failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(20).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "10")
	res = hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hincrby", "myhash", "field2", "-10")
	res = hIncrByHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("hincrby failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
}
func TestHIncrByFloat(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "10.50")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hincrbyfloat", "myhash", "field1", "0.1")
	res = hIncrByFloatHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("10.6")).ToBytes()) {
		t.Errorf("hincrbyfloat failed,result:%s,expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("10.6")).ToBytes())
	}
	cmd = NewCommand("hset", "myhash", "field2", "10.0")
	res = hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hincrbyfloat", "myhash", "field2", "-5.0")
	res = hIncrByFloatHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("5")).ToBytes()) {
		t.Errorf("hincrbyfloat failed,result:%s,expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("5")).ToBytes())
	}
}
func TestHStrLen(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("hset", "myhash", "field1", "value1")
	res := hSetHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(1).ToBytes()) {
		t.Errorf("hset failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(1).ToBytes())
	}
	cmd = NewCommand("hstrlen", "myhash", "field1")
	res = hStrLenHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(6).ToBytes()) {
		t.Errorf("hstrlen failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(6).ToBytes())
	}
	cmd = NewCommand("hstrlen", "myhash", "field2")
	res = hStrLenHash(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("hstrlen failed,result:%s,expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}

}
