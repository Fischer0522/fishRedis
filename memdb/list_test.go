package memdb

import (
	"bytes"
	"fishRedis/dblog"
	"fishRedis/resp"
	"fmt"
	"strconv"
	"testing"
)

func init() {
	dblog.InitLogger()
}

func NewCommand(name string, args ...string) [][]byte {
	cmd := make([][]byte, len(args)+1)
	cmd[0] = []byte(name)
	for i, arg := range args {
		cmd[i+1] = []byte(arg)
	}
	return cmd
}
func TestPushAndPopList(t *testing.T) {
	mem := NewMemdb()

	length := 10
	cmd := NewCommand("lpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	lPushList(mem, cmd)
	cmd = NewCommand("lpop", "list")
	for i := 0; i < length; i++ {
		res := lPopList(mem, cmd)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte(strconv.Itoa(length-i))).ToBytes()) {
			t.Errorf("wrong pop result, result:%s, expect:%s", res.ToBytes(), resp.MakeBulkData([]byte(strconv.Itoa(length-i))).ToBytes())
		}
	}
	mem.db.Clear()
	cmd = NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	rPushList(mem, cmd)
	cmd = NewCommand("rpop", "list")
	for i := 0; i < length; i++ {
		res := rPopList(mem, cmd)
		if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte(strconv.Itoa(length-i))).ToBytes()) {
			t.Errorf("wrong pop result, result:%s, expect:%s", res.ToBytes(), resp.MakeBulkData([]byte(strconv.Itoa(length-i))).ToBytes())
		}
	}
}

func TestLIndexList(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("lpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	lPushList(mem, cmd)
	cmd = NewCommand("lindex", "list", "0")
	res := lIndexList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("10")).ToBytes()) {
		t.Errorf("wrong lindex result, result:%s, expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("10")).ToBytes())
	}
	cmd = NewCommand("lindex", "list", "-1")
	res = lIndexList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData([]byte("1")).ToBytes()) {
		t.Errorf("wrong lindex result, result:%s, expect:%s", res.ToBytes(), resp.MakeBulkData([]byte("1")).ToBytes())
	}
	cmd = NewCommand("lindex", "list", "11")
	res = lIndexList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeBulkData(nil).ToBytes()) {
		t.Errorf("wrong lindex result, result:%s, expect:%s", res.ToBytes(), resp.MakeBulkData(nil).ToBytes())
	}

}
func TestLLenList(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("lpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	lPushList(mem, cmd)
	cmd = NewCommand("llen", "list")
	res := lLenList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(10).ToBytes()) {
		t.Errorf("wrong llen result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(10).ToBytes())
	}
	mem.db.Clear()
	cmd = NewCommand("llen", "list")
	res = lLenList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("wrong llen result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
}

func TestPushxList(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("lpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	lPushList(mem, cmd)
	cmd = NewCommand("lpushx", "list", "11")
	res := lPushxList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(11).ToBytes()) {
		t.Errorf("wrong lpushx result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(11).ToBytes())
	}
	cmd = NewCommand("lpushx", "list2", "11")
	res = lPushxList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("wrong lpushx result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	mem.db.Clear()
	cmd = NewCommand("lpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	lPushList(mem, cmd)
	cmd = NewCommand("rpushx", "list", "11")
	res = rPushxList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(11).ToBytes()) {
		t.Errorf("wrong rpushx result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(11).ToBytes())
	}
	cmd = NewCommand("rpushx", "list2", "11")
	res = rPushxList(mem, cmd)
	if !bytes.Equal(res.ToBytes(), resp.MakeIntData(0).ToBytes()) {
		t.Errorf("wrong rpushx result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}

}
func TestLPosTest(t *testing.T) {
	mem := NewMemdb()
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	cmd = NewCommand("lpos", "list", "1")
	res := lPosList(mem, cmd)
	expect := resp.MakeArrayData([]resp.RedisData{
		resp.MakeIntData(0),
	})

	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeIntData(0),
		resp.MakeIntData(10),
	})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "2", "maxlen", "2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeIntData(0),
	})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "2", "maxlen", "2", "RANK", "1")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeIntData(0),
	})

	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), resp.MakeIntData(0).ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "2", "maxlen", "20", "RANK", "2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeIntData(0),
		resp.MakeIntData(10),
	})
	cmd = NewCommand("lpos", "list", "1", "count", "2", "maxlen", "25", "rank", "2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(10), resp.MakeIntData(20)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "3", "maxlen", "25", "rank", "1")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(0), resp.MakeIntData(10), resp.MakeIntData(20)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "3", "maxlen", "25", "rank", "2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(10), resp.MakeIntData(20)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "3", "maxlen", "10000", "rank", "3")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(20), resp.MakeIntData(30)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// rank < 0
	cmd = NewCommand("lpos", "list", "1", "count", "3", "maxlen", "10000", "rank", "-1")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(30), resp.MakeIntData(20), resp.MakeIntData(10)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lpos", "list", "1", "count", "3", "maxlen", "10000", "rank", "-2")
	res = lPosList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(20), resp.MakeIntData(10), resp.MakeIntData(0)})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lpos result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

}

func TestLInsert(t *testing.T) {
	mem := NewMemdb()
	// use rpush to init list
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res := rPushList(mem, cmd)
	expect := resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// insert before
	cmd = NewCommand("linsert", "list", "before", "1", "0")
	res = lInsertList(mem, cmd)
	expect = resp.MakeIntData(11)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong linsert result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// insert after
	cmd = NewCommand("linsert", "list", "after", "1", "1.5")
	res = lInsertList(mem, cmd)
	expect = resp.MakeIntData(12)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong linsert result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// print the insert result
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	for _, element := range res.(*resp.ArrayData).Data() {
		fmt.Printf("%s ", element.ToBytes())
	}
	// use lpop to verify the insert result
	cmd = NewCommand("lpop", "list")
	res = lPopList(mem, cmd)
	expectBulk := resp.MakeBulkData([]byte("0"))
	if !bytes.Equal(res.ToBytes(), expectBulk.ToBytes()) {
		t.Errorf("wrong lpop result, result:%s, expect:%s", res.ToBytes(), expectBulk.ToBytes())
	}
	cmd = NewCommand("lpop", "list")
	res = lPopList(mem, cmd)
	// igore 1
	cmd = NewCommand("lpop", "list")
	res = lPopList(mem, cmd)
	expectBulk = resp.MakeBulkData([]byte("1.5"))
	if !bytes.Equal(res.ToBytes(), expectBulk.ToBytes()) {
		t.Errorf("wrong lpop result, result:%s, expect:%s", res.ToBytes(), expectBulk.ToBytes())
	}
}

func TestLRangeList(t *testing.T) {
	var expect resp.RedisData
	mem := NewMemdb()
	// use rpush to init list
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res := rPushList(mem, cmd)
	expect = resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// lrange
	cmd = NewCommand("lrange", "list", "0", "3")
	res = lRangeList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("1")), resp.MakeBulkData([]byte("2")), resp.MakeBulkData([]byte("3")), resp.MakeBulkData([]byte("4"))})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeBulkData([]byte("1")),
		resp.MakeBulkData([]byte("2")),
		resp.MakeBulkData([]byte("3")),
		resp.MakeBulkData([]byte("4")),
		resp.MakeBulkData([]byte("5")),
		resp.MakeBulkData([]byte("6")),
		resp.MakeBulkData([]byte("7")),
		resp.MakeBulkData([]byte("8")),
		resp.MakeBulkData([]byte("9")),
		resp.MakeBulkData([]byte("10")),
	})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	cmd = NewCommand("lrange", "list", "0", "100")
	res = lRangeList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeBulkData([]byte("1")),
		resp.MakeBulkData([]byte("2")),
		resp.MakeBulkData([]byte("3")),
		resp.MakeBulkData([]byte("4")),
		resp.MakeBulkData([]byte("5")),
		resp.MakeBulkData([]byte("6")),
		resp.MakeBulkData([]byte("7")),
		resp.MakeBulkData([]byte("8")),
		resp.MakeBulkData([]byte("9")),
		resp.MakeBulkData([]byte("10")),
	})
	// test lrange start < 0 and end < 0
	cmd = NewCommand("lrange", "list", "-3", "-1")
	res = lRangeList(mem, cmd)
	expect = resp.MakeArrayData([]resp.RedisData{
		resp.MakeBulkData([]byte("8")),
		resp.MakeBulkData([]byte("9")),
		resp.MakeBulkData([]byte("10")),
	})
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// test larnge start > length result should be a bulk "empty list or set"
	cmd = NewCommand("lrange", "list", "100", "200")
	res = lRangeList(mem, cmd)
	expect = resp.MakeBulkData([]byte("empty list or set"))
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// test lrange start > end result should be bulk "empty list or set"
	cmd = NewCommand("lrange", "list", "3", "2")
	res = lRangeList(mem, cmd)
	expect = resp.MakeBulkData([]byte("empty list or set"))
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

}

func TestLRemList(t *testing.T) {
	mem := NewMemdb()
	// use rpush to init list
	var expect resp.RedisData
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res := rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	expect = resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrem to remove all 1 params count = 0 means remove all element that equal to target
	cmd = NewCommand("lrem", "list", "0", "1")
	res = lRemList(mem, cmd)
	expect = resp.MakeIntData(4)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrem result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrange to check result there should be no 1 in list
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop  4 times to generate expect result
	var expectData []resp.RedisData
	for j := 0; j < 4; j++ {
		for i := 2; i <= 10; i++ {
			expectData = append(expectData, resp.MakeBulkData([]byte(strconv.Itoa(i))))
		}
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrem to remove 3  target is 2 params count = 0 means remove all element that equal to target
	// count = 3 means remove 3 times that equal to target and delete it from front to back
	// count = -3 means remove 3 times that equal to target and delete it from back to front
	cmd = NewCommand("lrem", "list", "3", "2")
	res = lRemList(mem, cmd)
	expect = resp.MakeIntData(3)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrem result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrange to check result there should be  in list
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop  4 times to generate expect result
	expectData = []resp.RedisData{}
	for j := 0; j < 4; j++ {
		for i := 2; i <= 10; i++ {
			if j != 3 && i == 2 {
				continue
			}
			expectData = append(expectData, resp.MakeBulkData([]byte(strconv.Itoa(i))))
		}
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

	// clear the db
	mem = NewMemdb()
	// use rpush to init list 4 times
	cmd = NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res = rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	rPushList(mem, cmd)
	// use ltrem to remove  target is 1 3 times and count = -3 means remove 3 times that equal to target and delete it from back to front
	cmd = NewCommand("lrem", "list", "-3", "1")
	res = lRemList(mem, cmd)
	expect = resp.MakeIntData(3)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrem result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrange to check result there is only one 1 in list and is at the front of list
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop  4 times to generate expect result
	expectData = []resp.RedisData{}
	for j := 0; j < 4; j++ {
		for i := 1; i <= 10; i++ {
			if j != 0 && i == 1 {
				continue
			}
			expectData = append(expectData, resp.MakeBulkData([]byte(strconv.Itoa(i))))
		}
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
}

func TestLSetList(t *testing.T) {
	mem := NewMemdb()
	// use rpush to init list
	var expect resp.RedisData
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res := rPushList(mem, cmd)
	expect = resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use for loop and lset to set value 114514 from index 0 to index 5
	for i := 0; i < 5; i++ {
		cmd = NewCommand("lset", "list", strconv.Itoa(i), "114514")
		res = lSetList(mem, cmd)
		expect = resp.MakeStringData("OK")
		if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
			t.Errorf("wrong lset result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
		}
	}
	// use for loop and lset to set value 1919810 from index -1 to index -5
	for i := -1; i >= -5; i-- {
		cmd = NewCommand("lset", "list", strconv.Itoa(i), "1919810")
		res = lSetList(mem, cmd)
		expect = resp.MakeStringData("OK")
		if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
			t.Errorf("wrong lset result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
		}
	}
	// use lrange to check result
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop 1 time to generate expect result
	var expectData []resp.RedisData
	for i := 0; i < 10; i++ {
		if i < 5 {
			expectData = append(expectData, resp.MakeBulkData([]byte("114514")))
		} else {
			expectData = append(expectData, resp.MakeBulkData([]byte("1919810")))
		}
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

}
func TestTrimList(t *testing.T) {
	mem := NewMemdb()
	// use rpush to init list
	var expect resp.RedisData
	cmd := NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res := rPushList(mem, cmd)
	expect = resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use ltrim to trim list
	cmd = NewCommand("ltrim", "list", "0", "4")
	res = lTrimList(mem, cmd)
	expect = resp.MakeStringData("OK")
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong ltrim result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrange to check result
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop 1 time to generate expect result
	var expectData []resp.RedisData
	for i := 1; i <= 5; i++ {
		expectData = append(expectData, resp.MakeBulkData([]byte(strconv.Itoa(i))))
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

	// clear db
	mem = NewMemdb()
	// test trim list with negative index
	cmd = NewCommand("rpush", "list", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	res = rPushList(mem, cmd)
	expect = resp.MakeIntData(10)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong rpush result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use ltrim to trim list
	cmd = NewCommand("ltrim", "list", "-5", "-1")
	res = lTrimList(mem, cmd)
	expect = resp.MakeStringData("OK")
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong ltrim result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}
	// use lrange to check result
	cmd = NewCommand("lrange", "list", "0", "-1")
	res = lRangeList(mem, cmd)
	// use for loop 1 time to generate expect result
	expectData = nil
	for i := 6; i <= 10; i++ {
		expectData = append(expectData, resp.MakeBulkData([]byte(strconv.Itoa(i))))
	}
	expect = resp.MakeArrayData(expectData)
	if !bytes.Equal(res.ToBytes(), expect.ToBytes()) {
		t.Errorf("wrong lrange result, result:%s, expect:%s", res.ToBytes(), expect.ToBytes())
	}

}
