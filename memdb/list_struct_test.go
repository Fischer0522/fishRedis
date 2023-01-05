package memdb

import (
	"bytes"
	"fishRedis/dblog"
	"fmt"
	"strconv"
	"testing"
)

func init() {
	dblog.InitLogger()
}
func verifyList(length int, list *List) bool {
	i := 1
	for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
		//fmt.Println(string(currentNode.Val))
		if !bytes.Equal([]byte(strconv.Itoa(length-i)), currentNode.Val) {
			return false
		}
		i++
	}
	return true
}

func generateNode(list *List, length int) {
	for i := 0; i < length; i++ {
		list.lPush([]byte(strconv.Itoa(i)))
	}
}
func generateNodeR(list *List, length int) {
	for i := 0; i < length; i++ {
		list.rPush([]byte(strconv.Itoa(i)))
	}
}
func isEmpty(list *List) bool {
	if list.Length != 0 || list.Tail.Prev != list.Head || list.Head.Next != list.Tail {
		return false
	}
	return true
}

func TestPushAndPop(t *testing.T) {
	list := NewList()
	length := 10
	generateNode(list, length)
	if !verifyList(length, list) {
		t.Errorf("lpush failed")
	}
	if list.Length != length {
		t.Errorf("wrong length")
	}
	for i := 0; i < length; i++ {
		res := list.lPop()
		if !bytes.Equal(res, []byte(strconv.Itoa(length-i-1))) {
			t.Errorf("lpop get wrong res")
			fmt.Println(string(res), length-1-i)
		}
	}
	if list.Length != 0 {
		t.Errorf("lpop failed")
	}
	res := list.lPop()
	if res != nil {
		t.Errorf("should be empty")
	}
	if list.Head.Next != list.Tail || list.Tail.Prev != list.Head {
		t.Errorf("should by empty")
	}

	generateNodeR(list, 10)
	for i := 0; i < length; i++ {
		res := list.rPop()
		if !bytes.Equal(res, []byte(strconv.Itoa(length-1-i))) {
			t.Errorf("rpop get the wrong ans,%s,%d", string(res), length-i-1)
		}
	}
	if !isEmpty(list) {
		t.Errorf("should be empty")
	}
}

func TestIndex(t *testing.T) {
	list := NewList()
	length := 10
	generateNode(list, length)

	for i := 0; i < length; i++ {
		res := list.index(i)
		if !bytes.Equal(res, []byte(strconv.Itoa(length-i-1))) {
			t.Errorf("get wrong index ersult:%s expect: %d", string(res), length-i-1)
		}
	}
	for i := 0; i < length; i++ {
		res := list.index(i - length)
		if !bytes.Equal(res, []byte(strconv.Itoa(length-i-1))) {
			t.Errorf("get wrong index result:%s expect:%d", string(res), length-i-1)
		}
	}

}

/*need a new test */
func TestLPos(t *testing.T) {
	list := NewList()
	length := 10
	generateNode(list, length)
	for i := 0; i < length; i++ {
		val := []byte(strconv.Itoa(i))
		res := list.lPos(val, 1, 1, 20)[0]
		if res != length-i-1 {
			t.Errorf("wrong pos,result:%d,expect:%d", res, length-1-i)
		}
	}
	generateNode(list, length)
	for i := 0; i < length; i++ {
		val := []byte(strconv.Itoa(i))
		res := list.lPos(val, 2, 1, 30)[0]
		if res != 2*length-i-1 {
			t.Errorf("wrong pos,result:%d,expect:%d", res, length+i)
		}
		resLimit := list.lPos(val, 2, 1, 10)
		if len(resLimit) != 0 {
			t.Errorf("ignore the maxLen limit")
		}

	}
	list.Clear()
	inputList := []string{"a", "b", "c", "1", "2", "3", "c", "c"}
	for _, ele := range inputList {
		list.rPush([]byte(ele))
	}
	res := list.lPos([]byte("c"), 1, 1, list.Length)[0]
	if res != 2 {
		t.Errorf("get wrong pos")
	}
	res = list.lPos([]byte("c"), -1, 1, list.Length)[0]
	if res != 7 {
		t.Errorf("get wrong pos,result:%d,expect:%d", res, 7)
	}
	resArr := list.lPos([]byte("c"), -1, 2, list.Length)
	if resArr[0] != 7 || resArr[1] != 6 {
		t.Errorf("get wrong pos,result:%v,expect:[7,6]", resArr)
	}
	resArr = list.lPos([]byte("c"), 1, 2, list.Length)
	if resArr[0] != 2 || resArr[1] != 6 {
		t.Errorf("get wrong pos,result:%v,expect:[2,6]", resArr)
	}
	resArr = list.lPos([]byte("c"), 1, 0, list.Length)
	if resArr[0] != 2 || resArr[1] != 6 || resArr[2] != 7 {
		t.Errorf("get wrong pos, result:%v,expect:[2:6,7]", resArr)
	}

	list.Clear()
	inputList = []string{"a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3"}
	for _, ele := range inputList {
		list.rPush([]byte(ele))
	}
	resArr = list.lPos([]byte("3"), -1, 3, list.Length)
	if len(resArr) != 3 || resArr[0] != 10 || resArr[1] != 9 || resArr[2] != 8 {
		t.Errorf("get wrong pos,result:%v,expect:[10,9,8]", resArr)
	}

}

func TestInsert(t *testing.T) {
	list := NewList()
	length := 10
	generateNode(list, 10)
	target := []byte(strconv.Itoa(length / 2))
	beforeVal := []byte(strconv.Itoa(10 * length))
	afterVal := []byte(strconv.Itoa(2 * length))
	list.lInsert(true, beforeVal, target)
	list.lInsert(false, afterVal, target)

	pos := list.lPos(target, 1, 1, 20)[0]
	beforeResult := list.index(pos - 1)
	afterResult := list.index(pos + 1)
	if !bytes.Equal(beforeVal, beforeResult) {
		t.Errorf("beforeVal insert failed,result:%s,expect:%s", string(beforeResult), string(beforeVal))
	}
	if !bytes.Equal(afterVal, afterResult) {
		t.Errorf("afterVal insert failed,result:%s,expect:%s", string(afterResult), string(afterVal))
	}

}

func TestLRange(t *testing.T) {
	// test normal range
	list := NewList()
	length := 10
	generateNode(list, 10)
	start := 0
	end := length - 2
	res := list.lRange(start, end)
	for index, ele := range res {
		ans := []byte(strconv.Itoa((length - index - 1) - start))
		if !bytes.Equal(ans, ele) {
			t.Errorf("result:%s expect:%s", string(ele), string(ans))
		}
	}
	// test out of range
	start = 0
	end = length + 1212
	res = list.lRange(start, end)
	for index, ele := range res {
		ans := []byte(strconv.Itoa((length - index - 1) - start))
		if !bytes.Equal(ans, ele) {
			t.Errorf("result:%s,expect:%s", string(ele), string(ans))
		}
	}
	// test negative number
	start = -7
	end = -3
	res = list.lRange(start, end)
	for index, ele := range res {
		ans := []byte(strconv.Itoa((length - index - 1) + end))
		if !bytes.Equal(ans, ele) {
			t.Errorf("result :%s,expect:%s", string(ele), string(ans))
		}
	}

	// test empty result
	start = length
	end = length + 1
	res = list.lRange(start, end)
	if len(res) != 0 {
		t.Errorf("should be empty")
	}
	start = 5
	end = start - 1
	res = list.lRange(start, end)
	if len(res) != 0 {
		t.Errorf("should be empty")
	}
}

func TestDeleteByVal(t *testing.T) {
	list := NewList()
	length := 10
	generateNodeR(list, length)
	generateNodeR(list, length)
	// test deleteAll
	target := []byte(strconv.Itoa(2))
	list.deleteByVal(target, 0)
	res := list.lPos(target, 1, 1, length)
	if len(res) != 0 {
		t.Errorf("delete all failed")
	}
	list = NewList()
	generateNodeR(list, length)
	generateNodeR(list, length)
	generateNodeR(list, length)
	count := -2
	list.deleteByVal(target, count)
	res1 := list.lPos(target, 1, 1, 3*length)[0]
	if res1 != 2 {
		t.Errorf("wrong pos result:%d expect %d", res, 2)
	}
	list = NewList()
	generateNodeR(list, length)
	generateNodeR(list, length)
	generateNodeR(list, length)
	count = 2
	list.deleteByVal(target, count)
	res1 = list.lPos(target, 1, 1, 3*length)[0]
	if res1 != 20 {
		t.Errorf("wrong pos reslut:%d,expect %d", res, 20)
	}

}
func printAllElement(list *List) {
	for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
		fmt.Println(string(currentNode.Val))
	}
}
func TestSet(t *testing.T) {
	list := NewList()
	length := 10
	generateNodeR(list, length)
	target := []byte(strconv.Itoa(1000))
	res := list.set(target, length/2)
	if !res {
		t.Errorf("set failed")
		printAllElement(list)
	}
	pos := list.lPos(target, 1, 1, length)[0]
	if pos != length/2 {
		t.Errorf("set pos wrong")
		printAllElement(list)
	}

	// test out of range
	list = NewList()
	generateNodeR(list, length)
	res = list.set(target, length)
	if res {
		t.Errorf("should set failed")
	}

	res = list.set(target, -length-1)
	if res {
		t.Errorf("should set failed")
	}
	res = list.set(target, -3)
	if !res {
		t.Errorf("set negative index failed")
	}
	pos = list.lPos(target, 1, 1, length)[0]
	if pos != length-3 {
		t.Errorf("set negative index failed")

	}
}
func TestTrim(t *testing.T) {
	list := NewList()
	length := 10
	generateNodeR(list, length)
	start := 2
	end := 8
	list.trim(start, end)
	resStart := list.index(0)
	resEnd := list.index(end - start)
	if !bytes.Equal(resStart, []byte(strconv.Itoa(start))) {
		t.Errorf("trim start failed,result:%s,expected:%d", string(resStart), start)
	}
	if !bytes.Equal(resEnd, []byte(strconv.Itoa(end))) {
		t.Errorf("trim end failed,result:%s,expected:%d", string(resEnd), end)
	}

	// test out of range
	list = NewList()
	generateNodeR(list, length)
	start = 0
	end = length + 10
	list.trim(start, end)
	resStart = list.index(start)
	resEnd = list.index(length - 1)
	if !bytes.Equal(resStart, []byte(strconv.Itoa(0))) {
		t.Errorf("...")
	}
	if !bytes.Equal(resEnd, []byte(strconv.Itoa(length-1))) {
		t.Errorf(".....")
	}
	list = NewList()
	generateNodeR(list, length)
	start = -8
	end = -2
	list.trim(start, end)
	resStart = list.index(0)
	resEnd = list.index(end - start)
	if !bytes.Equal(resStart, []byte(strconv.Itoa(length+start))) {
		t.Errorf("trim start failed,result:%s,expected:%d", string(resStart), start)
	}
	if !bytes.Equal(resEnd, []byte(strconv.Itoa(length+end))) {
		t.Errorf("trim end failed,result:%s,expected:%d", string(resEnd), end)
	}

}
