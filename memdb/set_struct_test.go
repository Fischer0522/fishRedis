package memdb

import (
	"strconv"
	"testing"
)

func TestAddAndGetAll(t *testing.T) {
	set := NewSet()
	set.sAdd("test")
	res := set.sGetAll()
	if len(res) != 1 {
		t.Errorf("sadd smembers failed")
	}
	if res[0] != "test" {
		t.Errorf("sadd smembers failed")
	}
	// use for loop to test
	set = NewSet()
	for i := 0; i < 100; i++ {
		set.sAdd("test" + strconv.Itoa(i))
	}
	res = set.sGetAll()
	if len(res) != 100 {
		t.Errorf("sadd smembers failed")
	}
	// verify each element in result
	resMap := make(map[string]null, 0)
	for i := 0; i < 100; i++ {
		resMap[res[i]] = null{}
	}
	for i := 0; i < 100; i++ {
		_, ok := resMap["test"+strconv.Itoa(i)]
		if !ok {
			t.Errorf("sadd smembers failed")
		}
	}

}
func TestSRandom(t *testing.T) {
	set := NewSet()
	for i := 0; i < 100; i++ {
		set.sAdd("test" + strconv.Itoa(i))
	}
	res := set.sRandom(10)
	if len(res) != 10 {
		t.Errorf("srandmember failed")
	}
	res = set.sRandom(100)
	if len(res) != 100 {
		t.Errorf("srandmember failed")
	}
	res = set.sRandom(200)
	if len(res) != 100 {
		t.Errorf("srandmember failed")
	}
}
func TestSDeleteAndContains(t *testing.T) {
	set := NewSet()
	for i := 0; i < 10; i++ {
		set.sAdd("test" + strconv.Itoa(i))
	}
	res := set.sDelete("test1")
	if res != 1 {
		t.Errorf("srem failed")
	}
	res = set.sDelete("test1")
	if res != 0 {
		t.Errorf("srem failed")
	}
	result := set.sIsContains("test1")
	if result {
		t.Errorf("srem failed")
	}
	res = set.sDelete("test11")
	if res != 0 {
		t.Errorf("srem failed")
	}
}
func TestSDiff(t *testing.T) {
	set1 := NewSet()
	set2 := NewSet()
	for i := 0; i < 10; i++ {
		set1.sAdd("test" + strconv.Itoa(i))
	}
	for i := 5; i < 15; i++ {
		set2.sAdd("test" + strconv.Itoa(i))
	}
	res := set1.sDiff(set2)
	if len(res) != 5 {
		t.Errorf("sdiff failed")
	}
	// verify each element in result
	resMap := make(map[string]null, 0)
	for i := 0; i < 5; i++ {
		resMap["test"+strconv.Itoa(i)] = null{}
	}
	for _, ele := range res.sGetAll() {
		_, ok := resMap[ele]
		if !ok {
			t.Errorf("sdiff failed")
		}
	}
	res = set2.sDiff(set1)
	if len(res) != 5 {
		t.Errorf("sdiff failed")
	}
	set3 := NewSet()
	res = set1.sDiff(set3)
	if len(res) != 10 {
		t.Errorf("sdiff failed")
	}
	res = set3.sDiff(set1)
	if len(res) != 0 {
		t.Errorf("sdiff failed")
	}
}
func TestSInter(t *testing.T) {
	set1 := NewSet()
	set2 := NewSet()
	for i := 0; i < 10; i++ {
		set1.sAdd("test" + strconv.Itoa(i))
	}
	for i := 5; i < 15; i++ {
		set2.sAdd("test" + strconv.Itoa(i))
	}
	res := set1.sInter(set2)
	if len(res) != 5 {
		t.Errorf("sinter failed")
	}
	// verify each element in result
	resMap := make(map[string]null, 0)
	for i := 5; i < 10; i++ {
		resMap["test"+strconv.Itoa(i)] = null{}
	}
	for _, ele := range res.sGetAll() {
		_, ok := resMap[ele]
		if !ok {
			t.Errorf("sinter failed")
		}
	}
	res = set2.sInter(set1)
	if len(res) != 5 {
		t.Errorf("sinter failed")
	}
	set3 := NewSet()
	res = set1.sInter(set3)
	if len(res) != 0 {
		t.Errorf("sinter failed")
	}
	res = set3.sInter(set1)
	if len(res) != 0 {
		t.Errorf("sinter failed")
	}
}
func TestSUnion(t *testing.T) {
	set1 := NewSet()
	set2 := NewSet()
	for i := 0; i < 10; i++ {
		set1.sAdd("test" + strconv.Itoa(i))
	}
	for i := 5; i < 15; i++ {
		set2.sAdd("test" + strconv.Itoa(i))
	}
	res := set1.sUnion(set2)
	if len(res) != 15 {
		t.Errorf("sunion failed")
	}
	// verify each element in result
	resMap := make(map[string]null, 0)
	for i := 0; i < 15; i++ {
		resMap["test"+strconv.Itoa(i)] = null{}
	}
	for _, ele := range res.sGetAll() {
		_, ok := resMap[ele]
		if !ok {
			t.Errorf("sunion failed")
		}
	}
	res = set2.sUnion(set1)
	if len(res) != 15 {
		t.Errorf("sunion failed")
	}
	set3 := NewSet()
	res = set1.sUnion(set3)
	if len(res) != 10 {
		t.Errorf("sunion failed")
	}
	res = set3.sUnion(set1)
	if len(res) != 10 {
		t.Errorf("sunion failed")
	}
}
