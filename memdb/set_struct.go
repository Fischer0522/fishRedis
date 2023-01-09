package memdb

type null struct{}

type Set map[string]null

func NewSet() Set {
	return make(map[string]null)
}
func NewCopySet(s Set) Set {
	res := NewSet()
	for _, key := range s.sGetAll() {
		res.sAdd(key)
	}
	return res
}
func (s Set) sAdd(key string) int {
	_, ok := s[key]
	count := 0
	if !ok {
		count++
		s[key] = null{}
	}
	return count
}

func (s Set) sLen() int {
	return len(s)
}

// sRandom handle count < 0 in set.go
func (s Set) sRandom(count int) []string {
	res := make([]string, 0, count)
	for key := range s {
		if count == 0 {
			break
		}
		res = append(res, key)
		count--
	}
	return res
}

func (s Set) sDelete(key string) int {
	_, ok := s[key]
	if !ok {
		return 0
	}
	delete(s, key)
	return 1
}

func (s Set) sIsContains(key string) bool {
	_, ok := s[key]
	return ok
}

// actually can use sRandom and count > len(set)
func (s Set) sGetAll() []string {
	result := make([]string, 0, len(s))
	for key := range s {
		result = append(result, key)
	}
	return result
}

func (s Set) sDiff(sets ...Set) Set {
	resSet := NewSet()
	for key := range s {
		flag := true
		for _, set := range sets {
			if set.sIsContains(key) {
				flag = false
			}
		}
		if flag {
			resSet.sAdd(key)
		}
	}
	return resSet
}

func (s Set) sInter(sets ...Set) Set {
	resSet := NewSet()
	for key := range s {
		flag := true
		for _, set := range sets {
			if !set.sIsContains(key) {
				flag = false
			}
		}
		if flag {
			resSet.sAdd(key)
		}
	}
	return resSet
}

func (s Set) sUnion(sets ...Set) Set {
	res := NewCopySet(s)
	for _, set := range sets {
		for key := range set {
			res.sAdd(key)
		}
	}
	return res
}
