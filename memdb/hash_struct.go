package memdb

import "strconv"

type Hash struct {
	table map[string][]byte
}

func NewHash() *Hash {
	return &Hash{
		table: make(map[string][]byte),
	}
}

func (h *Hash) Set(key string, val []byte) int {
	count := 0
	_, ok := h.table[key]
	if !ok {
		count = 1
	}
	h.table[key] = val
	return count
}

// Get return empty byte slice ,return a nil to redis-cli
func (h *Hash) Get(key string) []byte {
	val, ok := h.table[key]
	if !ok {
		return nil
	}
	return val
}

// Del return 1 or 0 ,1 means delete success 0 means no such key
func (h *Hash) Del(key string) int {
	_, ok := h.table[key]
	if !ok {
		return 0
	}
	delete(h.table, key)
	return 1
}

// KeysAndVals get all key-val in [][]byte
func (h *Hash) KeysAndVals() [][]byte {
	result := make([][]byte, 0)
	for key, val := range h.table {
		result = append(result, []byte(key))
		result = append(result, []byte(val))
	}
	return result
}

// Len return length of HashTable
func (h *Hash) Len() int {
	return len(h.table)
}

func (h *Hash) Strlen(key string) int {
	val, ok := h.table[key]
	if !ok {
		return 0
	}
	return len(val)
}

func (h *Hash) IncrBy(key string, inc int) (int, error) {
	val, ok := h.table[key]
	var intVal int
	if !ok {
		val = []byte(("0"))
	}
	intVal, err := strconv.Atoi(string(val))
	if err != nil {
		return 0, err
	}
	intVal += inc
	h.table[key] = []byte(strconv.Itoa(intVal))
	return intVal, nil
}

func (h *Hash) IncrByFloat(key string, inc float64) (float64, error) {
	val, ok := h.table[key]
	if !ok {
		val = []byte("0")
	}
	floatVal, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return 0, err
	}
	floatVal += inc
	h.table[key] = []byte(strconv.FormatFloat(floatVal, 'f', -1, 64))
	return floatVal, nil
}

// Scan TODO implement HSCAN
// count is just a hint
// need to implement an iterator first
//func (h *Hash) Scan(cursor int, count int) {
//
//}
