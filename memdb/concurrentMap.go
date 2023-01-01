package memdb

import (
	"fishRedis/util"
	"sync"
)

var (
	DEFAULT_SIZE = 1024
)

const MaxConSize = int(1<<31 - 1)

type shard struct {
	mp   map[string]any
	rwMu *sync.RWMutex
}

type ConcurrentMap struct {
	table []*shard
	size  int
	count int
}

func NewConcurrentMap(size int) *ConcurrentMap {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	m := &ConcurrentMap{
		table: make([]*shard, size),
		size:  size,
		count: 0,
	}
	for i := 0; i < size; i++ {
		m.table[i] = &shard{
			mp:   make(map[string]any),
			rwMu: &sync.RWMutex{},
		}
	}
	return m
}

func (m *ConcurrentMap) GetKeyPos(key string) int {
	hash := util.HashKey(key)
	pos := hash % m.size
	return pos

}

func (m *ConcurrentMap) Set(key string, value any) int {
	pos := m.GetKeyPos(key)
	added := 0
	slot := m.table[pos]
	slot.rwMu.Lock()
	defer slot.rwMu.Unlock()
	_, ok := slot.mp[key]
	if !ok {
		// new key
		m.count++
		added = 1
	}
	slot.mp[key] = value
	return added
}

func (m *ConcurrentMap) SetIfExist(key string, value any) int {
	pos := m.GetKeyPos(key)
	slot := m.table[pos]
	slot.rwMu.Lock()
	defer slot.rwMu.Unlock()
	_, ok := slot.mp[key]
	if ok {
		slot.mp[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) SetIfNotExist(key string, value any) int {
	pos := m.GetKeyPos(key)
	slot := m.table[pos]
	slot.rwMu.Lock()
	defer slot.rwMu.Unlock()
	_, ok := slot.mp[key]
	if !ok {
		m.count++
		slot.mp[key] = value
		return 1
	}
	return 0
}
func (m *ConcurrentMap) Get(key string) (any, bool) {
	pos := m.GetKeyPos(key)
	slot := m.table[pos]
	slot.rwMu.RLock()
	defer slot.rwMu.RUnlock()
	result, ok := slot.mp[key]
	return result, ok
}

func (m *ConcurrentMap) Delete(key string) int {
	pos := m.GetKeyPos(key)
	slot := m.table[pos]
	slot.rwMu.Lock()
	defer slot.rwMu.Unlock()
	_, ok := slot.mp[key]
	if ok {
		delete(slot.mp, key)
		m.count--
		return 1
	}
	return 0
}

func (m *ConcurrentMap) Len() int {
	return m.count
}

func (m *ConcurrentMap) Clear() {
	*m = *NewConcurrentMap(m.size)
}
func (m *ConcurrentMap) Keys() []string {
	keys := make([]string, m.count)
	i := 0
	for _, slot := range m.table {
		slot.rwMu.RLock()
		for key := range slot.mp {
			keys[i] = key
			i++
		}
		slot.rwMu.RUnlock()
	}
	return keys
}
