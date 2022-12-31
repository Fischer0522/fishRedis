package memdb

type MemDb struct {
	db      *ConcurrentMap
	ttlKeys *ConcurrentMap
	locks   *Locks
}
