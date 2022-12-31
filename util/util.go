package util

import (
	"hash/fnv"
)

func HashKey(key string) int {
	fnv32 := fnv.New32()
	key = "@#$" + key + "*^%$"
	_, _ = fnv32.Write([]byte(key))
	return int(fnv32.Sum32())
}
