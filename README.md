---
title: README.md
date: 2023-01-12T19:30:35Z
lastmod: 2023-01-12T19:43:34Z
---

# README.md

## Commands

|keys|stirng|list|hash|set|
| -----------| -------------| ---------| --------------| -------------|
|ping|set|llen|hdel|sadd|
|del|get|lindex|hexists|scard|
|exists|getrange|lpos|hgetall|sdiff|
|expire|setrange|lpop|hincrby|sdiffstore|
|keys|mget|rpop|hincybyfloat|sinter|
|persist|mset|lpush|hkeys|sinterstore|
|randomkey|setex|lpushx|hlen|sismember|
|rename|setnx|rpush|hmget|smismember|
|ttl|strlen|rpushx|hset|smembers|
|type|incr|linsert|hget|smove|
||decr|lset|hsetnx|spop|
||decrby|lrem|hstrlen|srandmember|
||incrbyfloat|ltrim|hvals|srem|
||append|lrange||sunion|
|||||sunionstore|

## benchmark

Benchmark result is based on [redis-benchmark](https://redis.io/topics/benchmarks) tool.  
Testing on ThinkBook Laptop with AMD Ryzen 7 4800U @1.80GHz, 16.0 GB RAM, and on windows 11 wsl2 ubuntu 20.04 system.

​`./redis-benchmark -c 50  -n 200000 -t get`​​

```
get:95192.77 requests per second
set: 92678.41 requests per second
incr:94384.15 requests per second
lpush:93240.09 requests per second
rpush:93632.96 requests per second
lpop:92936.80 requests per second
rpop:92678.41 requests per second
sadd:92592.59 requests per second
hset:88300.22 requests per second
spop:92336.11 requests per second


lrange_100 24140.01 requests per second
lrange_300 10260.09 requests per second
lrange_500 7857.93 requests per second
lrange_600 4680.44 requests per second
mset 64787.82 requests per second
```

## Todo

**Transaction Command**​

* ​`multi`​
* ​`watch`​
* ​`exec`​
* ​`dsicard`​

**refactor the redisClient**
