---
title: README.md
date: 2023-01-12T19:30:35Z
lastmod: 2023-01-12T19:43:34Z
---

# README.md

## Commands

|keys|stirng|list|hash|set|transaction|
| -----------| -------------| ---------| --------------| ------------- | ------------- |
|ping|set|llen|hdel|sadd|multi|
|del|get|lindex|hexists|scard|exec|
|exists|getrange|lpos|hgetall|sdiff|discard|
|expire|setrange|lpop|hincrby|sdiffstore|watch|
|keys|mget|rpop|hincybyfloat|sinter|unwatch|
|persist|mset|lpush|hkeys|sinterstore||
|randomkey|setex|lpushx|hlen|sismember||
|rename|setnx|rpush|hmget|smismember||
|ttl|strlen|rpushx|hset|smembers||
|type|incr|linsert|hget|smove||
||decr|lset|hsetnx|spop||
||decrby|lrem|hstrlen|srandmember||
||incrbyfloat|ltrim|hvals|srem||
||append|lrange||sunion||
|||||sunionstore||

## benchmark

Benchmark result is based on [redis-benchmark](https://redis.io/topics/benchmarks) tool.  
Testing on Xiaoxin Pro13 Laptop with AMD Ryzen 7 4800U @1.80GHz, 16.0 GB RAM, and on windows 11 wsl2 ubuntu 20.04 system And MacBook Pro With M1 Pro

`./redis-benchmark -c 50  -n 200000 -t get`​​

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

```
get:117164.62 requests per second
set: 110987.79 requests per second
incr:110864.74 requests per second
lpush:103519.66 requests per second
rpush:112803.16 requests per second
lpop:111731.84 requests per second
rpop:108283.70 requests per second
sadd:111111.12 requests per second
hset:113700.97 requests per second
spop:123762.38 requests per second


lrange_100 46072.33 requests per second
lrange_300 21822.15 requests per second
lrange_500 18185.12 requests per second
lrange_600 11410.32 requests per second
mset 84961.77 requests per second
```





## Todo

**Transaction Command**​

- [x] `multi`​
- [x] `watch`​
- [x] `exec`​
- [x] `dsicard`​

**refactor the redisClient**

- [x] mstate 


**AOF**
- [] 

**master-slave**
-[] use rpc to implement