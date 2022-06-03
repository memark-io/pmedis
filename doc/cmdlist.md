# Command List

All the commands supported by PMedis are listed in the table below. Most of them have the same behavior as the vanilla Redis. For those that are slightly different to the vanilla one, the difference are highlighted in the description.

| Command | Description | Known Issues |
| :- | :- | :- |
| PM.	APPEND	|	same as redis APPEND|
| PM.	DECR	|	same as redis DECR|
| PM.	DECRBY	|	same as redis DECRBY|
| PM.	GET	|	same as redis GET|
| PM.	GETDEL	|	same as redis GETDEL| different behavior in case:PM.SET foo bar; PM.GETDEL foo; PM.LPUSH foo bar; expected: OK
| PM.	GETEX	|	same as redis GETEX| bugs found in case: PM.GETEX(no arguments) expected: wrong number of arguments
| PM.	GETRANGE	|	same as redis GETRANGE|
| PM.	GETSET	|	same as redis GETSET|
| PM.	INCR	|	same as redis INCR|
| PM.	INCRBY	|	same as redis INCRBY|
| PM.	INCRBYFLOAT	|	same as redis INCRBYFLOAT|
| PM.	MGET	|	same as redis MGET| different behavior in case: PM.SET foo bar; PM.SADD myset a; PM.SADD myset b; PM.MGET foo myset; expected: bar (nil);
| PM.	MSET	|	same as redis MSET|
| PM.	MSETNX	|	same as redis MSETNX|
| PM.	PSETEX	|	same as redis PSETEX|
| PM.	SET	|	same as redis SET| 1.the behavior of enable NX/XX option is different from the default redis SET; 2.not report "wrong type error" when set a different type of key
| PM.	SETEX	|	same as redis SETEX|
| PM.	SETNX	|	same as redis SETNX|
| PM.	SETRANGE	|	same as redis SETRANGE| different behavior in case: PM.SETRANGE mykey 0 ""
| PM.	STRLEN	|	same as redis STRLEN|
| PM.	LINDEX	|	same as redis LINDEX| different behavior in case when executing on a wrong type data.
| PM.	LINSERT	|	same as redis LINSERT| 
| PM.	LLEN	|	same as redis LLEN| different behavior in case when executing on a wrong type data.
| PM. LPOP  | same as redis LPOP| different behavior in case: PM.LPOP listcount -123; expected: ERR value is out of range, must be positive
| PM.	LPUSH	|	same as redis LPUSH|
| PM.	LPUSHX	|	same as redis LPUSHX|
| PM.	LRANGE	|	same as redis LRANGE|
| PM.	LREM	|	same as redis LREM|
| PM.	LSET	|	same as redis LSET|
| PM.	LTRIM	|	same as redis LTRIM|
| PM.	LPOS	|	same as redis LPOS|
| PM.	LMOVE	|	same as redis LMOVE| current implementation do not support update atomically
| PM.	RPOP	|	same as redis RPOP| different behavior in case: PM.RPOP listcount -123; expected: ERR value is out of range, must be positive
| PM.	RPUSH	|	same as redis RPUSH|
| PM.	RPUSHX	|	same as redis RPUSHX|
| PM.	HDEL	|	same as redis HDEL|
| PM.	HEXISTS	|	same as redis HEXISTS|
| PM.	HGET	|	same as redis HGET|
| PM.	HGETALL	|	same as redis HGETALL|
| PM.	HINCRBY	|	same as redis HINCRBY|
| PM.	HINCRBYFLOAT	|	same as redis HINCRBYFLOAT|
| PM.	HKEYS	|	same as redis HKEYS|
| PM.	HLEN	|	same as redis HLEN|
| PM.	HMGET	|	same as redis HMGET| different behavior in case when executing on a wrong type data.
| PM.	HMSET	|	same as redis HMSET|
| PM.	HRANDFIELD	|	same as redis HRANDFIELD| 1. a negative count has the same behavior as a positive count; 2. random strategy can be further optimized; 3.do not support redis RESP3 protocal
| PM.	HSCAN	|	same as redis HSCAN| 1. not support cursor; 2. not support MATCH option
| PM.	HSET	|	same as redis HSET|
| PM.	HSETNX	|	same as redis HSETNX|
| PM.	HSTRLEN	|	same as redis HSTRLEN|
| PM.	HVALS	|	same as redis HVALS|
| PM.	SADD	|	same as redis SADD|
| PM.	SCARD	|	same as redis SCARD|
| PM.	SDIFF	|	same as redis SDIFF| in progress
| PM.	SDIFFSTORE	|	same as redis SDIFFSTORE| in progress
| PM.	SINTER	|	same as redis SINTER| in progress
| PM.	SINTERSTORE	|	same as redis SINTERSTORE| in progress
| PM.	SISMEMBER	|	same as redis SISMEMBER| in progress
| PM.	SMEMBERS	|	same as redis SMEMBERS| in progress
| PM.	SMISMEMBER	|	same as redis SMISMEMBER| in progress
| PM.	SMOVE	|	same as redis SMOVE| in progress
| PM.	SPOP	|	same as redis SPOP| in progress
| PM.	SRANDMEMBER	|	same as redis SRANDMEMBER| in progress
| PM.	SREM	|	same as redis SREM| in progress
| PM.	SSCAN	|	same as redis SSCAN| in progress
| PM.	SUNION	|	same as redis SUNION| in progress
| PM.	SUNIONSTORE	|	same as redis SUNIONSTORE| in progress
| PM.	ZADD	|	same as redis ZADD| 
| PM.	ZCARD	|	same as redis ZCARD| in progress
| PM.	ZCOUNT	|	same as redis ZCOUNT| in progress
| PM.	ZDIFF	|	same as redis ZDIFF| in progress
| PM.	ZDIFFSTORE	|	same as redis ZDIFFSTORE| in progress
| PM.	ZINCRBY	|	same as redis ZINCRBY| in progress
| PM.	ZINTER	|	same as redis ZINTER| in progress
| PM.	ZINTERCARD	|	same as redis ZINTERCARD| in progress
| PM.	ZINTERSTORE	|	same as redis ZINTERSTORE| in progress
| PM.	ZLEXCOUNT	|	same as redis ZLEXCOUNT| in progress
| PM.	ZMPOP	|	same as redis ZMPOP| in progress
| PM.	ZMSCORE	|	same as redis ZMSCORE| in progress
| PM.	ZPOPMAX	|	same as redis ZPOPMAX| in progress
| PM.	ZPOPMIN	|	same as redis ZPOPMIN|
| PM.	ZRANDMEMBER	|	same as redis ZRANDMEMBER| in progress
| PM.	ZRANGE	|	same as redis ZRANGE| in progress
| PM.	ZRANGEBYLEX	|	same as redis ZRANGEBYLEX| in progress
| PM.	ZRANGEBYSCORE	|	same as redis ZRANGEBYSCORE| in progress
| PM.	ZRANGESTORE	|	same as redis ZRANGESTORE| in progress
| PM.	ZRANK	|	same as redis ZRANK| in progress
| PM.	ZREM	|	same as redis ZREM| in progress
| PM.	ZREMRANGEBYLEX	|	same as redis ZREMRANGEBYLEX| in progress
| PM.	ZREMRANGEBYRANK	|	same as redis ZREMRANGEBYRANK| in progress
| PM.	ZREMRANGEBYSCORE	|	same as redis ZREMRANGEBYSCORE| in progress
| PM.	ZREVRANGE	|	same as redis ZREVRANGE| in progress
| PM.	ZREVRANGEBYLEX	|	same as redis ZREVRANGEBYLEX| in progress
| PM.	ZREVRANGEBYSCORE	|	same as redis ZREVRANGEBYSCORE| in progress
| PM.	ZREVRANK	|	same as redis ZREVRANK| in progress
| PM.	ZSCAN	|	same as redis ZSCAN| in progress
| PM.	ZSCORE	|	same as redis ZSCORE| in progress
| PM.	ZUNION	|	same as redis ZUNION| in progress
| PM.	ZUNIONSTORE	|	same as redis ZUNIONSTORE| in progress

If you find any issues and bugs of these commands, please feel free to report it by raising a github issue, we will look into it as soon as possible.
