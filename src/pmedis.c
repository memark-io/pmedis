/*
 * Copyright 2022 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "pmedis.h"

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (RedisModule_Init(ctx, "pmedis", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  InitKVDK(ctx, argv, argc);
  srand(time(NULL));

  thp = thp_create(8, 8, 100);
  if (thp == NULL) {
    RedisModule_Log(ctx, "warning", "Threadpool creation failed");
    return REDISMODULE_ERR;
  }
  RedisModule_Log(
      ctx, "notice",
      "Threadpool created, threadMin: %d, threadMax: %d, taskMax: %d", 8, 8,
      100);

  if (RedisModule_CreateCommand(ctx, "pm.hello", PmHello_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sleep1m", PmSleep_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "hello.block", HelloBlock_RedisCommand, "",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "hello.keys", HelloKeys_RedisCommand, "",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  // if (RedisModule_CreateCommand(ctx, "pm.setmt", PmSetMT_RedisCommand,
  // "write",
  //                              1, 1, 1) == REDISMODULE_ERR)
  //  return REDISMODULE_ERR;
  // if (RedisModule_CreateCommand(ctx, "pm.getmt", PmGetMT_RedisCommand,
  //                              "readonly", 1, 1, 1) == REDISMODULE_ERR)
  //  return REDISMODULE_ERR;
  // String Commands
  RedisModule_Log(ctx, "notice", "dummy cmd created");
  if (RedisModule_CreateCommand(ctx, "pm.incr", pmIncrCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.decr", pmDecrCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.incrby", pmIncrbyCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.incrbyfloat", pmIncrbyfloatCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.decrby", pmDecrbyCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.append", pmAppendCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.strlen", pmStrlenCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.mget", pmMgetCommand, "readonly fast",
                                1, -1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.mset", pmMsetCommand, "write deny-oom",
                                1, -1, 2) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.msetnx", pmMsetnxCommand,
                                "write deny-oom", 1, -1, 2) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.getrange", pmGetrangeCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.getset", pmGetsetCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.getdel", pmGetdelCommand, "write fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.getex", pmGetexCommand, "write fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.get", pmGetCommand, "readonly fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.set", pmSetCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.setnx", pmSetnxCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.setrange", pmSetrangeCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.setex", pmSetexCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.psetex", pmPsetexCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  RedisModule_Log(ctx, "notice", "string cmd created");
  // List Commands
  if (RedisModule_CreateCommand(ctx, "pm.rpush", pmRpushCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lpush", pmLpushCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.rpushx", pmRpushxCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lpushx", pmLpushxCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.linsert", pmLinsertCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.rpop", pmRpopCommand, "write fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lpop", pmLpopCommand, "write fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  //   if (RedisModule_CreateCommand(ctx, "pm.brpop", pmBrpopCommand,
  //                                 "write deny-script blocking", 1, -2,
  //                                 1) == REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
  //   if (RedisModule_CreateCommand(ctx, "pm.brpoplpush", pmBrpoplpushCommand,
  //                                 "write deny-oom deny-script blocking", 1,
  //                                 2, 1) == REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
  //   if (RedisModule_CreateCommand(ctx, "pm.blmove", pmBlmoveCommand,
  //                                 "write deny-oom deny-script blocking", 1,
  //                                 2, 1) == REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
  //   if (RedisModule_CreateCommand(ctx, "pm.blpop", pmBlpopCommand,
  //                                 "write deny-script blocking", 1, -2,
  //                                 1) == REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.llen", pmLlenCommand, "readonly fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lindex", pmLindexCommand, "readonly",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lset", pmLsetCommand, "write deny-oom",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lrange", pmLrangeCommand, "readonly",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.ltrim", pmLtrimCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lpos", pmLposCommand, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lrem", pmLremCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  //   if (RedisModule_CreateCommand(ctx, "pm.rpoplpush", pmRpoplpushCommand,
  //                                 "write deny-oom", 1, 2, 1) ==
  //                                 REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.lmove", pmLmoveCommand,
                                "write deny-oom", 1, 2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  RedisModule_Log(ctx, "notice", "list cmd created");

  // Hash Commands
  if (RedisModule_CreateCommand(ctx, "pm.hset", pmHsetCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hsetnx", pmHsetnxCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hget", pmHgetCommand, "readonly fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hmset", pmHmsetCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hmget", pmHmgetCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hincrby", pmHincrbyCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hincrbyfloat", pmHincrbyfloatCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hdel", pmHdelCommand, "write fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hlen", pmHlenCommand, "readonly fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hstrlen", pmHstrlenCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hkeys", pmHkeysCommand, "readonly", 1,
                                1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hvals", pmHvalsCommand, "readonly", 1,
                                1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hgetall", pmHgetallCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hexists", pmHexistsCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hrandfield", pmHrandfieldCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.hscan", pmHscanCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  RedisModule_Log(ctx, "notice", "hash cmd created");
  // Set Commands
  if (RedisModule_CreateCommand(ctx, "pm.sadd", pmSaddCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.scard", pmScardCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sdiff", pmSdiffCommand, "readonly", 1,
                                -1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sdiffstore", pmSdiffstoreCommand,
                                "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sinter", pmSinterCommand, "readonly",
                                1, -1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sinterstore", pmSinterstoreCommand,
                                "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sismember", pmSismemberCommand,
                                "readonly fast", 1, 1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.smembers", pmSmembersCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.smismember", pmSmismemberCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.smove", pmSmoveCommand, "write fast",
                                1, 2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.spop", pmSpopCommand, "readonly fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.srandmember", pmSrandmemberCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.srem", pmSremCommand, "write fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sscan", pmSscanCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sunion", pmSunionCommand, "readonly",
                                1, -1, 1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sunionstore", pmSunionstoreCommand,
                                "write deny-oom", 1, -1,
                                1) == REDISMODULE_ERR)  // to-sort
    return REDISMODULE_ERR;
  // ZSet Commands
  if (RedisModule_CreateCommand(ctx, "pm.zadd", pmZaddCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zcard", pmZcardCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zcount", pmZcountCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zdiff", pmZdiffCommand, "readonly", 0,
                                0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zdiffstore", pmZdiffstoreCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zincrby", pmZincrbyCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zinter", pmZinterCommand, "readonly",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zinterstore", pmZinterstoreCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zlexcount", pmZlexcountCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zmscore", pmZmscoreCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zpopmax", pmZpopmaxCommand,
                                "write fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zpopmin", pmZpopminCommand,
                                "write fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrandmember", pmZrandmemberCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrange", pmZrangeCommand, "readonly",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrangebylex", pmZrangebylexCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrangebyscore", pmZrangebyscoreCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrangestore", pmZrangestoreCommand,
                                "write deny-oom", 1, 2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrank", pmZrankCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrem", pmZremCommand, "write fast", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zremrangebylex",
                                pmZremrangebylexCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zremrangebyrank",
                                pmZremrangebyrankCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zremrangebyscore",
                                pmZremrangebyscoreCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrevrange", pmZrevrangeCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrevrangebylex",
                                pmZrevrangebylexCommand, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrevrangebyscore",
                                pmZrevrangebylexCommand, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zrevrank", pmZrevrankCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zscan", pmZscanCommand,
                                "readonly random", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zscore", pmZscoreCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zunion", pmZunionCommand, "readonly",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zunionstore", pmZunionstoreCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
  int ret = thp_destroy(thp);
  if (ret == 0) {
    RedisModule_Log(ctx, "notice", "Threadpool destroyed");
  } else {
    RedisModule_Log(ctx, "warning", "Failed to destory threadpool");
  }
  return REDISMODULE_OK;
}