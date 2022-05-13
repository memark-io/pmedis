#include "pmedis.h"

/*
void PmSet_Task(RedisModuleBlockedClient *bc, RedisModuleString **argv,
                int argc) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
  if (s != Ok) {
    RedisModule_ReplyWithError(ctx, enum_to_str[s]);
    goto err;
  }
  RedisModule_ReplyWithLongLong(ctx, 1);
err:
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_UnblockClient(bc, NULL);
}

void PmGet_Task(RedisModuleBlockedClient *bc, RedisModuleString **argv,
                int argc) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    RedisModule_ReplyWithError(ctx, enum_to_str[s]);
    goto err;
  }
  RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
err:
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_UnblockClient(bc, NULL);
}

int PmSetMT_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModuleBlockedClient *bc =
      RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);

  thp_add(thp, (void *)&PmSet_Task, bc, argv, argc);
  return REDISMODULE_OK;
}

int PmGetMT_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  RedisModuleBlockedClient *bc =
      RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);

  thp_add(thp, (void *)&PmGet_Task, bc, argv, argc);
  return REDISMODULE_OK;
}
*/

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
                                "write deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
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
  //   if (RedisModule_CreateCommand(ctx, "pm.linsert", pmLinsertCommand,
  //                                 "write deny-oom", 1, 1, 1) ==
  //                                 REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
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
  //   if (RedisModule_CreateCommand(ctx, "pm.lmove", pmLmoveCommand,
  //                                 "write deny-oom", 1, 2, 1) ==
  //                                 REDISMODULE_ERR)
  //     return REDISMODULE_ERR;
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
  if (RedisModule_CreateCommand(ctx, "pm.spop", pmSpopCommand, "readonly fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  // ZSet Commands
  if (RedisModule_CreateCommand(ctx, "pm.zadd", pmZaddCommand,
                                "write deny-oom fast", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zpopmin", pmZpopminCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.zpopmax", pmZpopmaxCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
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