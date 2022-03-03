#include "pmedis.h"

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

int PmGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
}

int PmSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
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

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (RedisModule_Init(ctx, "pmedis", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  InitKVDK(ctx, argv, argc);

  thp = thp_create(4, 32, 100);
  if (thp == NULL) {
    RedisModule_Log(ctx, "warning", "Threadpool creation failed");
    return REDISMODULE_ERR;
  }
  RedisModule_Log(
      ctx, "notice",
      "Threadpool created, threadMin: %d, threadMax: %d, taskMax: %d", 4, 32,
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

  if (RedisModule_CreateCommand(ctx, "pm.set", PmSet_RedisCommand, "write", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.setmt", PmSetMT_RedisCommand, "write",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.get", PmGet_RedisCommand, "readonly",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.getmt", PmGetMT_RedisCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
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