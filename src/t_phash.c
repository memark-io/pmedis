#include "pmedis.h"

// return value:
int pmHsetGeneral(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t i, key_len, created, field_len, val_len = 0;
  KVDKStatus s;
  if ((argc % 2) == 1) {
    return RedisModule_ReplyWithError(
        ctx, "wrong number of arguments for pm.hset command");
  }
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (i = 2; i < argc; i += 2) {
    const char *field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    const char *val_str = RedisModule_StringPtrLen(argv[i + 1], &val_len);
    s = KVDKHashSet(engine, key_str, key_len, field_str, field_len, val_str,
                    val_len);
    if (s != Ok) {
      return -1;
    }
    ++created;
  }
  return created;
}

// Hash Commands
int pmHsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int res = pmHsetGeneral(ctx, argv, argc);
  if (-1 == res) {
    return RedisModule_ReplyWithError(ctx, "KVDKHashSet ERR");
  }
  return RedisModule_ReplyWithLongLong(ctx, res);
}
int pmHsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHmsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int res = pmHsetGeneral(ctx, argv, argc);
  if (-1 == res) {
    return RedisModule_ReplyWithError(ctx, "KVDKHashSet ERR");
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
int pmHmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHincrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  return REDISMODULE_OK;
}
int pmHdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHstrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHvalsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHexistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
int pmHrandfieldCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc) {
  return REDISMODULE_OK;
}
int pmHscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
