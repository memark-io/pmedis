#include "pmedis.h"

// return value:
int pmHsetGeneral(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t i, key_len, created, field_len, val_len = 0;
  KVDKStatus s;
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
  if ((argc % 2) == 1) {
    return RedisModule_WrongArity(ctx);
  }
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
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char* val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len, &val, &val_len);
  if (s != Ok)
  {
    return RedisModule_ReplyWithNull(ctx);
  }
  RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
  free(val);
  return REDISMODULE_OK;
}
int pmHmsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc % 2) == 1) {
    return RedisModule_WrongArity(ctx);
  }
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
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if (s != Ok)
  {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return RedisModule_ReplyWithLongLong(ctx, hash_sz);
}
int pmHstrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char* val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len, &val, &val_len);
  if (s != Ok)
  {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  RedisModule_ReplyWithLongLong(ctx, val_len);
  free(val);
  return REDISMODULE_OK;
}
int pmHkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0))
  {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator * iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter))
  {
    char* field;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field, &field_len);
    RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
    count++;
    free(field);
    KVDKHashIteratorNext(iter);
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  KVDKHashIteratorDestroy(iter);
  return REDISMODULE_OK;
}
int pmHvalsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0))
  {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator * iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter))
  {
    char* val;
    size_t val_len;
    KVDKHashIteratorGetValue(iter, &val, &val_len);
    RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
    count++;
    free(val);
    KVDKHashIteratorNext(iter);
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  KVDKHashIteratorDestroy(iter);
  return REDISMODULE_OK;
}
int pmHgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0))
  {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator * iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter))
  {
    char* field;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field, &field_len);
    RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
    count++;
    free(field);
    char* val;
    size_t val_len;
    KVDKHashIteratorGetValue(iter, &val, &val_len);
    RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
    count++;
    free(val);
    KVDKHashIteratorNext(iter);
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  KVDKHashIteratorDestroy(iter);
  return REDISMODULE_OK;
}
int pmHexistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char* val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len, &val, &val_len);
  if (s != Ok)
  {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  free(val);
  return RedisModule_ReplyWithLongLong(ctx, 1);
}
int pmHrandfieldCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc) {
  if (argc > 4)
    return RedisModule_WrongArity(ctx);
  long long count = 0;
  int with_value = 0;
  int with_count = 0;
  if (argc > 2)
  {
    size_t withvalue_len;
    if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
    }
    with_count = 1;
    if (argc > 3)
    {
      const char * withvalue_str = RedisModule_StringPtrLen(argv[3], &withvalue_len);
      if ((memcmp(withvalue_str, "WITHVALUES", withvalue_len) == 0) || (memcmp(withvalue_str, "withvalues", withvalue_len) == 0))
      /* Simply use memcpy to check if provided arg is "WITHVALUES" or "withvalues" for now,
       * should specify this when creating command */
      {
        with_value = 1;
      } else
      {
        return RedisModule_ReplyWithError(ctx, "ERR syntax error");
      }
    }
  }
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if (s != Ok)
  {
    if (with_count)
    {
      return RedisModule_ReplyWithEmptyArray(ctx);
    } else
    {
      return RedisModule_ReplyWithNull(ctx);
    }
  }
  if (!with_count) {
    size_t rand_pos = (rand()+rand()) % hash_sz;
    KVDKHashIterator * iter = KVDKHashIteratorCreate(engine, key_str, key_len);
    
  } else
  {

  }
  return REDISMODULE_OK;
}
int pmHscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
