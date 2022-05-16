#include "pmedis.h"

int pmRpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (int i = 0; i < (argc - 2); i++) {
    size_t val_len;
    const char *val = RedisModule_StringPtrLen(argv[i + 2], &val_len);
    KVDKStatus s = KVDKListPushBack(engine, key_str, key_len, val, val_len);
    if (s != Ok) {
      RedisModule_ReplyWithError(ctx, "RPUSH error");
      return REDISMODULE_OK;
    }
  }
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx,
                                      "RPUSH cannot get the size of the list");
  }
  RedisModule_ReplyWithLongLong(ctx, list_sz);
  return REDISMODULE_OK;
}

int pmLpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (int i = 0; i < (argc - 2); i++) {
    size_t val_len;
    const char *val = RedisModule_StringPtrLen(argv[i + 2], &val_len);
    KVDKStatus s = KVDKListPushFront(engine, key_str, key_len, val, val_len);
    if (s != Ok) {
      RedisModule_ReplyWithError(ctx, "LPUSH error");
      return REDISMODULE_OK;
    }
  }
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx,
                                      "LPUSH cannot get the size of the list");
  }
  RedisModule_ReplyWithLongLong(ctx, list_sz);
  return REDISMODULE_OK;
}

int pmRpushxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return pmRpushCommand(ctx, argv, argc);
}

int pmLpushxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return pmLpushCommand(ctx, argv, argc);
}

int pmLinsertCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // TODO
  if (argc != 5) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_OK;
}

int pmRpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc != 2) && (argc != 3)) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if ((s != Ok) || (list_sz <= 0)) {
    return RedisModule_ReplyWithNull(ctx);
  }
  if (argc == 2) {
    char *val;
    size_t val_len;
    s = KVDKListPopBack(engine, key_str, key_len, &val, &val_len);
    if (s != Ok) {
      return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
    free(val);
  } else {
    long long count;
    if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
      return RedisModule_ReplyWithNull(ctx);
    }
    if (count <= 0) return RedisModule_ReplyWithNull(ctx);
    if (count > list_sz) count = list_sz;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int i = 0; i < count; i++) {
      char *val;
      size_t val_len;
      s = KVDKListPopBack(engine, key_str, key_len, &val, &val_len);
      if (s != Ok) {
        return RedisModule_ReplyWithNull(ctx);
      }
      RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
      free(val);
    }
    RedisModule_ReplySetArrayLength(ctx, count);
  }
  return REDISMODULE_OK;
}

int pmLpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc != 2) && (argc != 3)) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if ((s != Ok) || (list_sz <= 0)) {
    return RedisModule_ReplyWithNull(ctx);
  }
  if (argc == 2) {
    char *val;
    size_t val_len;
    s = KVDKListPopFront(engine, key_str, key_len, &val, &val_len);
    if (s != Ok) {
      return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
    free(val);
  } else {
    long long count;
    if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
      return RedisModule_ReplyWithNull(ctx);
    }
    if (count <= 0) return RedisModule_ReplyWithNull(ctx);
    if (count > list_sz) count = list_sz;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int i = 0; i < count; i++) {
      char *val;
      size_t val_len;
      s = KVDKListPopBack(engine, key_str, key_len, &val, &val_len);
      if (s != Ok) {
        return RedisModule_ReplyWithNull(ctx);
      }
      RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
      free(val);
    }
    RedisModule_ReplySetArrayLength(ctx, count);
  }
  return REDISMODULE_OK;
}

int pmBrpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, MSG_IMPLEMENT_IN_NEXT_PHASE);
}

int pmBrpoplpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc) {
  return RedisModule_ReplyWithError(ctx, MSG_IMPLEMENT_IN_NEXT_PHASE);
}

int pmBlmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, MSG_IMPLEMENT_IN_NEXT_PHASE);
}

int pmBlpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, MSG_IMPLEMENT_IN_NEXT_PHASE);
}

int pmLlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  RedisModule_ReplyWithLongLong(ctx, list_sz);
  return REDISMODULE_OK;
}

int pmLindexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  long long idx;
  if (RedisModule_StringToLongLong(argv[2], &idx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithNull(ctx);
  }
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if ((s != Ok) || ((idx >= 0) && (idx >= list_sz)) ||
      ((idx < 0) && (list_sz < -idx))) {
    return RedisModule_ReplyWithNull(ctx);
  }
  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  if (idx >= 0)
    KVDKListIteratorSeekPos(iter, (long)idx);
  else
    KVDKListIteratorSeekPos(iter, list_sz - (-idx));
  char *val;
  size_t val_len;
  KVDKListIteratorGetValue(iter, &val, &val_len);
  RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
  free(val);
  KVDKListIteratorDestroy(iter);
  return REDISMODULE_OK;
}

int pmLsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, "ERR no such key");
  }
  long long idx;
  if (RedisModule_StringToLongLong(argv[2], &idx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "ERR index out of range");
  }
  if (((idx >= 0) && (idx >= list_sz)) || ((idx < 0) && (list_sz < -idx))) {
    return RedisModule_ReplyWithError(ctx, "ERR index out of range");
  }
  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  if (idx >= 0)
    KVDKListIteratorSeekPos(iter, (long)idx);
  else
    KVDKListIteratorSeekPos(iter, list_sz - (-idx));
  size_t val_len;
  const char *val = RedisModule_StringPtrLen(argv[3], &val_len);
  s = KVDKListPut(engine, iter, val, val_len);
  if (s == Ok) {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return REDISMODULE_OK;
}

int pmLrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  long long start, end;
  if (RedisModule_StringToLongLong(argv[2], &start) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  if (RedisModule_StringToLongLong(argv[3], &end) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  if (start < 0) {
    start = list_sz + start;
    if (start < 0) start = 0;
  }
  if (end < 0) end = list_sz + end;
  if ((start >= list_sz) || (end < 0) || (start > end))
    return RedisModule_ReplyWithEmptyArray(ctx);
  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  KVDKListIteratorSeekPos(iter, (long)start);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  long i;
  for (i = 0; i <= end - start; i++) {
    if (KVDKListIteratorIsValid(iter)) {
      char *val;
      size_t val_len;
      KVDKListIteratorGetValue(iter, &val, &val_len);
      RedisModule_ReplyWithStringBuffer(ctx, val, val_len);
      free(val);
      KVDKListIteratorNext(iter);
    } else {
      break;
    }
  }
  RedisModule_ReplySetArrayLength(ctx, i);
  KVDKListIteratorDestroy(iter);
  return REDISMODULE_OK;
}

int pmLtrimCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  long long start, end;
  if (RedisModule_StringToLongLong(argv[2], &start) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  if (RedisModule_StringToLongLong(argv[3], &end) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  if (start < 0) {
    start = list_sz + start;
    if (start < 0) start = 0;
  }
  if (end < 0) end = list_sz + end;
  if ((start >= list_sz) || (end < 0) || (start > end)) {
    KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
    if (iter == NULL) {
      return RedisModule_ReplyWithError(ctx,
                                        "ERR KVDK cannot create ListIterator");
    }
    KVDKListIteratorSeekToFirst(iter);
    while (KVDKListIteratorIsValid(iter)) {
      s = KVDKListErase(engine, iter);
      if (s != Ok) {
        KVDKListIteratorDestroy(iter);
        return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
      }
    }
    KVDKListIteratorDestroy(iter);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  if (iter == NULL) {
    return RedisModule_ReplyWithError(ctx,
                                      "ERR KVDK cannot create ListIterator");
  }
  KVDKListIteratorSeekPos(iter, (long)end);
  KVDKListIteratorNext(iter);
  while (KVDKListIteratorIsValid(iter)) {
    s = KVDKListErase(engine, iter);
    if (s != Ok) {
      KVDKListIteratorDestroy(iter);
      return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
    }
  }
  KVDKListIteratorSeekPos(iter, (long)0);
  for (int i = 0; i < start; i++) {
    if (KVDKListIteratorIsValid(iter)) {
      s = KVDKListErase(engine, iter);
      if (s != Ok) {
        KVDKListIteratorDestroy(iter);
        return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
      }
    }
  }
  KVDKListIteratorDestroy(iter);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int pmLposCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithLongLong(ctx, RAND_MAX);
  ;
}

int pmLremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // TODO
  if (argc != 4) return RedisModule_WrongArity(ctx);

  return REDISMODULE_OK;
}

int pmRpoplpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  return REDISMODULE_OK;
}
int pmLmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}