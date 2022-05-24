#include "pmedis.h"

int pmRpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (int i = 0; i < (argc - 2); i++) {
    size_t val_len;
    const char *val = RedisModule_StringPtrLen(argv[i + 2], &val_len);
    KVDKStatus s = KVDKListPushBack(engine, key_str, key_len, val, val_len);
    if (s == NotFound) {
      s = KVDKListCreate(engine, key_str, key_len);
      if (s != Ok) {
        return RedisModule_ReplyWithError(ctx, "KVDKListCreate ERR");
      }
      s = KVDKListPushBack(engine, key_str, key_len, val, val_len);
    }
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
    if (s == NotFound) {
      s = KVDKListCreate(engine, key_str, key_len);
      if (s != Ok) {
        return RedisModule_ReplyWithError(ctx, "KVDKListCreate ERR");
      }
      s = KVDKListPushFront(engine, key_str, key_len, val, val_len);
    }
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
  if (argc != 5) return RedisModule_WrongArity(ctx);
  size_t key_len, list_sz, insertFrom_len, pivot_len, target_len;
  int where;
  int inserted = 0;
  char *val;
  size_t val_len;
  KVDKStatus s;

  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *insertFrom_str =
      RedisModule_StringPtrLen(argv[2], &insertFrom_len);

  if (strcasecmp(insertFrom_str, "after") == 0) {
    where = LIST_TAIL;
  } else if (strcasecmp(insertFrom_str, "before") == 0) {
    where = LIST_HEAD;
  } else {
    return RedisModule_ReplyWithError(
        ctx, "ERR PM.LINSERT expect BEFORE or AFTER command here.");
  }
  const char *pivot_str = RedisModule_StringPtrLen(argv[3], &pivot_len);
  const char *target_str = RedisModule_StringPtrLen(argv[4], &target_len);

  if (target_len > LIST_MAX_ITEM_SIZE) {
    return RedisModule_ReplyWithError(ctx, "Element too large");
  }

  // add by cc
  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  while (KVDKListIteratorIsValid(iter)) {
    KVDKListIteratorGetValue(iter, &val, &val_len);
    if ((val_len == pivot_len) && (0 == memcmp(pivot_str, val, val_len))) {
      if (where == LIST_TAIL) {
        s = KVDKListInsertAfter(engine, iter, target_str, target_len);
        if (s != Ok) {
          free(val);
          KVDKListIteratorDestroy(iter);
          return RedisModule_ReplyWithError(ctx, "PM.LINSERT insert after ERR");
        }
        inserted = 1;
        break;
      } else if (where == LIST_HEAD) {
        s = KVDKListInsertBefore(engine, iter, target_str, target_len);
        if (s != Ok) {
          free(val);
          KVDKListIteratorDestroy(iter);
          return RedisModule_ReplyWithError(ctx,
                                            "PM.LINSERT insert before ERR");
        }
        inserted = 1;
        break;
      } else {
        free(val);
        KVDKListIteratorDestroy(iter);
        return RedisModule_ReplyWithError(ctx, "PM.LINSERT ERR");
      }
    }
    free(val);
    KVDKListIteratorNext(iter);
  }
  free(val);
  KVDKListIteratorDestroy(iter);
  if (inserted) {
    KVDKStatus s = KVDKListLength(engine, key_str, key_len, &list_sz);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "PM.LINSERT Get list length ERR");
    }
    return RedisModule_ReplyWithLongLong(ctx, list_sz);
  }
  return RedisModule_ReplyWithLongLong(ctx, -1);
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
  size_t key_len, target_len;
  long long count, del_count = 0;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
    return RedisModule_ReplyWithNull(ctx);
  }

  const char *target_str = RedisModule_StringPtrLen(argv[3], &target_len);
  if (target_len > LIST_MAX_ITEM_SIZE) {
    return RedisModule_ReplyWithError(ctx, "Element too large");
  }

  KVDKListIterator *iter = KVDKListIteratorCreate(engine, key_str, key_len);
  if (iter == NULL) {
    return RedisModule_ReplyWithLongLong(ctx, del_count);
  }
  if (count < 0) {
    count = -count;
    for (int i = 0; i < count; ++i) {
      KVDKListIteratorSeekToLastElem(iter, target_str, target_len);
      if (KVDKListIteratorIsValid(iter)) {
        s = KVDKListErase(engine, iter);
        if (s != Ok) {
          KVDKListIteratorDestroy(iter);
          return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
        }
        ++del_count;
      }
    }
  } else if (count > 0) {
    for (int i = 0; i < count; ++i) {
      KVDKListIteratorSeekToFirstElem(iter, target_str, target_len);
      if (KVDKListIteratorIsValid(iter)) {
        s = KVDKListErase(engine, iter);
        if (s != Ok) {
          KVDKListIteratorDestroy(iter);
          return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
        }
        ++del_count;
      }
    }
  } else {
    // remove all matched items
    KVDKListIteratorSeekToFirstElem(iter, target_str, target_len);
    while (KVDKListIteratorIsValid(iter)) {
      s = KVDKListErase(engine, iter);
      if (s != Ok) {
        KVDKListIteratorDestroy(iter);
        return RedisModule_ReplyWithError(ctx, "ERR KVDKListErase failed");
      }
      ++del_count;
      KVDKListIteratorSeekToFirstElem(iter, target_str, target_len);
    }
  }

  return RedisModule_ReplyWithLongLong(ctx, del_count);
}

int pmRpoplpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  return REDISMODULE_OK;
}
int pmLmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}