#include "pmedis.h"

int HSetNXFunc(char const *old_data, size_t old_len, char **new_data,
               size_t *new_len, void *args) {
  HSetArgs *my_args = (HSetArgs *)args;
  if (old_data == NULL) {
    assert(old_len == 0);
    *new_data = (char *)my_args->data;
    *new_len = my_args->len;
    my_args->ret = 1;
    return KVDK_MODIFY_WRITE;
  } else {
    my_args->ret = 0;
    return KVDK_MODIFY_NOOP;
  }
}
int HSetFunc(char const *old_data, size_t old_len, char **new_data,
             size_t *new_len, void *args) {
  HSetArgs *my_args = (HSetArgs *)args;
  *new_data = (char *)my_args->data;
  *new_len = my_args->len;
  if (old_data == NULL) {
    assert(old_len == 0);
    my_args->ret = 1;
  } else {
    my_args->ret = 0;
  }
  return KVDK_MODIFY_WRITE;
}

// Hash Commands
int pmHsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc % 2) == 1) {
    return RedisModule_WrongArity(ctx);
  }
  size_t i, key_len, created = 0, field_len, val_len;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (i = 2; i < argc; i += 2) {
    const char *field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    const char *val = RedisModule_StringPtrLen(argv[i + 1], &val_len);
    HSetArgs args;
    args.data = val;
    args.len = val_len;
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, HSetFunc,
                       &args, NULL);
    if (s == NotFound) {
      s = KVDKHashCreate(engine, key_str, key_len);
      if (s != Ok) {
        return RedisModule_ReplyWithError(ctx, "ERR KVDKHashCreate");
      }
      s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                         HSetFunc, &args, NULL);
    }
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashModify");
    }
    created += args.ret;
  }
  return RedisModule_ReplyWithLongLong(ctx, created);
}
int pmHsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len, field_len, val_len = 0;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  const char *val = RedisModule_StringPtrLen(argv[3], &val_len);
  HSetArgs args;
  args.data = val;
  args.len = val_len;
  s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, HSetNXFunc,
                     &args, NULL);
  if (s == NotFound) {
    s = KVDKHashCreate(engine, key_str, key_len);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashCreate");
    }
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                       HSetNXFunc, &args, NULL);
  }
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, "ERR KVDKHashModify");
  }
  return RedisModule_ReplyWithLongLong(ctx, args.ret);
}
int pmHgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char *val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len,
                             &val, &val_len);
  if (s != Ok) {
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
  size_t i, key_len, field_len, val_len = 0;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (i = 2; i < argc; i += 2) {
    const char *field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    const char *val = RedisModule_StringPtrLen(argv[i + 1], &val_len);
    HSetArgs args;
    args.data = val;
    args.len = val_len;
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, HSetFunc,
                       &args, NULL);
    if (s == NotFound) {
      s = KVDKHashCreate(engine, key_str, key_len);
      if (s != Ok) {
        return RedisModule_ReplyWithError(ctx, "ERR KVDKHashCreate");
      }
      s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                         HSetFunc, &args, NULL);
    }
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashModify");
    }
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
int pmHmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}

int HincrbyFunc(const char *old_val, size_t old_val_len, char **new_val,
                size_t *new_val_len, void *args_pointer) {
  // assert(args_pointer);
  IncNArgs *args = (IncNArgs *)args_pointer;
  long long incr = args->ll_incr_by;
  long long l_old_val, l_new_val;

  *new_val = (char *)malloc(MAX_LLSTR_SIZE);
  if (*new_val == NULL) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }

  if (old_val == NULL) {
    l_old_val = 0;
  } else {
    /* return err if the old value can not convert to a long long number */
    if (0 == string2ll(old_val, old_val_len, &l_old_val)) {
      args->err_no = RMW_INVALID_LONGLONG;
      return KVDK_MODIFY_ABORT;
    }
  }
  /* return err if overflow*/
  if ((incr < 0 && l_old_val < 0 && incr < (LLONG_MIN - l_old_val)) ||
      (incr > 0 && l_old_val > 0 && incr > (LLONG_MAX - l_old_val))) {
    args->err_no = RMW_NUMBER_OVERFLOW;
    return KVDK_MODIFY_ABORT;
  }
  l_new_val = l_old_val + incr;

  *new_val_len = ll2string(*new_val, MAX_LLSTR_SIZE, l_new_val);
  if (0 == *new_val_len) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }
  args->ll_result = l_new_val;
  args->err_no = RMW_SUCCESS;
  return KVDK_MODIFY_WRITE;
}

int pmHincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  KVDKStatus s;
  long long incr;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  if (REDISMODULE_ERR == RedisModule_StringToLongLong(argv[3], &incr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  IncNArgs args;
  args.ll_incr_by = incr;
  s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                     HincrbyFunc, &args, free);
  if (s == NotFound) {
    s = KVDKHashCreate(engine, key_str, key_len);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashCreate");
    }
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                       HincrbyFunc, &args, free);
  }
  if (s != Ok)
    return RMW_ErrMsgPrinter(ctx, args.err_no);
  else
    return RedisModule_ReplyWithLongLong(ctx, args.ll_result);
}

int HincrbyFloatFunc(const char *old_val, size_t old_val_len, char **new_val,
                     size_t *new_val_len, void *args_pointer) {
  // assert(args_pointer);
  IncNArgs *args = (IncNArgs *)args_pointer;
  long double incr = args->ld_incr_by;
  long double ld_old_val, ld_new_val;

  *new_val = (char *)malloc(MAX_LONG_DOUBLE_CHARS);
  if (*new_val == NULL) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }
  if (old_val == NULL) {
    ld_old_val = 0;
  } else {
    /* return err if the old value can not convert to a long long number */
    if (0 == string2ld(old_val, old_val_len, &ld_old_val)) {
      args->err_no = RMW_INVALID_LONGDOUBLE;
      return KVDK_MODIFY_ABORT;
    }
  }
  ld_new_val = ld_old_val + incr;
  if (isnan(ld_new_val) || isinf(ld_new_val)) {
    args->err_no = RMW_ISNAN_OR_INFINITY;
    return KVDK_MODIFY_ABORT;
  }

  *new_val_len =
      ld2string(*new_val, MAX_LONG_DOUBLE_CHARS, ld_new_val, LD_STR_AUTO);
  if (0 == *new_val_len) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }
  args->ld_result = ld_new_val;
  return KVDK_MODIFY_WRITE;
}

int pmHincrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  long double incr, ori_value;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  if (REDISMODULE_ERR == RedisModule_StringToLongDouble(argv[3], &incr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an float or out of range");
  }
  IncNArgs args;
  args.ld_incr_by = incr;

  s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                     HincrbyFloatFunc, &args, free);
  if (s == NotFound) {
    s = KVDKHashCreate(engine, key_str, key_len);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashCreate");
    }
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                       HincrbyFloatFunc, &args, free);
  }
  if (s != Ok)
    return RMW_ErrMsgPrinter(ctx, args.err_no);
  else
    return RedisModule_ReplyWithLongDouble(ctx, args.ld_result);
}

int HDelFunc(char const *old_data, size_t old_len, char **new_data,
             size_t *new_len, void *args) {
  HDelArgs *my_args = (HDelArgs *)args;
  if (old_data == NULL) {
    assert(old_len == 0);
    my_args->ret = 0;
    return KVDK_MODIFY_NOOP;
  } else {
    my_args->ret = 1;
    return KVDK_MODIFY_DELETE;
  }
}

int pmHdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  KVDKStatus s;
  size_t key_len, deleted = 0;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (int i = 2; i < argc; ++i) {
    size_t field_len;
    const char *field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    HDelArgs args;
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, HDelFunc,
                       &args, NULL);
    if (s == NotFound) {
      continue;
    }
    if (s != Ok && s != NotFound) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashModify");
    }
    deleted += args.ret;
  }
  return RedisModule_ReplyWithLongLong(ctx, deleted);
}

int pmHlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return RedisModule_ReplyWithLongLong(ctx, hash_sz);
}
int pmHstrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char *val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len,
                             &val, &val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  RedisModule_ReplyWithLongLong(ctx, val_len);
  free(val);
  return REDISMODULE_OK;
}
int pmHkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0)) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator *iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter)) {
    char *field;
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
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0)) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator *iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter)) {
    char *val;
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
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, hash_sz;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0)) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator *iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter)) {
    char *field;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field, &field_len);
    RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
    count++;
    free(field);
    char *val;
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
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len, field_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *field_str = RedisModule_StringPtrLen(argv[2], &field_len);
  char *val;
  size_t val_len;
  KVDKStatus s = KVDKHashGet(engine, key_str, key_len, field_str, field_len,
                             &val, &val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  free(val);
  return RedisModule_ReplyWithLongLong(ctx, 1);
}
int pmHrandfieldCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc) {
  // if (argc > 4)
  //   return RedisModule_WrongArity(ctx);
  // long long count = 0;
  // int with_value;
  // int with_count = 0;
  // if (argc > 2)
  // {
  //   size_t withvalue_len;
  //   if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
  //     return RedisModule_ReplyWithError(ctx, "ERR value is not an integer or
  //     out of range");
  //   }
  //   with_count = 1;
  //   if (argc > 3)
  //   {
  //     const char * withvalue_str = RedisModule_StringPtrLen(argv[3],
  //     &withvalue_len); if ((memcmp(withvalue_str, "WITHVALUES",
  //     withvalue_len) == 0) || (memcmp(withvalue_str, "withvalues",
  //     withvalue_len) == 0))
  //     /* Simply use memcpy to check if provided arg is "WITHVALUES" or
  //     "withvalues" for now,
  //      * should specify this when creating command */
  //     {
  //       with_value = 1;
  //     } else
  //     {
  //       return RedisModule_ReplyWithError(ctx, "ERR syntax error");
  //     }
  //   }
  // }
  // size_t key_len, hash_sz;
  // const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  // KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  // if (s != Ok)
  // {
  //   if (with_count)
  //   {
  //     return RedisModule_ReplyWithEmptyArray(ctx);
  //   } else
  //   {
  //     return RedisModule_ReplyWithNull(ctx);
  //   }
  // }
  // if (!with_count) {
  //   size_t rand_pos = (rand()+rand()) % hash_sz;
  //   KVDKHashIterator * iter = KVDKHashIteratorCreate(engine, key_str,
  //   key_len);
  // } else
  // {

  // }
  return REDISMODULE_OK;
}
int pmHscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return REDISMODULE_OK;
}
