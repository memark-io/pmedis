#include "pmedis.h"

int IncN(const char *old_val, size_t old_val_len, char **new_val,
         size_t *new_val_len, void *args_pointer) {
  // assert(args_pointer);
  IncNArgs *args = (IncNArgs *)args_pointer;
  long long incr = args->ll_incr_by;
  long long l_old_val, l_new_val;
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

  *new_val = (char *)malloc(MAX_LLSTR_SIZE);
  if (*new_val == NULL) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }
  *new_val_len = ll2string(*new_val, MAX_LLSTR_SIZE, l_new_val);
  if (0 == *new_val_len) {
    args->err_no = RMW_MALLOC_ERR;
    return KVDK_MODIFY_ABORT;
  }
  args->ll_result = l_new_val;
  args->err_no = RMW_SUCCESS;
  return KVDK_MODIFY_WRITE;
}

KVDKStatus incrDecr(RedisModuleCtx *ctx, const char *key_str, size_t key_len,
                    IncNArgs *args) {
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  KVDKStatus s =
      KVDKModify(engine, key_str, key_len, IncN, args, free, write_option);
  KVDKDestroyWriteOptions(write_option);
  return s;
}

KVDKStatus RMW_ErrMsgPrinter(RedisModuleCtx *ctx, IncNArgs *args) {
  assert(RMW_SUCCESS != args->err_no);
  switch (args->err_no) {
    case RMW_INVALID_LONGLONG:
      return RedisModule_ReplyWithError(
          ctx, "ERR value is not an integer or out of range");
    case RMW_INVALID_LONGDOUBLE:
      return RedisModule_ReplyWithError(
          ctx, "ERR value is not an float or out of range");
    case RMW_NUMBER_OVERFLOW:
      return RedisModule_ReplyWithError(ctx, "number is overflow");
    case RMW_MALLOC_ERR:
      return RedisModule_ReplyWithError(ctx, "memory allocation err");
    case RMW_ISNAN_OR_INFINITY:
      return RedisModule_ReplyWithError(
          ctx, "increment would produce NaN or Infinity");
    default:
      return RedisModule_ReplyWithError(ctx, "unknown err code");
  }
}

int pmIncrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len;
  if (argc != 2) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  IncNArgs args;
  args.ll_incr_by = 1;
  KVDKStatus s = incrDecr(ctx, key_str, key_len, &args);
  if (s == Abort)
    return RMW_ErrMsgPrinter(ctx, &args);
  else
    return RedisModule_ReplyWithLongLong(ctx, args.ll_result);
}
int pmDecrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len;
  if (argc != 2) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  IncNArgs args;
  args.ll_incr_by = -1;
  KVDKStatus s = incrDecr(ctx, key_str, key_len, &args);
  if (s == Abort)
    return RMW_ErrMsgPrinter(ctx, &args);
  else
    return RedisModule_ReplyWithLongLong(ctx, args.ll_result);
}

int pmIncrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  long long incr;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  if (REDISMODULE_ERR == RedisModule_StringToLongLong(argv[2], &incr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  IncNArgs args;
  args.ll_incr_by = incr;
  KVDKStatus s = incrDecr(ctx, key_str, key_len, &args);
  if (s == Abort)
    return RMW_ErrMsgPrinter(ctx, &args);
  else
    return RedisModule_ReplyWithLongLong(ctx, args.ll_result);
}

int pmDecrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  long long decr;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  if (REDISMODULE_ERR == RedisModule_StringToLongLong(argv[2], &decr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  IncNArgs args;
  args.ll_incr_by = -decr;
  KVDKStatus s = incrDecr(ctx, key_str, key_len, &args);
  if (s == Abort)
    return RMW_ErrMsgPrinter(ctx, &args);
  else
    return RedisModule_ReplyWithLongLong(ctx, args.ll_result);
}

int IncN_ld(const char *old_val, size_t old_val_len, char **new_val,
            size_t *new_val_len, void *args_pointer) {
  // assert(args_pointer);
  IncNArgs *args = (IncNArgs *)args_pointer;
  long double incr = args->ld_incr_by;
  long double ld_old_val, ld_new_val;
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

  *new_val = (char *)malloc(MAX_LONG_DOUBLE_CHARS);
  if (*new_val == NULL) {
    args->err_no = RMW_MALLOC_ERR;
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

int pmIncrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  size_t key_len;
  long double incr, ori_value;
  if (argc != 3) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);

  if (REDISMODULE_ERR == RedisModule_StringToLongDouble(argv[2], &incr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an float or out of range");
  }

  IncNArgs args;
  args.ld_incr_by = incr;

  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  KVDKStatus s =
      KVDKModify(engine, key_str, key_len, IncN_ld, &args, free, write_option);
  KVDKDestroyWriteOptions(write_option);
  if (s == Abort)
    return RMW_ErrMsgPrinter(ctx, &args);
  else
    return RedisModule_ReplyWithLongDouble(ctx, args.ld_result);
}

int pmAppendCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len, append_val_len, ori_val_len;
  char *ori_val_str;
  size_t target_len;
  if (argc != 3) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *append_val_str =
      RedisModule_StringPtrLen(argv[2], &append_val_len);
  char *target_str = NULL;

  KVDKStatus s = KVDKGet(engine, key_str, key_len, &ori_val_len, &ori_val_str);

  if (s != Ok && s != NotFound) {
    /* Something Err in KVDK */
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  } else if (s == NotFound) {
    /* Create the key */
    target_len = append_val_len;
    target_str = (char *)append_val_str;
  } else {
    /* Key exists, check type */
    // TODO: require KVDK support input key, output: key-type
    // TODO: if(type != OBJ_STRING) return RedisModule_ReplyWithError(ctx,"ERR
    // Wrong Type");
    /* Append the value */
    target_len = append_val_len + ori_val_len;
    target_str =
        safeStrcat(ori_val_str, ori_val_len, append_val_str, append_val_len);
  }
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  s = KVDKSet(engine, key_str, key_len, target_str, target_len, write_option);
  free(ori_val_str);  // free memory allocated by KVDKGet
  RedisModule_Free(target_str);
  // free(target_str);

  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, target_len);
}

int pmStrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, val_len, str_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  } else if (s == NotFound) {
    /* Key not exist*/
    str_len = 0;
  } else {
    /* Key exists, check type */
    // TODO: require KVDK support input key, output: key-type
    // TODO: if(type != OBJ_STRING) return RedisModule_ReplyWithError(ctx,"ERR
    // Wrong Type");
    /* Append the value */
    str_len = val_len;
  }
  return RedisModule_ReplyWithLongLong(ctx, str_len);
}

// Wait KVDK Transaction Support
int pmMsetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc, int nx) {
  if (0 == (argc % 2))
    return RedisModule_ReplyWithError(ctx,
                                      "wrong number of arguments for MSET");
  int j;
  size_t key_len, val_len;
  KVDKStatus s;
  char *val_str;
  /* Handle the NX flag. The MSETNX semantic is to return zero and don't
   * set anything if at least one key already exists. */
  if (nx) {
    for (j = 1; j < argc; j += 2) {
      const char *key_str = RedisModule_StringPtrLen(argv[j], &key_len);
      s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
      if (s == Ok) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
      }
    }
  }
  /*
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  for (j = 1; j < argc; j += 2) {
    const char *key_str = RedisModule_StringPtrLen(argv[j], &key_len);
    const char *val_str = RedisModule_StringPtrLen(argv[j + 1], &val_len);
    s = KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
    }
  }
  */
  KVDKWriteBatch *kvdk_wb = KVDKWriteBatchCreate();
  for (j = 1; j < argc; j += 2) {
    const char *key_str = RedisModule_StringPtrLen(argv[j], &key_len);
    const char *val_str = RedisModule_StringPtrLen(argv[j + 1], &val_len);
    KVDKWriteBatchPut(kvdk_wb, key_str, strlen(key_str), val_str,
                      strlen(val_str));
  }
  s = KVDKWrite(engine, kvdk_wb);
  KVDKWriteBatchDestory(kvdk_wb);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int pmMsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return pmMsetGenericCommand(ctx, argv, argc, 0);
}

int pmMsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return pmMsetGenericCommand(ctx, argv, argc, 1);
}

int pmMgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) return RedisModule_WrongArity(ctx);
  size_t key_len, val_len;
  int i;
  RedisModule_ReplyWithArray(ctx, argc - 1);

  for (i = 1; i < argc; ++i) {
    const char *key_str = RedisModule_StringPtrLen(argv[i], &key_len);
    char *val_str;
    KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
    if (s != Ok && s != NotFound) {
      return RedisModule_ReplyWithError(ctx, "MGET KVDKGet Return Err");
    } else if (s == NotFound) {
      RedisModule_ReplyWithStringBuffer(ctx, "(nil)", 5);
    } else {
      RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
      free(val_str);
    }
  }
  return REDISMODULE_OK;
}

int pmGetGenericCommand(RedisModuleCtx *ctx, const char *key_str,
                        size_t key_len, KVDKStatus *s) {
  size_t val_len;
  char *val_str;
  *s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (*s != Ok && *s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[*s]);
  } else if (NotFound == *s) {
    return RedisModule_ReplyWithNull(ctx);
  }
  RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
  free(val_str);
  return REDISMODULE_OK;
}

int pmGetdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus sGet, sDel;
  pmGetGenericCommand(ctx, key_str, key_len, &sGet);
  if (Ok == sGet) {
    sDel = KVDKDelete(engine, key_str, key_len);
    if (sDel != Ok) {
      return RedisModule_ReplyWithError(ctx, enum_to_str[sDel]);
    }
  }
  return REDISMODULE_OK;
}

/*
 * PMGETEX <key> [PERSIST][EX seconds][PX milliseconds][EXAT
 * seconds-timestamp][PXAT milliseconds-timestamp]
 *
 * The getexCommand() function implements extended options and variants of the
 * GET command. Unlike GET command this command is not read-only.
 *
 * The default behavior when no options are specified is same as GET and does
 * not alter any TTL.
 *
 * Only one of the below options can be used at a given time.
 *
 * 1. PERSIST removes any TTL associated with the key.
 * 2. EX Set expiry TTL in seconds.
 * 3. PX Set expiry TTL in milliseconds.
 * 4. EXAT Same like EX instead of specifying the number of seconds representing
 * the TTL (time to live), it takes an absolute Unix timestamp
 * 5. PXAT Same like PX instead of specifying the number of milliseconds
 * representing the TTL (time to live), it takes an absolute Unix timestamp
 *
 * Command would either return the bulk string, error or nil.
 */
// Wait for KVDK TTL
int pmGetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, MSG_WAIT_KVDK_FUNC_SUPPORT);
}
// Wait for KVDK TTL
int pmGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s;
  return pmGetGenericCommand(ctx, key_str, key_len, &s);
}

// Wait for KVDK TTL
int pmSetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  return RedisModule_ReplyWithError(ctx, MSG_WAIT_KVDK_FUNC_SUPPORT);
}

// Wait for KVDK TTL
int pmPsetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  return RedisModule_ReplyWithError(ctx, MSG_WAIT_KVDK_FUNC_SUPPORT);
}

int pmSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  KVDKStatus s =
      KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
  KVDKDestroyWriteOptions(write_option);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int pmSetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len, val_len, ori_val_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  char *ori_val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &ori_val_len, &ori_val_str);
  if (s == Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  s = KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}