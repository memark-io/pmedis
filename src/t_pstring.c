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

/*
 * The parseExtendedStringArguments() function performs the common
 * validation for extended string arguments used in SET and GET command.
 *
 * Get specific commands - PERSIST/DEL
 * Set specific commands - XX/NX/GET
 * Common commands - EX/EXAT/PX/PXAT/KEEPTTL
 *
 * Function takes pointers to client, flags, unit, pointer to pointer of expire
 * obj if needed to be determined and command_type which can be COMMAND_GET or
 * COMMAND_SET.
 *
 * 'flags' changes the behavior of the command (NX, XX or GET, see below).
 * 'expire' represents an expire to set by the user, if 'expire' == LLONG_MAX,
 * the key is the persist key 'unit' indicate the expire unit is second or
 *
 * If there are any syntax violations C_ERR is returned else C_OK is returned.
 *
 * Input flags are updated upon parsing the arguments. Unit and expire are
 * updated if there are any EX/EXAT/PX/PXAT arguments. Unit is updated to
 * millisecond if PX/PXAT is set.
 */
#define OBJ_NO_FLAGS 0
#define OBJ_SET_NX (1 << 0)  /* Set if key not exists. */
#define OBJ_SET_XX (1 << 1)  /* Set if key exists. */
#define OBJ_EX (1 << 2)      /* Set if time in seconds is given */
#define OBJ_PX (1 << 3)      /* Set if time in ms in given */
#define OBJ_KEEPTTL (1 << 4) /* Set and keep the ttl */
#define OBJ_SET_GET (1 << 5) /* Set if want to get key before set */
#define OBJ_EXAT (1 << 6)    /* Set if timestamp in second is given */
#define OBJ_PXAT (1 << 7)    /* Set if timestamp in ms is given */
#define OBJ_PERSIST (1 << 8) /* Set if we need to remove the ttl */

#define COMMAND_GET 0
#define COMMAND_SET 1
int parseExtendedStringArguments(RedisModuleString **argv, int argc, int *flags,
                                 int *unit, long long *expire,
                                 int command_type) {
  long long expire_time = LLONG_MAX;
  int j = command_type == COMMAND_GET ? 2 : 3;
  for (; j < argc; j++) {
    // char *opt = c->argv[j]->ptr;
    size_t opt_len;
    const char *opt = RedisModule_StringPtrLen(argv[j], &opt_len);
    // robj *next = (j == argc-1) ? NULL : c->argv[j+1];

    if ((opt[0] == 'n' || opt[0] == 'N') && (opt[1] == 'x' || opt[1] == 'X') &&
        opt[2] == '\0' && !(*flags & OBJ_SET_XX) && !(*flags & OBJ_SET_GET) &&
        (command_type == COMMAND_SET)) {
      *flags |= OBJ_SET_NX;
    } else if ((opt[0] == 'x' || opt[0] == 'X') &&
               (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
               !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET)) {
      *flags |= OBJ_SET_XX;
    } else if ((opt[0] == 'g' || opt[0] == 'G') &&
               (opt[1] == 'e' || opt[1] == 'E') &&
               (opt[2] == 't' || opt[2] == 'T') && opt[3] == '\0' &&
               !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET)) {
      *flags |= OBJ_SET_GET;
    } else if (!strcasecmp(opt, "KEEPTTL") && !(*flags & OBJ_PERSIST) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               (command_type == COMMAND_SET)) {
      *flags |= OBJ_KEEPTTL;
    } else if (!strcasecmp(opt, "PERSIST") && (command_type == COMMAND_GET) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               !(*flags & OBJ_KEEPTTL)) {
      *flags |= OBJ_PERSIST;
    } else if ((opt[0] == 'e' || opt[0] == 'E') &&
               (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
               !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
               !(*flags & OBJ_EXAT) && !(*flags & OBJ_PX) &&
               !(*flags & OBJ_PXAT) &&
               ((j == argc - 1)
                    ? false
                    : (REDISMODULE_OK == RedisModule_StringToLongLong(
                                             argv[j + 1], &expire_time))))
    // !(*flags & OBJ_PXAT) && next)
    {
      *flags |= OBJ_EX;
      *expire = expire_time;
      j++;
    } else if ((opt[0] == 'p' || opt[0] == 'P') &&
               (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
               !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PXAT) &&
               ((j == argc - 1)
                    ? false
                    : (REDISMODULE_OK == RedisModule_StringToLongLong(
                                             argv[j + 1], &expire_time))))
    // !(*flags & OBJ_PXAT) && next)
    {
      *flags |= OBJ_PX;
      *unit = UNIT_MILLISECONDS;
      *expire = expire_time;
      j++;
    } else if ((opt[0] == 'e' || opt[0] == 'E') &&
               (opt[1] == 'x' || opt[1] == 'X') &&
               (opt[2] == 'a' || opt[2] == 'A') &&
               (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
               !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_PX) &&
               !(*flags & OBJ_PXAT) &&
               ((j == argc - 1)
                    ? false
                    : (REDISMODULE_OK == RedisModule_StringToLongLong(
                                             argv[j + 1], &expire_time))))
    // !(*flags & OBJ_PXAT) && next)
    {
      *flags |= OBJ_EXAT;
      *expire = expire_time;
      j++;
    } else if ((opt[0] == 'p' || opt[0] == 'P') &&
               (opt[1] == 'x' || opt[1] == 'X') &&
               (opt[2] == 'a' || opt[2] == 'A') &&
               (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
               !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) &&
               ((j == argc - 1)
                    ? false
                    : (REDISMODULE_OK == RedisModule_StringToLongLong(
                                             argv[j + 1], &expire_time))))
    // !(*flags & OBJ_PX) && next)
    {
      *flags |= OBJ_PXAT;
      *unit = UNIT_MILLISECONDS;
      *expire = expire_time;
      j++;
    } else {
      // addReplyErrorObject(c,shared.syntaxerr);
      return C_ERR;
    }
  }
  return C_OK;
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
  } else if (sGet != Ok && sGet != NotFound) {
    return REDISMODULE_ERR;
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
int pmGetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int unit = UNIT_SECONDS;
  int flags = OBJ_NO_FLAGS;
  long long milliseconds = LLONG_MAX;
  size_t key_len, ori_val_len;
  char *ori_val_str;
  KVDKStatus s;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  // const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);
  if (parseExtendedStringArguments(argv, argc, &flags, &unit, &milliseconds,
                                   COMMAND_GET) != C_OK) {
    return RedisModule_ReplyWithError(ctx, "PM.GET Option Err!");
  }

  if (flags != OBJ_NO_FLAGS) {
    if (milliseconds != LLONG_MAX) {
      // check expire time overflow
      if (milliseconds <= 0 ||
          (unit == UNIT_SECONDS && milliseconds > LLONG_MAX / 1000)) {
        return RedisModule_ReplyWithError(ctx, "invalid expire time in PM.Set");
      }
      if (unit == UNIT_SECONDS) milliseconds *= 1000;
      // if OBJ_PXAT or OBJ_EXAT is set, we have to convert it to abs time
      if ((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) {
        milliseconds -= mstime();
      }
    }
    if (flags & OBJ_PERSIST) {
      milliseconds = INT64_MAX;
    }
    s = KVDKExpire(engine, key_str, key_len, milliseconds);
    if (s != Ok && s != NotFound) {
      return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
    } else if (NotFound == s) {
      return RedisModule_ReplyWithNull(ctx);
    }
  }
  s = KVDKGet(engine, key_str, key_len, &ori_val_len, &ori_val_str);
  if (s != Ok && s != NotFound) {
    free(ori_val_str);
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  RedisModule_ReplyWithStringBuffer(ctx, ori_val_str, ori_val_len);
  free(ori_val_str);
  return REDISMODULE_OK;
}
// Wait for KVDK TTL
int pmGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s;
  return pmGetGenericCommand(ctx, key_str, key_len, &s);
}

int pmSetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[3], &val_len);
  long long milliseconds = LLONG_MAX;
  if ((REDISMODULE_ERR ==
       RedisModule_StringToLongLong(argv[2], &milliseconds)) ||
      (milliseconds > LLONG_MAX / 1000)) {
    return RedisModule_ReplyWithError(ctx, "invalid expire time");
  }
  milliseconds *= 1000;
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  KVDKWriteOptionsSetTTLTime(write_option, milliseconds);
  KVDKStatus s =
      KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
  KVDKDestroyWriteOptions(write_option);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int pmPsetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[3], &val_len);
  long long milliseconds = LLONG_MAX;
  if (REDISMODULE_ERR == RedisModule_StringToLongLong(argv[2], &milliseconds)) {
    return RedisModule_ReplyWithError(ctx, "invalid expire time");
  }
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  KVDKWriteOptionsSetTTLTime(write_option, milliseconds);
  KVDKStatus s =
      KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
  KVDKDestroyWriteOptions(write_option);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
 *     [EXAT <seconds-timestamp>][PXAT <milliseconds-timestamp>] */
int pmSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  KVDKStatus s;
  int unit = UNIT_SECONDS;
  int flags = OBJ_NO_FLAGS;
  long long milliseconds = LLONG_MAX;
  size_t key_len, val_len, ori_val_len;
  char *ori_val_str;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  if (parseExtendedStringArguments(argv, argc, &flags, &unit, &milliseconds,
                                   COMMAND_SET) != C_OK) {
    return RedisModule_ReplyWithError(ctx, "PM.SET Option Err!");
  }
  KVDKWriteOptions *write_option = KVDKCreateWriteOptions();
  if ((flags & OBJ_EX) || (flags & OBJ_PX) || (flags & OBJ_EXAT) ||
      (flags & OBJ_PXAT)) {
    // check expire time overflow
    if (milliseconds <= 0 ||
        (unit == UNIT_SECONDS && milliseconds > LLONG_MAX / 1000)) {
      return RedisModule_ReplyWithError(ctx, "invalid expire time in PM.Set");
    }
    if (unit == UNIT_SECONDS) milliseconds *= 1000;
    // if OBJ_PXAT or OBJ_EXAT is set, we have to convert it to abs time
    if ((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) {
      milliseconds -= mstime();
    }
    KVDKWriteOptionsSetTTLTime(write_option, milliseconds);
  }

  if ((flags & OBJ_SET_NX) || (flags & OBJ_SET_XX) || (flags & OBJ_SET_GET)) {
    s = KVDKGet(engine, key_str, key_len, &ori_val_len, &ori_val_str);
    // condition 1 NX set, but key exists --> then err
    if (flags & OBJ_SET_NX && s == Ok) {
      free(ori_val_str);
      return RedisModule_ReplyWithError(ctx, "Err! Set NX but key exists");
    }
    // condition 2 XX set, but key not exists --> then err
    if (flags & OBJ_SET_XX && s == NotFound) {
      free(ori_val_str);
      return RedisModule_ReplyWithError(ctx, "Err! Set XX but key not exists");
    }
  }

  s = KVDKSet(engine, key_str, key_len, val_str, val_len, write_option);
  KVDKDestroyWriteOptions(write_option);
  if ((s == Ok) && (flags & OBJ_SET_GET)) {
    RedisModule_ReplyWithStringBuffer(ctx, ori_val_str, ori_val_len);
    free(ori_val_str);
    return REDISMODULE_OK;
  }
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  if ((flags & OBJ_SET_NX) || (flags & OBJ_SET_XX) || (flags & OBJ_SET_GET)) {
    free(ori_val_str);
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