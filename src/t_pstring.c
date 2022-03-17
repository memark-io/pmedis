#include <limits.h>

#include "pmedis.h"
#include "redismodule.h"
#include "util.h"

// Wait: KVDK Read-modify-write
int incrDecr(RedisModuleCtx *ctx, const char *key_str, size_t key_len,
             long long incr) {
  // long long value, oldvalue;
  /* Get long value */
  // TODO: KVDKGetLong
  /* Key exists, check type string */
  // TODO: require KVDK support input key, output: key-type
  // TODO: if(type != OBJ_STRING) return RedisModule_ReplyWithError(ctx,"ERR
  // Wrong Type");
  /* Check whether it is a number */
  // TODO: require kvdk DecodeInt64
  // TODO: if(type != OBJ_INT) return RedisModule_ReplyWithError(ctx,"ERR value
  // is not an integer or out of range");

  /* Read-modify-write */
  /*
  oldvalue = value;
  if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
      (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
      return RedisModule_ReplyWithError(ctx, "increment or decrement would
  overflow");
  }
  value += incr;
  */
  return RedisModule_ReplyWithError(ctx, MSG_WAIT_KVDK_FUNC_SUPPORT);
}

int pmIncrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len;
  if (argc != 2) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return incrDecr(ctx, key_str, key_len, 1);
}
int pmDecrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len;
  if (argc != 2) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return incrDecr(ctx, key_str, key_len, -1);
}

int pmIncrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len;
  long long incr;
  if (argc != 3) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  if (REDISMODULE_ERR == RedisModule_StringToLongLong(argv[2], &incr)) {
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  }
  /*
  size_t incr_str_len;
  const char *incr_str = RedisModule_StringPtrLen(argv[2], &incr_str_len);
  if (0 == string2ll(incr_str, incr_str_len, &incr))
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");
  */
  return incrDecr(ctx, key_str, key_len, incr);
}

int pmDecrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t key_len, incr_str_len;
  long long incr;
  if (argc != 3) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char *incr_str = RedisModule_StringPtrLen(argv[2], &incr_str_len);

  if (0 == string2ll(incr_str, incr_str_len, &incr))
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an integer or out of range");

  return incrDecr(ctx, key_str, key_len, -incr);
}

// Wait: KVDK Read-modify-write
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

  /*
  size_t incr_str_len;
  const char *incr_str = RedisModule_StringPtrLen(argv[2], &incr_str_len);
  if (!string2ld(incr_str, incr_str_len, &incr))
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an float or out of range");
  */
  /* Get String from KVDK */
  /* Check the original value is float or not */
  /*
  if (!string2ld(ori_value_str, ori_value_str_len, &ori_value))
    return RedisModule_ReplyWithError(
        ctx, "ERR value is not an float or out of range");
  */
  (void)key_str;
  (void)ori_value;
  return RedisModule_ReplyWithError(ctx, MSG_WAIT_KVDK_FUNC_SUPPORT);
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
  s = KVDKSet(engine, key_str, key_len, target_str, target_len);
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
int pmMsetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int nx){
  if (0==(argc % 2)) return RedisModule_ReplyWithError(ctx, "wrong number of arguments for MSET");
  int j;
  size_t key_len, val_len;

  /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key already exists. */
  if (nx) {
      for (j = 1; j < argc; j += 2) {
          const char *key_str = RedisModule_StringPtrLen(argv[j], &key_len);
          char *val_str;
          KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
          if (s == Ok) {
            return RedisModule_ReplyWithLongLong(ctx, 0);
          }
      }
  }

  for (j = 1; j < argc; j += 2) {
    const char *key_str = RedisModule_StringPtrLen(argv[j], &key_len);
    const char *val_str = RedisModule_StringPtrLen(argv[j+1], &val_len);
    KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
    }
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);

}

// Wait KVDK Transaction Support
int pmMsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return pmMsetGenericCommand(ctx, argv, argc, 0);
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
      // Warrning: memory leak
      RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
    }
  }
  return REDISMODULE_OK;
}

int pmGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len, val_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  // Warrning: memory leak
  return RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
}

int pmSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
