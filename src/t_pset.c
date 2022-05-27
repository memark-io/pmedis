#include "pmedis.h"

int SaddFunc(char const* old_data, size_t old_len, char** new_data,
             size_t* new_len, void* args) {
  HSetArgs* my_args = (HSetArgs*)args;
  if (old_data == NULL) {
    assert(old_len == 0);
    *new_data = (char*)my_args->data;
    *new_len = my_args->len;
    my_args->ret = 1;
    return KVDK_MODIFY_WRITE;
  } else {
    my_args->ret = 0;
    return KVDK_MODIFY_NOOP;
  }
}

int pmSaddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len, field_len, created = 0;
  KVDKStatus s;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  const char* field_str;
  HSetArgs args;
  args.data = "0";
  args.len = strlen(args.data);
  for (int i = 2; i < argc; i++) {
    field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, SaddFunc,
                       &args, NULL);
    if (s == NotFound) {
      s = KVDKHashCreate(engine, key_str, key_len);
      if (s != Ok) {
        return RedisModule_ReplyWithError(ctx, "ERR PM.SADD create err");
      }
      s = KVDKHashModify(engine, key_str, key_len, field_str, field_len,
                         SaddFunc, &args, NULL);
    }
    if (s != Ok) {
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashModify");
    }
    created += args.ret;
  }
  return RedisModule_ReplyWithLongLong(ctx, created);
}

int pmScardCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len, hash_sz;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if (s != Ok) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return RedisModule_ReplyWithLongLong(ctx, hash_sz);
}

int pmSdiffCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  // if (argc !=2) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSdiffstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                        int argc) {
  // if (argc !=2) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSinterCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  // if (argc !=2) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSinterstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  // if (argc !=2) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSismemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmSmembersCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmSmismemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                        int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSmoveCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmSpopCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  long long maxCnt = 0;
  if ((argc != 3) && (argc != 2)) {
    return RedisModule_WrongArity(ctx);
  }
  if (argc == 3) {
    if ((RedisModule_StringToLongLong(argv[2], &maxCnt) != REDISMODULE_OK) ||
        (maxCnt < 0)) {
      return RedisModule_ReplyWithError(
          ctx, "ERR value is out of range, must be positive");
    }
    if (maxCnt == 0) return RedisModule_ReplyWithEmptyArray(ctx);
  }
  size_t key_len, hash_sz;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0)) {
    if (maxCnt == 0)
      return RedisModule_ReplyWithNull(ctx);
    else
      return RedisModule_ReplyWithEmptyArray(ctx);
  }
  size_t cnt = 0;
  KVDKHashIterator* iter = KVDKHashIteratorCreate(engine, key_str, key_len);
  if (maxCnt > 0) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }
  for (KVDKHashIteratorSeekToFirst(iter); KVDKHashIteratorIsValid(iter);
       KVDKHashIteratorNext(iter)) {
    char* field;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field, &field_len);
    s = KVDKHashDelete(engine, key_str, key_len, field, field_len);
    if (s != Ok) {
      KVDKHashIteratorDestroy(iter);
      free(field);
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashDelete");
    }
    RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
    free(field);
    cnt++;
    if (maxCnt == 0) {
      KVDKHashIteratorDestroy(iter);
      return REDISMODULE_OK;
    }
    if (cnt == maxCnt) {
      break;
    }
  }
  RedisModule_ReplySetArrayLength(ctx, cnt);
  KVDKHashIteratorDestroy(iter);
  return REDISMODULE_OK;
}

int pmSrandmemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSremCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSscanCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSunionCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}

int pmSunionstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  // if (argc !=3) {
  //   return RedisModule_WrongArity(ctx);
  // }
  return REDISMODULE_OK;
}