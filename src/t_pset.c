/*
 * Copyright 2022 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/time.h>

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

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

int pmSunionDiffGenericCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                               int setnum, const char* dstkey, size_t dst_len,
                               int op) {
  KVDKHashIterator** iter_sets =
      (KVDKHashIterator**)RedisModule_Alloc(sizeof(KVDKHashIterator*) * setnum);
  size_t key_len;
  KVDKStatus s;
  int j, cardinality = 0;
  for (j = 0; j < setnum; ++j) {
    const char* key_str = RedisModule_StringPtrLen(argv[j], &key_len);
    // const char* key_str = RedisModule_StringPtrLen(argv[j+start], &key_len);
    iter_sets[j] = KVDKHashIteratorCreate(engine, key_str, key_len);
  }

  /* return empty array if the first set is empty */
  if (op == SET_OP_DIFF && NULL == iter_sets[0]) {
    RedisModule_Free((void*)iter_sets);
    for(j = 0; j<setnum; ++j){
      if(NULL != iter_sets[j]){
        KVDKHashIteratorDestroy(iter_sets[j]);
      }
    }
    if (dstkey != NULL) {
      KVDKHashDestroy(engine, dstkey, dst_len);
    }
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  // TODO: provide different Algorithm for DIFF to compute what is the best bet
  // with the current input here.

  // create an temp zset
  struct timeval curTime;
  gettimeofday(&curTime, NULL);
  RedisModuleString* dstset_str =
      RedisModule_CreateStringFromLongLong(ctx, (long long)(curTime.tv_usec));
  RedisModuleKey* dstset = RedisModule_OpenKey(
      ctx, dstset_str, REDISMODULE_READ | REDISMODULE_WRITE);
  
  if (op == SET_OP_UNION) {
    /* For union, just add all element of every set to temp dstset*/
    for (j = 0; j < setnum; ++j) {
      if (NULL == iter_sets[j]) {
        continue;
      }
      KVDKHashIterator* iter = iter_sets[j];
      KVDKHashIteratorSeekToFirst(iter);
      double score = 0;
      while (KVDKHashIteratorIsValid(iter)) {
        char* field;
        size_t field_len;
        KVDKHashIteratorGetKey(iter, &field, &field_len);
        RedisModuleString* field_str =
            RedisModule_CreateString(ctx, field, field_len);
        int flags = REDISMODULE_ZADD_NX;
        RedisModule_ZsetAdd(dstset, score, field_str, &flags);
        RedisModule_FreeString(ctx, field_str);
        free(field);
        ++score;
        KVDKHashIteratorNext(iter);
      }
      KVDKHashIteratorDestroy(iter);
    }
  } else if (op == SET_OP_DIFF && iter_sets[0]) {
    /* DIFF algorithm
     * Algorithm is O(N) where N is the total number of elements in all the
     * sets.
     *
     * Method:
     * Add all the elements of the first set to the dstset.
     * Then remove all the elements of all the next sets from it.
     */
    // TODO: provide another Algorithm for DIFF, which performs better
    // performance when the first set is smaller than others.
    for (j = 0; j < setnum; ++j) {
      if (NULL == iter_sets[j]) {
        continue;
      }
      KVDKHashIterator* iter = iter_sets[j];
      KVDKHashIteratorSeekToFirst(iter);
      double score = 0;
      while (KVDKHashIteratorIsValid(iter)) {
        char* field;
        size_t field_len;
        KVDKHashIteratorGetKey(iter, &field, &field_len);
        RedisModuleString* field_str =
            RedisModule_CreateString(ctx, field, field_len);
        if (0 == j) {
          int flags = REDISMODULE_ZADD_NX;
          int res = RedisModule_ZsetAdd(dstset, score, field_str, &flags);
          if ((flags & REDISMODULE_ZADD_ADDED) && (res == REDISMODULE_OK)) {
            ++cardinality;
          }
          ++score;
        } else {
          int deleted;
          if (REDISMODULE_OK ==
              RedisModule_ZsetRem(dstset, field_str, &deleted)) {
            --cardinality;
          }
        }
        RedisModule_FreeString(ctx, field_str);
        free(field);
        /* Exit if result set is empty as any additional removal
         * of elements will have no effect. */
        if (cardinality == 0) break;
        KVDKHashIteratorNext(iter);
      }
      KVDKHashIteratorDestroy(iter);
    }
  }
 
  /* Output the content */
  if (!dstkey) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  } else {
    // TODO: delete key no matter what kind of type
    // TODO: wait for KVDK for type detect function
    s = KVDKHashDestroy(engine, dstkey, dst_len);
    s = KVDKHashCreate(engine, dstkey, dst_len);
  }
  long long dst_count = 0;
  RedisModule_ZsetFirstInScoreRange(dstset, REDISMODULE_NEGATIVE_INFINITE,
                                    REDISMODULE_POSITIVE_INFINITE, 0, 0);
  while (!RedisModule_ZsetRangeEndReached(dstset)) {
    RedisModuleString* ele = RedisModule_ZsetRangeCurrentElement(dstset, NULL);
    if (!dstkey) {
      RedisModule_ReplyWithString(ctx, ele);
      RedisModule_FreeString(ctx, ele);
    } else {
      size_t ele_len;
      const char* ele_str = RedisModule_StringPtrLen(ele, &ele_len);
      s = KVDKHashPut(engine, dstkey, dst_len, ele_str, ele_len, "0", 1);
      if (s != Ok) {
        return RedisModule_ReplyWithError(
            ctx, "Err when insert elements to target set ");
      }
    }
    RedisModule_FreeString(ctx, ele);
    RedisModule_ZsetRangeNext(dstset);
    ++dst_count;
  }
  RedisModule_ZsetRangeStop(dstset);


  RedisModule_CloseKey(dstset);
  RedisModule_DeleteKey(dstset);
  RedisModule_FreeString(ctx, dstset_str);

  if (!dstkey) {
    RedisModule_ReplySetArrayLength(ctx, dst_count);
  } else {
    RedisModule_ReplyWithLongLong(ctx, dst_count);
  }
  return REDISMODULE_OK;
}

int pmSdiffCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return pmSunionDiffGenericCommand(ctx, argv + 1, argc - 1, NULL, 0,
                                    SET_OP_DIFF);
}

int pmSdiffstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                        int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return pmSunionDiffGenericCommand(ctx, argv + 2, argc - 2, key_str, key_len,
                                    SET_OP_DIFF);
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
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return pmSunionDiffGenericCommand(ctx, argv + 1, argc - 1, NULL, 0,
                                    SET_OP_UNION);
}

int pmSunionstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return pmSunionDiffGenericCommand(ctx, argv + 2, argc - 2, key_str, key_len,
                                    SET_OP_UNION);
}