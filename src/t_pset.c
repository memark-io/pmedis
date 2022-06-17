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
    iter_sets[j] = KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
  }

  /* return empty array if the first set is empty */
  if (op == SET_OP_DIFF && NULL == iter_sets[0]) {
    RedisModule_Free((void*)iter_sets);
    for (j = 0; j < setnum; ++j) {
      if (NULL != iter_sets[j]) {
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
  RedisModuleString* dstset_rstr =
      RedisModule_CreateStringFromLongLong(ctx, (long long)(curTime.tv_usec));
  RedisModuleKey* dstset = RedisModule_OpenKey(
      ctx, dstset_rstr, REDISMODULE_READ | REDISMODULE_WRITE);

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
        char* field_str;
        size_t field_len;
        KVDKHashIteratorGetKey(iter, &field_str, &field_len);
        RedisModuleString* field_rstr =
            RedisModule_CreateString(ctx, field_str, field_len);
        int flags = REDISMODULE_ZADD_NX;
        RedisModule_ZsetAdd(dstset, score, field_rstr, &flags);
        RedisModule_FreeString(ctx, field_rstr);
        free(field_str);
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
        char* field_str;
        size_t field_len;
        KVDKHashIteratorGetKey(iter, &field_str, &field_len);
        RedisModuleString* field_rstr =
            RedisModule_CreateString(ctx, field_str, field_len);
        if (0 == j) {
          int flags = REDISMODULE_ZADD_NX;
          int res = RedisModule_ZsetAdd(dstset, score, field_rstr, &flags);
          if ((flags & REDISMODULE_ZADD_ADDED) && (res == REDISMODULE_OK)) {
            ++cardinality;
          }
          ++score;
        } else {
          int deleted;
          if (REDISMODULE_OK ==
              RedisModule_ZsetRem(dstset, field_rstr, &deleted)) {
            --cardinality;
          }
        }
        RedisModule_FreeString(ctx, field_rstr);
        free(field_str);
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
  RedisModule_FreeString(ctx, dstset_rstr);

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
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return pmSunionDiffGenericCommand(ctx, argv + 2, argc - 2, key_str, key_len,
                                    SET_OP_DIFF);
}

int pmSinterGenericCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int setnum, const char* dstkey, size_t dst_len) {
  KVDKHashIterator** iter_sets =
      (KVDKHashIterator**)RedisModule_Alloc(sizeof(KVDKHashIterator*) * setnum);
  size_t key_len;
  KVDKStatus s;
  int j, cardinality = 0;
  int empty = 0;
  int min_pos = 0;
  size_t min_hash_sz = SIZE_MAX;
  RedisModuleKey* dstset;
  const char* key_str = NULL;
  for (j = 0; j < setnum; ++j) {
    key_str = RedisModule_StringPtrLen(argv[j], &key_len);
    iter_sets[j] = KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
    if (NULL == iter_sets[j]) {
      ++empty;
      break;
    }

    size_t hash_sz;
    KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
    assert(s == Ok);
    if (hash_sz < min_hash_sz) {
      min_hash_sz = hash_sz;
      min_pos = j;
    }
  }

  /* Return empty if there is an empty set */
  if (empty > 0) {
    if (dstkey) {
      // TODO: delete key no matter what kind of type
      // TODO: wait for KVDK for type detect type function
      s = KVDKHashDestroy(engine, dstkey, dst_len);
      s = KVDKHashCreate(engine, dstkey, dst_len);
      return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
      return RedisModule_ReplyWithEmptyArray(ctx);
    }
  }

  /* create a temp zset to store the result if dstkey is setted. */
  if (dstkey) {
    // create an temp result zset
    struct timeval curTime;
    gettimeofday(&curTime, NULL);
    RedisModuleString* dstset_rstr =
        RedisModule_CreateStringFromLongLong(ctx, (long long)(curTime.tv_usec));
    dstset = RedisModule_OpenKey(ctx, dstset_rstr,
                                 REDISMODULE_READ | REDISMODULE_WRITE);
    RedisModule_FreeString(ctx, dstset_rstr);
  } else {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }

  /* Iterate all the elements of the first (smallest) set, and test
   * the element against all the other sets, if at least one set does
   * not include the element it is discarded */
  KVDKHashIterator* min_iter = iter_sets[min_pos];
  KVDKHashIteratorSeekToFirst(min_iter);
  bool is_founded = false;
  double score = 0;
  while (KVDKHashIteratorIsValid(min_iter)) {
    char* target_field_str = NULL;
    size_t target_field_len = 0;
    KVDKHashIteratorGetKey(min_iter, &target_field_str, &target_field_len);
    RedisModuleString* target_field_rstr =
        RedisModule_CreateString(ctx, target_field_str, target_field_len);
    // for(int j=0; j<setnum; j++){
    j = 0;
    while (j < setnum) {
      if (j == min_pos) {
        ++j;
        continue;
      }
      /* search key in the set */
      is_founded = false;
      KVDKHashIterator* iter = iter_sets[j];
      KVDKHashIteratorSeekToFirst(iter);
      while (KVDKHashIteratorIsValid(iter)) {
        char* field_str;
        size_t field_len;
        KVDKHashIteratorGetKey(iter, &field_str, &field_len);
        if ((target_field_len == field_len) &&
            (0 == memcmp(field_str, target_field_str, target_field_len))) {
          is_founded = true;
          free(field_str);
          break;
        }
        free(field_str);
        KVDKHashIteratorNext(iter);
      }
      if (false == is_founded) {
        break;
      }
      ++j;
    }

    /* take action when all sets contain the target member*/
    if (j == setnum) {
      ++score;
      if (dstkey) {
        /*
         * insert target member to temp zset if dstkey is not null
         * We can not modify dstkey directly before geting all the result
         * elements since dstkey may involved as a set members Eg:
         * PM.SINTERSTORE dstkey dstkey s1 s2
         */
        int flags = REDISMODULE_ZADD_NX;
        int res = RedisModule_ZsetAdd(dstset, score, target_field_rstr, &flags);
        assert(flags & REDISMODULE_ZADD_ADDED);
        assert(res == REDISMODULE_OK);
      } else {
        RedisModule_ReplyWithString(ctx, target_field_rstr);
      }
    }
    free(target_field_str);
    RedisModule_FreeString(ctx, target_field_rstr);
    KVDKHashIteratorNext(min_iter);
  }
  // KVDKHashIteratorDestroy(min_iter);
  /* destroy iter */
  for (j = 0; j < setnum; ++j) {
    KVDKHashIteratorDestroy(iter_sets[j]);
  }

  /* Output the content */
  if (!dstkey) {
    RedisModule_ReplySetArrayLength(ctx, (long long)score);
  } else {
    /* copy data from temp zset to dstkey */
    // TODO: delete key no matter what kind of type
    // TODO: wait for KVDK for type detect function
    // TODO: implemented by using  hash batch ops
    s = KVDKHashDestroy(engine, dstkey, dst_len);
    s = KVDKHashCreate(engine, dstkey, dst_len);
    long long dst_count = 0;
    RedisModule_ZsetFirstInScoreRange(dstset, REDISMODULE_NEGATIVE_INFINITE,
                                      REDISMODULE_POSITIVE_INFINITE, 0, 0);
    while (!RedisModule_ZsetRangeEndReached(dstset)) {
      RedisModuleString* ele =
          RedisModule_ZsetRangeCurrentElement(dstset, NULL);

      size_t ele_len;
      const char* ele_str = RedisModule_StringPtrLen(ele, &ele_len);
      // TODO: implemented by using hash batch put
      s = KVDKHashPut(engine, dstkey, dst_len, ele_str, ele_len, "0", 1);
      if (s != Ok) {
        return RedisModule_ReplyWithError(
            ctx, "Err when insert elements to target set ");
      }

      RedisModule_FreeString(ctx, ele);
      RedisModule_ZsetRangeNext(dstset);
      ++dst_count;
    }
    RedisModule_ZsetRangeStop(dstset);
    RedisModule_CloseKey(dstset);
    RedisModule_DeleteKey(dstset);
    assert(dst_count == (long long)score);
    return RedisModule_ReplyWithLongLong(ctx, dst_count);
  }
  return REDISMODULE_OK;
}

int pmSinterCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return pmSinterGenericCommand(ctx, argv + 1, argc - 1, NULL, 0);
}

int pmSinterstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return pmSinterGenericCommand(ctx, argv + 2, argc - 2, key_str, key_len);
}

int pmSismemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t target_field_len;
  const char* target_field_str =
      RedisModule_StringPtrLen(argv[2], &target_field_len);

  // TODO:
  // do we need to check the behavior of the below case
  // command: sismember mmmmmmm "3
  // return:  Invalid argument(s)

  KVDKHashIterator* iter =
      KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
  if (NULL == iter) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  };
  KVDKHashIteratorSeekToFirst(iter);
  while (KVDKHashIteratorIsValid(iter)) {
    char* field_str;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field_str, &field_len);
    if ((target_field_len == field_len) &&
        (0 == memcmp(field_str, target_field_str, target_field_len))) {
      KVDKHashIteratorDestroy(iter);
      return RedisModule_ReplyWithLongLong(ctx, 1);
    }
    KVDKHashIteratorNext(iter);
  }
  KVDKHashIteratorDestroy(iter);
  return RedisModule_ReplyWithLongLong(ctx, 0);
}

int pmSmembersCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKHashIterator* iter =
      KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
  if (NULL == iter) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  };
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter)) {
    char* field_str;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field_str, &field_len);
    RedisModule_ReplyWithStringBuffer(ctx, field_str, field_len);
    count++;
    free(field_str);
    KVDKHashIteratorNext(iter);
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  KVDKHashIteratorDestroy(iter);

  return REDISMODULE_OK;
}

int pmSmismemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                        int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIterator* iter =
      KVDKHashIteratorCreate(engine, key_str, key_len, NULL);

  for (int i = 2; i < argc; ++i) {
    if (NULL == iter) {
      RedisModule_ReplyWithLongLong(ctx, 0);
      continue;
    }
    bool isfounded = false;
    KVDKHashIteratorSeekToFirst(iter);
    while (KVDKHashIteratorIsValid(iter)) {
      char* field_str;
      size_t field_len, target_field_len;
      KVDKHashIteratorGetKey(iter, &field_str, &field_len);
      const char* target_field_str =
          RedisModule_StringPtrLen(argv[i], &target_field_len);
      if ((target_field_len == field_len) &&
          (0 == memcmp(field_str, target_field_str, target_field_len))) {
        RedisModule_ReplyWithLongLong(ctx, 1);
        isfounded = true;
        break;
      }
      KVDKHashIteratorNext(iter);
    }
    if (false == isfounded) {
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
  }
  KVDKHashIteratorDestroy(iter);
  RedisModule_ReplySetArrayLength(ctx, argc - 2);
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
  KVDKHashIterator* iter =
      KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
  if (maxCnt > 0) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }
  for (KVDKHashIteratorSeekToFirst(iter); KVDKHashIteratorIsValid(iter);
       KVDKHashIteratorNext(iter)) {
    char* field_str;
    size_t field_len;
    KVDKHashIteratorGetKey(iter, &field_str, &field_len);
    s = KVDKHashDelete(engine, key_str, key_len, field_str, field_len);
    if (s != Ok) {
      KVDKHashIteratorDestroy(iter);
      free(field_str);
      return RedisModule_ReplyWithError(ctx, "ERR KVDKHashDelete");
    }
    RedisModule_ReplyWithStringBuffer(ctx, field_str, field_len);
    free(field_str);
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

/*
 *  TODO: optimizing random function to meet O(1) time complexity for each ops.
 */

int pmSrandmemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }
  long long count = 0;
  int with_count = 0;
  char* field;
  size_t field_len;
  char* val;
  size_t val_len;
  size_t rand_pos;
  if (argc > 2) {
    size_t withvalue_len;
    if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(
          ctx, "ERR value is not an integer or out of range");
    }
    with_count = 1;
  }
  size_t key_len, hash_sz;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if (s != Ok) {
    if (with_count) {
      return RedisModule_ReplyWithEmptyArray(ctx);
    } else {
      return RedisModule_ReplyWithNull(ctx);
    }
  }

  if (!with_count) {
    /* only return 1 random field */
    /* TODO: random strategy can be optimized for single random number */
    rand_pos = (rand()) % hash_sz;
    KVDKHashIterator* iter =
        KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
    KVDKHashIteratorSeekToFirst(iter);
    long long count = 0;

    for (size_t i = 0; i < rand_pos; ++i) {
      assert(KVDKHashIteratorIsValid(iter));
      KVDKHashIteratorNext(iter);
    }
    KVDKHashIteratorGetKey(iter, &field, &field_len);
    RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
    free(field);
    KVDKHashIteratorDestroy(iter);
  } else {
    /* return multiple field */
    /* TODO:  when count !=0, random strategy can be optimized
     * currently a negative count has the same behavior as a positive count.
     */
    if (count >= 0) {
      /* return distinct fields */
      rand_pos = (rand()) % hash_sz;
      if (rand_pos + count > hash_sz) {
        if (count >= hash_sz) {
          count = hash_sz;
          rand_pos = 0;
        } else {
          rand_pos = hash_sz - count;
        }
      }
    } else {
      /* return random fields, same field can be returned multiple times. */
      count = -count;
      if (rand_pos + count > hash_sz) {
        if (count >= hash_sz) {
          count = hash_sz;
          rand_pos = 0;
        } else {
          rand_pos = hash_sz - count;
        }
      }
    }
    // RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithArray(ctx, count);
    KVDKHashIterator* iter =
        KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
    KVDKHashIteratorSeekToFirst(iter);
    for (size_t i = 0; i < rand_pos; ++i) {
      assert(KVDKHashIteratorIsValid(iter));
      KVDKHashIteratorNext(iter);
    }
    for (size_t i = 0; i < count; ++i) {
      assert(KVDKHashIteratorIsValid(iter));
      KVDKHashIteratorGetKey(iter, &field, &field_len);
      RedisModule_ReplyWithStringBuffer(ctx, field, field_len);
      free(field);
      KVDKHashIteratorNext(iter);
    }
    // RedisModule_ReplySetArrayLength(ctx, count);
    KVDKHashIteratorDestroy(iter);
  }
  return REDISMODULE_OK;
}

int SremFunc(char const* old_data, size_t old_len, char** new_data,
             size_t* new_len, void* args) {
  HDelArgs* my_args = (HDelArgs*)args;
  if (old_data == NULL) {
    assert(old_len == 0);
    my_args->ret = 0;
    return KVDK_MODIFY_NOOP;
  } else {
    my_args->ret = 1;
    return KVDK_MODIFY_DELETE;
  }
}

int pmSremCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  KVDKStatus s;
  size_t key_len, deleted = 0;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (int i = 2; i < argc; ++i) {
    size_t field_len;
    const char* field_str = RedisModule_StringPtrLen(argv[i], &field_len);
    HDelArgs args;
    s = KVDKHashModify(engine, key_str, key_len, field_str, field_len, SremFunc,
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

/*
 *  Currently sscan return all the elements
 *  TODO: not support cursor (future work)
 *  TODO: not support match (future work)
 */
int pmSscanCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t key_len, hash_sz;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKStatus s = KVDKHashLength(engine, key_str, key_len, &hash_sz);
  if ((s != Ok) || (hash_sz == 0)) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  KVDKHashIterator* iter =
      KVDKHashIteratorCreate(engine, key_str, key_len, NULL);
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithSimpleString(ctx, "0");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  KVDKHashIteratorSeekToFirst(iter);
  long long count = 0;
  while (KVDKHashIteratorIsValid(iter)) {
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
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  return pmSunionDiffGenericCommand(ctx, argv + 2, argc - 2, key_str, key_len,
                                    SET_OP_UNION);
}