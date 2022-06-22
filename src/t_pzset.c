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

#include "pmedis.h"

int pmZaddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if ((argc % 2) == 1) {
    return RedisModule_WrongArity(ctx);
  }
  size_t i, key_len, created = 0, member_len;
  KVDKStatus s;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  for (i = 2; i < argc; i += 2) {
    const char* member_str = RedisModule_StringPtrLen(argv[i + 1], &member_len);
    char* score_key;
    char* string_key;
    size_t score_key_len;
    size_t string_key_len;
    size_t score_len;
    const char* score_str_with_zero =
        RedisModule_StringPtrLen(argv[i], &score_len);
    int start_non_zero = 0;
    while ((start_non_zero < score_len) &&
           (score_str_with_zero[start_non_zero] == '0'))
      start_non_zero++;
    if (start_non_zero == score_len) {
      if (score_str_with_zero[start_non_zero - 1] == '0')
        start_non_zero--;
      else
        return RedisModule_ReplyWithError(ctx,
                                          "ERR value is not a valid int64");
    }
    char* score_str = malloc(score_len - start_non_zero + 1);
    memcpy(score_str, score_str_with_zero + start_non_zero,
           score_len - start_non_zero);
    score_str[score_len - start_non_zero] = '\0';
    char* endptr;
    char* score_str_end = score_str + score_len - start_non_zero;
    int64_t score = strtoll(score_str, &endptr, 10);
    free(score_str);
    if ((score == 0) && (endptr != score_str_end)) {
      return RedisModule_ReplyWithError(ctx, "ERR value is not a valid int64");
    }
    EncodeScoreKey(score, member_str, member_len, &score_key, &score_key_len);
    EncodeStringKey(key_str, key_len, member_str, member_len, &string_key,
                    &string_key_len);
    s = KVDKSortedPut(engine, key_str, key_len, score_key, score_key_len, "",
                      0);
    if (s == NotFound) {
      KVDKSortedCollectionConfigs* s_config =
          KVDKCreateSortedCollectionConfigs();
      KVDKSetSortedCollectionConfigs(
          s_config, comp_name, strlen(comp_name),
          0 /*we do not need hash index for score part*/);
      s = KVDKSortedCreate(engine, key_str, key_len, s_config);
      if (s == Ok) {
        s = KVDKSortedPut(engine, key_str, key_len, score_key, score_key_len,
                          "", 0);
      }
      KVDKDestroySortedCollectionConfigs(s_config);
    }

    if (s == Ok) {
      KVDKWriteOptions* write_option = KVDKCreateWriteOptions();
      s = KVDKPut(engine, string_key, string_key_len, (char*)&score,
                  sizeof(int64_t), write_option);
      KVDKDestroyWriteOptions(write_option);
      created++;
    }

    free(score_key);
    free(string_key);
  }
  return RedisModule_ReplyWithLongLong(ctx, created);
}

int pmZcardCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZcountCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZdiffCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZdiffstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                        int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZincrbyCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZinterCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZinterstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZlexcountCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZmscoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZpopminCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  long long maxCnt = 1;
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
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  KVDKSortedIterator* iter =
      KVDKKVDKSortedIteratorCreate(engine, key_str, key_len, NULL, NULL);
  if (iter == NULL) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  size_t cnt = 0;
  KVDKStatus s = Ok;
  for (KVDKSortedIteratorSeekToFirst(iter);
       KVDKSortedIteratorValid(iter) && maxCnt > 0;
       KVDKSortedIteratorNext(iter)) {
    char* score_key;
    char* string_key;
    char* member;
    size_t score_key_len;
    size_t string_key_len;
    size_t member_len;
    int64_t score;
    if (cnt == 0)
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    KVDKSortedIteratorKey(iter, &score_key, &score_key_len);
    DecodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    EncodeStringKey(key_str, key_len, member, member_len, &string_key,
                    &string_key_len);
    s = KVDKSortedDelete(engine, key_str, key_len, score_key, score_key_len);
    if (s == Ok) {
      s = KVDKDelete(engine, string_key, string_key_len);
    }

    if (s == Ok) {
      RedisModule_ReplyWithStringBuffer(ctx, member, member_len);
      RedisModuleString* score_str =
          RedisModule_CreateStringFromLongLong(ctx, (long long)score);
      RedisModule_ReplyWithString(ctx, score_str);
      RedisModule_FreeString(ctx, score_str);
      cnt++;
    }

    free(score_key);
    free(string_key);
    if (s != Ok || cnt == maxCnt) {
      break;
    }
  }
  KVDKSortedIteratorDestroy(engine, iter);
  if (cnt == 0) {
    return RedisModule_ReplyWithEmptyArray(ctx);
  }
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, "ERR KVDKSorted");
  } else {
    RedisModule_ReplySetArrayLength(ctx, cnt * 2);
  }
  return REDISMODULE_OK;
}
int pmZpopmaxCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrandmemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrangeCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrangebylexCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrangebyscoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrangestoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrankCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZremCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZremrangebylexCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                            int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZremrangebyrankCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                             int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZremrangebyscoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                              int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrevrangeCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrevrangebylexCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                            int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrevrangebyscoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                              int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZrevrankCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZscanCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZscoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZunionCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

int pmZunionstoreCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}