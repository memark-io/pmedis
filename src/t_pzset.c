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
    s = KVDKSortedSet(engine, key_str, key_len, score_key, score_key_len, "",
                      0);
    if (s == NotFound) {
      KVDKSortedCollectionConfigs* s_config =
          KVDKCreateSortedCollectionConfigs();
      KVDKSetSortedCollectionConfigs(
          s_config, comp_name, strlen(comp_name),
          0 /*we do not need hash index for score part*/);
      s = KVDKCreateSortedCollection(engine, key_str, key_len, s_config);
      if (s == Ok) {
        s = KVDKSortedSet(engine, key_str, key_len, score_key, score_key_len,
                          "", 0);
      }
      KVDKDestroySortedCollectionConfigs(s_config);
    }

    if (s == Ok) {
      KVDKWriteOptions* write_option = KVDKCreateWriteOptions();
      s = KVDKSet(engine, string_key, string_key_len, (char*)&score,
                  sizeof(int64_t), write_option);
      KVDKDestroyWriteOptions(write_option);
      created++;
    }

    free(score_key);
    free(string_key);
  }
  return RedisModule_ReplyWithLongLong(ctx, created);
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
      KVDKKVDKSortedIteratorCreate(engine, key_str, key_len, NULL);
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
  return REDISMODULE_OK;
}