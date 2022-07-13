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

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zpm_range_type;

typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zpm_range_direction;

/* Populate the rangespec according to the objects min and max. */
static int zpmSlParseRange(RedisModuleString** argv, int min, int max, zrangespec *spec) {
  char *eptr;
  spec->minex = spec->maxex = 0;
  size_t min_len, max_len;
  const char* min_str = RedisModule_StringPtrLen(argv[min], &min_len);
  const char* max_str = RedisModule_StringPtrLen(argv[max], &max_len);

  /* Parse the min-max interval. If one of the values is prefixed
   * by the "(" character, it's considered "open". For instance
   * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
   * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
  if (min_str[0] == '(') {
      spec->min = strtod(min_str+1,&eptr);
      if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
      spec->minex = 1;
  } else {
      spec->min = strtod(min_str,&eptr);
      if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
  }

  if (max_str[0] == '(') {
      spec->max = strtod(max_str+1,&eptr);
      if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
      spec->maxex = 1;
  } else {
      spec->max = strtod(max_str,&eptr);
      if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
  }
  return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */
int zpmSlParseLexRangeItem(RedisModuleString** argv, int item_loc, sds *dest, int *ex) {
    size_t item_len;
    const char* item_str = RedisModule_StringPtrLen(argv[item_loc], &item_len);

    switch(item_str[0]) {
    case '+':
        if (item_str[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = maxstring;
        return C_OK;
    case '-':
        if (item_str[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = minstring;
        return C_OK;
    case '(':
        *ex = 1;
        *dest = sdsnewlen(item_str+1,item_len-1);
        return C_OK;
    case '[':
        *ex = 0;
        *dest = sdsnewlen(item_str+1,item_len-1);
        return C_OK;
    default:
        return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (C_OK returned). */
void zpmSlFreeLexRange(zlexrangespec *spec) {
    if (spec->min != minstring &&
        spec->min != maxstring) sdsfree(spec->min);
    if (spec->max != minstring &&
        spec->max != maxstring) sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zpmSlFreeLexRange(),
 * otherwise no release is needed. */
int zpmSlParseLexRange(RedisModuleString** argv, int min, int max, zlexrangespec *spec) {
  
    spec->min = spec->max = NULL;
    if (zpmSlParseLexRangeItem(argv, min, &spec->min, &spec->minex) == C_ERR ||
        zpmSlParseLexRangeItem(argv, max, &spec->max, &spec->maxex) == C_ERR) {
        zpmSlFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}


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
  for (KVDKKVDKSortedIteratorSeekToLast(iter);
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

int pmZrandmemberCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  return REDISMODULE_OK;
}

void cleanup_zrange(zpm_range_type rangetype, zlexrangespec *lexrange){
    if (rangetype == ZRANGE_LEX) {
        zpmSlFreeLexRange(lexrange);
    }
}

/* This command implements ZRANGE, ZREVRANGE. */
KVDKStatus genericPMZrangebyrankCommand(RedisModuleCtx* ctx, const char* key_str, size_t key_len,
    char * dst_str, size_t dst_len, long start, long end, int withscores, int store, int reverse) {

  KVDKStatus s;
  size_t result_len;
  KVDKSortedIterator* iter = KVDKKVDKSortedIteratorCreate(
      engine, key_str, key_len, NULL, &s);
  if (iter == NULL) {
    return s;
  }

  /* Sanitize indexes. */
  size_t zset_len, rangelen;
  s = KVDKSortedSize(engine, key_str, key_len, &zset_len);
  if(s != Ok){
    return s;
  }
  if (start < 0) start = zset_len + start;
  if (end < 0) end = zset_len + end;
  if (start < 0) start = 0;
  
  /* Invariant: start >= 0, so this test will be true when end < 0.
   * The range is empty when start > end or start >= length. */
  if (start > end || start >= zset_len) {
    RedisModule_ReplyWithEmptyArray(ctx);
    return Ok;
  }
  if (end >= zset_len) end = zset_len-1;
  rangelen = (end - start) + 1;
  result_len = rangelen;

  /* for commands except ZRANGESTORE */
  if(store == 0){
    assert((dst_str == NULL));
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }

  size_t skip_step = start;
  if(reverse){
    KVDKKVDKSortedIteratorSeekToLast(iter);
    while(skip_step-- && KVDKSortedIteratorValid(iter)){
      KVDKSortedIteratorPrev(iter);
    }
  }else{
    KVDKSortedIteratorSeekToFirst(iter);
    while(skip_step-- && KVDKSortedIteratorValid(iter)){
      KVDKSortedIteratorNext(iter);
    }
  }

  while(rangelen--){
    if(!KVDKSortedIteratorValid(iter)){
      RedisModule_ReplyWithError(ctx, "ERR Zset iterator");
      return Ok;
    }
    char* score_key;
    size_t score_key_len;
    KVDKSortedIteratorKey(iter, &score_key, &score_key_len);
    char* member;
    char* string_key;
    size_t member_len, string_key_len;
    int64_t score;
    DecodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    if(store != 0){
      assert(dst_str != NULL);
      EncodeStringKey(dst_str, dst_len, member, member_len, &string_key,
                    &string_key_len);
      s = KVDKSortedPut(engine, dst_str, dst_len, score_key, score_key_len, "",
                      0);
      assert(s == Ok);
      KVDKWriteOptions* write_option = KVDKCreateWriteOptions();
      s = KVDKPut(engine, string_key, string_key_len, (char*)&score,
                  sizeof(int64_t), write_option);
      KVDKDestroyWriteOptions(write_option);
    }else{
      RedisModule_ReplyWithStringBuffer(ctx, member, member_len);
      if(withscores){
        RedisModuleString* score_str = RedisModule_CreateStringFromLongLong(ctx, (long long)score);
        RedisModule_ReplyWithString(ctx, score_str);
        RedisModule_FreeString(ctx, score_str);
      }
    }

    free(score_key);
    if(store != 0){
      free(string_key);
    }
    if(reverse){
      KVDKSortedIteratorPrev(iter);
    }else{
      KVDKSortedIteratorNext(iter);
    }
  }
  KVDKSortedIteratorDestroy(engine, iter);

  if(store == 0){
    assert(dst_str == NULL);
    if(withscores){
      RedisModule_ReplySetArrayLength(ctx, result_len * 2);
    }else{
      RedisModule_ReplySetArrayLength(ctx, result_len);
    }
  }else{
    RedisModule_ReplyWithLongLong(ctx, result_len);
  }
  return Ok;

}

/**
 * This function handles PM.ZRANGE and PM.ZRANGESTORE, and also the deprecated
 * PM.Z[REV]RANGE[BYPOS|BYLEX] commands.
 *
 * The simple PM.ZRANGE and PM.ZRANGESTORE can take _AUTO in rangetype and direction,
 * other command pass explicit value.
 *
 * The argc_start points to the src key argument, so following syntax is like:
 * <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
 */
int zPmRangeGenericCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc, int argc_start, int store,
                          zpm_range_type rangetype, zpm_range_direction direction){
  KVDKStatus s;
  zrangespec range;
  zlexrangespec lexrange;
  int minidx = argc_start + 1;
  int maxidx = argc_start + 2;

  /* Options common to all */
  long long opt_start = 0;
  long long opt_end = 0;
  int opt_withscores = 0;
  long long opt_offset = 0;
  long long opt_limit = -1;

  char* dst_str = NULL;
  size_t dst_len = 0;
  /* Detect key type */  
  size_t key_len;
  const char* key_str = RedisModule_StringPtrLen(argv[argc_start], &key_len);
  KVDKValueType type;
  s = KVDKTypeOf(engine, key_str, key_len, &type);
  if(type != SortedSet){
    return RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  if(s == NotFound){
    return RedisModule_ReplyWithEmptyArray(ctx);
  }else if(s != Ok){
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  
  /* Step 1: Skip the <src> <min> <max> args and parse remaining optional arguments. */
  for(int j=argc_start + 3; j < argc; ++j){
    int leftargs = argc-j-1;
    size_t arg_len;
    const char* arg_str = RedisModule_StringPtrLen(argv[j], &arg_len);
    if(!store && !strcasecmp(arg_str, "withscores")){
      opt_withscores = 1;
    } else if(!strcasecmp(arg_str, "limit") && leftargs>=2){
      if((RedisModule_StringToLongLong(argv[j+1], &opt_offset) != REDISMODULE_OK) ||
      (RedisModule_StringToLongLong(argv[j+2], &opt_limit) != REDISMODULE_OK)){
        return RedisModule_ReplyWithError(ctx, "ERR options (numbers)");
      }
      j += 2;
    } else if(direction == ZRANGE_DIRECTION_AUTO && 
              !strcasecmp(arg_str,"rev")){
      direction = ZRANGE_DIRECTION_REVERSE;
    } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(arg_str,"bylex")){
      rangetype = ZRANGE_LEX;
    }else if (rangetype == ZRANGE_AUTO &&
                !strcasecmp(arg_str,"byscore"))
    {
      rangetype = ZRANGE_SCORE;
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR syntax error");
    }
  }

  /* Define defaults if not be set. */
  if (direction == ZRANGE_DIRECTION_AUTO)
      direction = ZRANGE_DIRECTION_FORWARD;
  if (rangetype == ZRANGE_AUTO)
      rangetype = ZRANGE_RANK;

  /* Check for conflicting arguments. */
  if (opt_limit != -1 && rangetype == ZRANGE_RANK) {
      return RedisModule_ReplyWithError(ctx,"syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
  }
  if (opt_withscores && rangetype == ZRANGE_LEX) {
      return RedisModule_ReplyWithError(ctx,"syntax error, WITHSCORES not supported in combination with BYLEX");
  }

  if (direction == ZRANGE_DIRECTION_REVERSE &&
        ((ZRANGE_SCORE == rangetype) || (ZRANGE_LEX == rangetype)))
  {
      /* Range is given as [max,min] */
      int tmp = maxidx;
      maxidx = minidx;
      minidx = tmp;
  }

  /* Step 2: Parse the range. */
  switch (rangetype) {
  case ZRANGE_AUTO:
  case ZRANGE_RANK:
      /* Z[REV]RANGE, ZRANGESTORE [REV]RANGE */
      if((RedisModule_StringToLongLong(argv[minidx], &opt_start) != REDISMODULE_OK) ||
        (RedisModule_StringToLongLong(argv[maxidx], &opt_end) != REDISMODULE_OK))
      {
          return RedisModule_ReplyWithError(ctx, "ERR options for Z[REV]RANGE, ZRANGESTORE [REV]RANGE");
      }
      break;

  case ZRANGE_SCORE:
      /* Z[REV]RANGEBYSCORE, ZRANGESTORE [REV]RANGEBYSCORE */
      if (zpmSlParseRange(argv, minidx, maxidx, &range) != C_OK) {
          return RedisModule_ReplyWithError(ctx, "min or max is not a float");
      }
      break;

  case ZRANGE_LEX:
      /* Z[REV]RANGEBYLEX, ZRANGESTORE [REV]RANGEBYLEX */
      if (zpmSlParseLexRange(argv, minidx, maxidx, &lexrange) != C_OK) {
          return RedisModule_ReplyWithError(ctx, "min or max not valid string range item");
      }
      break;
  }

  /* Step 3: Prepare reply or dst zset. */
  if(store != 0){
    /* check exist */
    const char* c_dst_str = RedisModule_StringPtrLen(argv[argc_start-1], &dst_len);
    dst_str = (char*)c_dst_str;

    /* delete dst_str */
    if (Ok == KVDKTypeOf(engine, dst_str, dst_len, &type)) {
      DeleteKey(ctx, engine, key_str, key_len, type);
    }
    /* create zset of dst_str */
    KVDKSortedCollectionConfigs* s_config = KVDKCreateSortedCollectionConfigs();
    KVDKSetSortedCollectionConfigs(
        s_config, comp_name, strlen(comp_name),
        0 /*we do not need hash index for score part*/);
    s = KVDKSortedCreate(engine, dst_str, dst_len, s_config);
    KVDKDestroySortedCollectionConfigs(s_config);
    if(s != Ok){
      cleanup_zrange(rangetype, &lexrange);
      return RedisModule_ReplyWithError(ctx, "KVDKSortedCreate dst zset Err.");
    }
  }
  
  /* Lookup the key and get range. */
  switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        s = genericPMZrangebyrankCommand(ctx, key_str, key_len, dst_str, dst_len, opt_start, opt_end,
            opt_withscores, store, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_SCORE:
        //s = genericZrangebyscoreCommand(handler, &range, zobj, opt_offset,
        //    opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_LEX:
        //s = genericZrangebylexCommand(handler, &lexrange, zobj, opt_withscores || store,
        //    opt_offset, opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;
  }
  cleanup_zrange(rangetype, &lexrange);
  if(s != Ok){
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return REDISMODULE_OK;
}

// TODO: 
/* PM.ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count] */
int pmZrangeCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }
  return zPmRangeGenericCommand(ctx, argv, argc, 1, 0, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
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
  return zPmRangeGenericCommand(ctx, argv, argc, 2, 1, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
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
  return zPmRangeGenericCommand(ctx, argv, argc, 1, 0, ZRANGE_RANK, ZRANGE_DIRECTION_REVERSE);
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