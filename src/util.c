
#include "util.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "redismodule.h"

char* safeStrcat(char* __restrict s1, size_t s1_size, const char* __restrict s2,
                 size_t s2_size) {
  size_t res_len = 1 + s1_size + s2_size;
  char* res = (char*)RedisModule_Alloc(res_len);
  // char *res = (char *)malloc(res_len);
  memset(res, 0, res_len);
  memcpy(res, s1, s1_size);
  memcpy(res + s1_size, s2, s2_size);
  return res;
}

int StrCompare(const char* a, size_t a_len, const char* b, size_t b_len) {
  int n = (a_len < b_len) ? a_len : b_len;
  int cmp = memcmp(a, b, n);
  if (cmp == 0) {
    if (a_len < b_len)
      cmp = -1;
    else if (a_len > b_len)
      cmp = 1;
  }
  return cmp;
}

int ScoreCmp(const char* score_key_a, size_t a_len, const char* score_key_b,
             size_t b_len) {
  //   assert(a_len >= sizeof(int64_t));
  //   assert(b_len >= sizeof(int64_t));
  int cmp = (*(int64_t*)score_key_a) - (*(int64_t*)score_key_b);
  return cmp == 0 ? StrCompare(
                        score_key_a + sizeof(int64_t), a_len - sizeof(int64_t),
                        score_key_b + sizeof(int64_t), b_len - sizeof(int64_t))
                  : cmp;
}

// Store score key in sorted collection to index score->member
void EncodeScoreKey(int64_t score, const char* member, size_t member_len,
                    char** score_key, size_t* score_key_len) {
  *score_key_len = sizeof(int64_t) + member_len;
  *score_key = (char*)malloc(*score_key_len);
  memcpy(*score_key, &score, sizeof(int64_t));
  memcpy(*score_key + sizeof(int64_t), member, member_len);
}

// Store member with string type to index member->score
void EncodeStringKey(const char* collection, size_t collection_len,
                     const char* member, size_t member_len, char** string_key,
                     size_t* string_key_len) {
  *string_key_len = collection_len + member_len;
  *string_key = (char*)malloc(*string_key_len);
  memcpy(*string_key, collection, collection_len);
  memcpy(*string_key + collection_len, member, member_len);
}

// notice: we set member as a view of score key (which means no ownership)
void DecodeScoreKey(char* score_key, size_t score_key_len, char** member,
                    size_t* member_len, int64_t* score) {
  //   assert(score_key_len > sizeof(int64_t));
  memcpy(score, score_key, sizeof(int64_t));
  *member = score_key + sizeof(int64_t);
  *member_len = score_key_len - sizeof(int64_t);
}