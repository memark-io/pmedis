#ifndef __PMEDIS_UTIL_H
#define __PMEDIS_UTIL_H
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to ld2string */
#define MAX_LONG_DOUBLE_CHARS 5 * 1024

#define MSG_WAIT_KVDK_FUNC_SUPPORT \
  "Func not support at this moment, wait for kvdk support!"
#define MSG_IMPLEMENT_IN_NEXT_PHASE \
  "Func not support at this moment, will implement in next phase"
/* util from redis */
int string2ll(const char* s, size_t slen, long long* value);
int string2ld(const char* s, size_t slen, long double* dp);
int ll2string(char* s, size_t len, long long value);

/* Err code */
#define INVALID_NUMBER -1
#define NUMBER_OVERFLOW -2
#define MALLOC_ERR -3

/* Long Long string size */
#define LLSTR_SIZE 21

char* safeStrcat(char* __restrict s1, size_t s1_size, const char* __restrict s2,
                 size_t s2_size);

extern const char* comp_name;
int StrCompare(const char* a, size_t a_len, const char* b, size_t b_len);
int ScoreCmp(const char* score_key_a, size_t a_len, const char* score_key_b,
             size_t b_len);
void EncodeScoreKey(int64_t score, const char* member, size_t member_len,
                    char** score_key, size_t* score_key_len);
void EncodeStringKey(const char* collection, size_t collection_len,
                     const char* member, size_t member_len, char** string_key,
                     size_t* string_key_len);
void DecodeScoreKey(char* score_key, size_t score_key_len, char** member,
                    size_t* member_len, int64_t* score);
#endif