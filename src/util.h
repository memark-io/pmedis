#ifndef __PMEDIS_UTIL_H
#define __PMEDIS_UTIL_H
#include <limits.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#define MSG_WAIT_KVDK_FUNC_SUPPORT \
  "Func not support at this moment, wait for kvdk support!"
#define MSG_IMPLEMENT_IN_NEXT_PHASE \
  "Func not support at this moment, will implement in next phase"

/* Read-modify-write Err code */
typedef enum {
  RMW_SUCCESS,
  RMW_INVALID_LONGLONG,
  RMW_INVALID_LONGDOUBLE,
  RMW_NUMBER_OVERFLOW,
  RMW_MALLOC_ERR,
  RMW_STRING_OVER_MAXSIZE,
  RMW_ISNAN_OR_INFINITY
} rmw_err_msg;

/* Error codes */
#define C_OK 0
#define C_ERR -1

/* KVDK string size */
#define MAX_KVDK_STRING_SIZE 512 * 1024 * 1024

/* Long Long string size */
#define MAX_LLSTR_SIZE 21

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