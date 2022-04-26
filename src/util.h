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

int string2ll(const char *s, size_t slen, long long *value);
int string2ld(const char *s, size_t slen, long double *dp);

char *safeStrcat(char *__restrict s1, size_t s1_size, const char *__restrict s2,
                 size_t s2_size);
#endif