#ifndef __PMEDIS_UTIL_H
#define __PMEDIS_UTIL_H
#include <limits.h>
#include <stddef.h>
#include <stdint.h>


int string2ll(const char *s, size_t slen, long long *value);
char *safeStrcat(char *__restrict s1, size_t s1_size, const char *__restrict s2,
                 size_t s2_size);
#endif