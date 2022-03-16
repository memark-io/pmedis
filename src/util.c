#include "util.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "redismodule.h"

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a long long: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. */
int string2ll(const char *s, size_t slen, long long *value) {
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  /* A zero length string is not a valid number. */
  if (plen == slen) return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL) *value = 0;
    return 1;
  }

  /* Handle negative numbers: just set a flag and continue like if it
   * was a positive number. Later convert into negative. */
  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen) return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else {
    return 0;
  }

  /* Parse all the other digits, checking for overflow at every step. */
  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen) return 0;

  /* Convert to negative if needed, and do the final overflow check when
   * converting from unsigned long long to long long. */
  if (negative) {
    if (v > ((unsigned long long)(-(LLONG_MIN + 1)) + 1)) /* Overflow. */
      return 0;
    if (value != NULL) *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL) *value = v;
  }
  return 1;
}

/* Convert a string into a double. Returns 1 if the string could be parsed
 * into a (non-overflowing) double, 0 otherwise. The value will be set to
 * the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a double: no spaces or other characters before or after the string
 * representing the number are accepted. */
int string2ld(const char *s, size_t slen, long double *dp) {
  char buf[MAX_LONG_DOUBLE_CHARS];
  long double value;
  char *eptr;

  if (slen == 0 || slen >= sizeof(buf)) return 0;
  memcpy(buf, s, slen);
  buf[slen] = '\0';

  errno = 0;
  value = strtold(buf, &eptr);
  if (isspace(buf[0]) || eptr[0] != '\0' || (size_t)(eptr - buf) != slen ||
      (errno == ERANGE &&
       (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
      errno == EINVAL || isnan(value))
    return 0;

  if (dp) *dp = value;
  return 1;
}

char *safeStrcat(char *__restrict s1, size_t s1_size, const char *__restrict s2,
                 size_t s2_size) {
  size_t res_len = 1 + s1_size + s2_size;
  char *res = (char *)RedisModule_Alloc(res_len);
  // char *res = (char *)malloc(res_len);
  memset(res, 0, res_len);
  memcpy(res, s1, s1_size);
  memcpy(res + s1_size, s2, s2_size);
  return res;
}
