/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
//#include "util.h"
#include "util_redis.h"

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
int ll2string(char* dst, size_t dstlen, long long svalue) {
  static const char digits[201] =
      "0001020304050607080910111213141516171819"
      "2021222324252627282930313233343536373839"
      "4041424344454647484950515253545556575859"
      "6061626364656667686970717273747576777879"
      "8081828384858687888990919293949596979899";
  int negative;
  unsigned long long value;

  /* The main loop works with 64bit unsigned integers for simplicity, so
   * we convert the number here and remember if it is negative. */
  if (svalue < 0) {
    if (svalue != LLONG_MIN) {
      value = -svalue;
    } else {
      value = ((unsigned long long)LLONG_MAX) + 1;
    }
    negative = 1;
  } else {
    value = svalue;
    negative = 0;
  }

  /* Check length. */
  uint32_t const length = digits10(value) + negative;
  if (length >= dstlen) return 0;

  /* Null term. */
  uint32_t next = length;
  dst[next] = '\0';
  next--;
  while (value >= 100) {
    int const i = (value % 100) * 2;
    value /= 100;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
    next -= 2;
  }

  /* Handle last 1-2 digits. */
  if (value < 10) {
    dst[next] = '0' + (uint32_t)value;
  } else {
    int i = (uint32_t)value * 2;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
  }

  /* Add sign. */
  if (negative) dst[0] = '-';
  return length;
}

/* Create a string object from a long double.
 * If mode is humanfriendly it does not use exponential format and trims
 * trailing zeroes at the end (may result in loss of precision). If mode is
 * default exp format is used and the output of snprintf() is not modified (may
 * result in loss of precision). If mode is hex hexadecimal format is used (no
 * loss of precision)
 *
 * The function returns the length of the string or zero if there was not
 * enough buffer room to store it. */
int ld2string(char* buf, size_t len, long double value, ld2string_mode mode) {
  size_t l = 0;

  if (isinf(value)) {
    /* Libc in odd systems (Hi Solaris!) will format infinite in a
     * different way, so better to handle it in an explicit way. */
    if (len < 5) return 0; /* No room. 5 is "-inf\0" */
    if (value > 0) {
      memcpy(buf, "inf", 3);
      l = 3;
    } else {
      memcpy(buf, "-inf", 4);
      l = 4;
    }
  } else {
    switch (mode) {
      case LD_STR_AUTO:
        l = snprintf(buf, len, "%.17Lg", value);
        if (l + 1 > len) return 0; /* No room. */
        break;
      case LD_STR_HEX:
        l = snprintf(buf, len, "%La", value);
        if (l + 1 > len) return 0; /* No room. */
        break;
      case LD_STR_HUMAN:
        /* We use 17 digits precision since with 128 bit floats that precision
         * after rounding is able to represent most small decimal numbers in a
         * way that is "non surprising" for the user (that is, most small
         * decimal numbers will be represented in a way that when converted
         * back into a string are exactly the same as what the user typed.) */
        l = snprintf(buf, len, "%.17Lf", value);
        if (l + 1 > len) return 0; /* No room. */
        /* Now remove trailing zeroes after the '.' */
        if (strchr(buf, '.') != NULL) {
          char* p = buf + l - 1;
          while (*p == '0') {
            p--;
            l--;
          }
          if (*p == '.') l--;
        }
        if (l == 2 && buf[0] == '-' && buf[1] == '0') {
          buf[0] = '0';
          l = 1;
        }
        break;
      default:
        return 0; /* Invalid mode. */
    }
  }
  buf[l] = '\0';
  return l;
}

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
int string2ll(const char* s, size_t slen, long long* value) {
  const char* p = s;
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
int string2ld(const char* s, size_t slen, long double* dp) {
  char buf[MAX_LONG_DOUBLE_CHARS];
  long double value;
  char* eptr;

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

static long long mstime(void) {
  struct timeval tv;
  long long mst;

  gettimeofday(&tv, NULL);
  mst = ((long long)tv.tv_sec) * 1000;
  mst += tv.tv_usec / 1000;
  return mst;
}