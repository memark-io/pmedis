#include "redismodule.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int PmHello_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  RedisModule_ReplyWithSimpleString(ctx, "Hello, this is Pmedis!");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (RedisModule_Init(ctx, "pmedis", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  /* Log the list of parameters passing loading the module. */
  for (int j = 0; j < argc; j++) {
    const char *s = RedisModule_StringPtrLen(argv[j], NULL);
    printf("Module loaded with ARGV[%d] = %s\n", j, s);
  }

  if (RedisModule_CreateCommand(ctx, "pm.hello", PmHello_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
