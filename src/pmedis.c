#include "pmedis.h"

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (RedisModule_Init(ctx, "pmedis", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (argc != 1) {
    RedisModule_Log(ctx, "warning", "Number of args for pmedis must be 1");
    return REDISMODULE_ERR;
  }

  const char *s = RedisModule_StringPtrLen(argv[0], NULL);
  RedisModule_Log(ctx, "notice", "PMem root : %s", s);
  InitKVDK(ctx, argv, argc);

  if (RedisModule_CreateCommand(ctx, "pm.hello", PmHello_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "pm.sleep1m", PmSleep_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "hello.block", HelloBlock_RedisCommand, "",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "hello.keys", HelloKeys_RedisCommand, "",
                                0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
