#define REDISMODULE_EXPERIMENTAL_API
#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "kvdk/engine.h"
#include "redismodule.h"

#define PMEM_FILE_SIZE 1 << 30
#define HASH_BUCKET_NUM 1 << 27
#define PMEM_SEG_BLOCKS 2 << 20
#define PMEM_BLOCK_SIZE 64
#define HASH_BUCKET_SIZE 128
#define NUM_BUCKETS_PER_SLOT 1
#define POPULATE_PMEM_SPACE 1U

extern const char *pmem_path;
extern KVDKEngine *engine;
extern KVDKConfigs *config;
extern const char *enum_to_str[];

extern int InitKVDK(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

extern int PmHello_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
extern int HelloBlock_Reply(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int HelloBlock_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv,
                              int argc);
extern void HelloBlock_FreeData(RedisModuleCtx *ctx, void *privdata);
extern void *HelloBlock_ThreadMain(void *arg);
extern void HelloBlock_Disconnected(RedisModuleCtx *ctx,
                                    RedisModuleBlockedClient *bc);
extern int HelloBlock_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv, int argc);
extern void *HelloKeys_ThreadMain(void *arg);
extern int HelloKeys_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                  int argc);
extern int PmSleep_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
// String commands
extern int PmGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                              int argc);
extern int PmSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                              int argc);