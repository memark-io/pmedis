/*
 * Copyright 2022 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define REDISMODULE_EXPERIMENTAL_API
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "kvdk/engine.h"
#include "redismodule.h"
#include "util.h"
#include "util_redis.h"

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

typedef struct ThdPoolTask ThdPoolTask_t;
typedef struct ThdPool ThdPool_t;
extern ThdPool_t *thp;

typedef struct {
  char const *data;
  size_t len;
  size_t ret;
} HSetArgs;

typedef struct {
  size_t ret; /*1: exists; 0: not exists */
} HDelArgs;

typedef struct {
  long double ld_incr_by;
  long double ld_result;
  long long ll_incr_by;
  long long ll_result;
  rmw_err_msg err_no;
} IncNArgs;

typedef struct {
  const char *val_str;
  size_t val_len;
  long long ll_offset;
  long long ll_strlen_after_result;
  rmw_err_msg err_no;
} SetRangeArgs;

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
extern int pmAppendCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmIncrCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmDecrCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmIncrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmIncrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
extern int pmDecrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmStrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc);
extern int pmSetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSetrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc);
extern int pmPsetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc);
extern int pmGetrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc);
extern int pmGetsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmGetdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmGetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmMgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmMsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmMsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);

// List Commands
extern int pmRpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmLpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmRpushxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmLpushxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmLinsertCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmRpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmLpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmBrpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmBrpoplpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);
extern int pmBlmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmBlpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmLlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmLindexCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmLsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmLrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmLtrimCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmLposCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmLremCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmRpoplpushCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                              int argc);
extern int pmLmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);

// Hash Commands
extern int pmHsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmHsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmHgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmHmsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmHmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmHincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmHincrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                 int argc);
extern int pmHdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmHlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmHstrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmHkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmHvalsCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmHgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmHexistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmHrandfieldCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);
extern int pmHscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);

// Set Commands
extern int pmSaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmScardCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSdiffCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSdiffstoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);
extern int pmSinterCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmSinterstoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
extern int pmSismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                              int argc);
extern int pmSmembersCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc);
extern int pmSmismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);
extern int pmSmoveCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSpopCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmSrandmemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
extern int pmSremCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmSscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
extern int pmSunionCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
extern int pmSunionstoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
// ZSet Commands
extern int pmZaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
extern int pmZpopminCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
extern int pmZpopmaxCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
// Thread pool
extern int thp_destroy(ThdPool_t *);
extern ThdPool_t *thp_create(int, int, int);
extern int thp_add(ThdPool_t *, void *func, RedisModuleBlockedClient *,
                   RedisModuleString **, int);