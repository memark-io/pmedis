#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

int PmHello_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
	REDISMODULE_NOT_USED(argv);
	REDISMODULE_NOT_USED(argc);
	RedisModule_ReplyWithSimpleString(ctx, "Hello, this is Pmedis!");
	return REDISMODULE_OK;
}

/* Reply callback for blocking command HELLO.BLOCK */
int HelloBlock_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    int *myint = RedisModule_GetBlockedClientPrivateData(ctx);
    return RedisModule_ReplyWithLongLong(ctx,*myint);
}

/* Timeout callback for blocking command HELLO.BLOCK */
int HelloBlock_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    return RedisModule_ReplyWithSimpleString(ctx,"Request timedout");
}

/* Private data freeing callback for HELLO.BLOCK command. */
void HelloBlock_FreeData(RedisModuleCtx *ctx, void *privdata) {
    REDISMODULE_NOT_USED(ctx);
    RedisModule_Free(privdata);
}

/* The thread entry point that actually executes the blocking part
 * of the command HELLO.BLOCK. */
void *HelloBlock_ThreadMain(void *arg) {
    void **targ = arg;
    RedisModuleBlockedClient *bc = targ[0];
    long long delay = (unsigned long)targ[1];
    RedisModule_Free(targ);

    sleep(delay);
    int *r = RedisModule_Alloc(sizeof(int));
    *r = rand();
    RedisModule_UnblockClient(bc,r);
    return NULL;
}

/* An example blocked client disconnection callback.
 *
 * Note that in the case of the HELLO.BLOCK command, the blocked client is now
 * owned by the thread calling sleep(). In this specific case, there is not
 * much we can do, however normally we could instead implement a way to
 * signal the thread that the client disconnected, and sleep the specified
 * amount of seconds with a while loop calling sleep(1), so that once we
 * detect the client disconnection, we can terminate the thread ASAP. */
void HelloBlock_Disconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    RedisModule_Log(ctx,"warning","Blocked client %p disconnected!",
        (void*)bc);

    /* Here you should cleanup your state / threads, and if possible
     * call RedisModule_UnblockClient(), or notify the thread that will
     * call the function ASAP. */
}

/* HELLO.BLOCK <delay> <timeout> -- Block for <count> seconds, then reply with
 * a random number. Timeout is the command timeout, so that you can test
 * what happens when the delay is greater than the timeout. */
int HelloBlock_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long long delay;
    long long timeout;

    if (RedisModule_StringToLongLong(argv[1],&delay) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count");
    }

    if (RedisModule_StringToLongLong(argv[2],&timeout) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count");
    }

    pthread_t tid;
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,HelloBlock_Reply,HelloBlock_Timeout,HelloBlock_FreeData,timeout);

    /* Here we set a disconnection handler, however since this module will
     * block in sleep() in a thread, there is not much we can do in the
     * callback, so this is just to show you the API. */
    RedisModule_SetDisconnectCallback(bc,HelloBlock_Disconnected);

    /* Now that we setup a blocking client, we need to pass the control
     * to the thread. However we need to pass arguments to the thread:
     * the delay and a reference to the blocked client handle. */
    void **targ = RedisModule_Alloc(sizeof(void*)*2);
    targ[0] = bc;
    targ[1] = (void*)(unsigned long) delay;

    if (pthread_create(&tid,NULL,HelloBlock_ThreadMain,targ) != 0) {
        RedisModule_AbortBlock(bc);
        return RedisModule_ReplyWithError(ctx,"-ERR Can't start thread");
    }
    return REDISMODULE_OK;
}

/* The thread entry point that actually executes the blocking part
 * of the command HELLO.KEYS.
 *
 * Note: this implementation is very simple on purpose, so no duplicated
 * keys (returned by SCAN) are filtered. However adding such a functionality
 * would be trivial just using any data structure implementing a dictionary
 * in order to filter the duplicated items. */
void *HelloKeys_ThreadMain(void *arg) {
    RedisModuleBlockedClient *bc = arg;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
    long long cursor = 0;
    size_t replylen = 0;

    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    do {
        RedisModule_ThreadSafeContextLock(ctx);
        RedisModuleCallReply *reply = RedisModule_Call(ctx,
            "SCAN","l",(long long)cursor);
        RedisModule_ThreadSafeContextUnlock(ctx);

        RedisModuleCallReply *cr_cursor =
            RedisModule_CallReplyArrayElement(reply,0);
        RedisModuleCallReply *cr_keys =
            RedisModule_CallReplyArrayElement(reply,1);

        RedisModuleString *s = RedisModule_CreateStringFromCallReply(cr_cursor);
        RedisModule_StringToLongLong(s,&cursor);
        RedisModule_FreeString(ctx,s);

        size_t items = RedisModule_CallReplyLength(cr_keys);
        for (size_t j = 0; j < items; j++) {
            RedisModuleCallReply *ele =
                RedisModule_CallReplyArrayElement(cr_keys,j);
            RedisModule_ReplyWithCallReply(ctx,ele);
            replylen++;
        }
        RedisModule_FreeCallReply(reply);
    } while (cursor != 0);
    RedisModule_ReplySetArrayLength(ctx,replylen);

    RedisModule_FreeThreadSafeContext(ctx);
    RedisModule_UnblockClient(bc,NULL);
    return NULL;
}

/* HELLO.KEYS -- Return all the keys in the current database without blocking
 * the server. The keys do not represent a point-in-time state so only the keys
 * that were in the database from the start to the end are guaranteed to be
 * there. */
int HelloKeys_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) return RedisModule_WrongArity(ctx);

    pthread_t tid;

    /* Note that when blocking the client we do not set any callback: no
     * timeout is possible since we passed '0', nor we need a reply callback
     * because we'll use the thread safe context to accumulate a reply. */
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx,NULL,NULL,NULL,0);

    /* Now that we setup a blocking client, we need to pass the control
     * to the thread. However we need to pass arguments to the thread:
     * the reference to the blocked client handle. */
    if (pthread_create(&tid,NULL,HelloKeys_ThreadMain,bc) != 0) {
        RedisModule_AbortBlock(bc);
        return RedisModule_ReplyWithError(ctx,"-ERR Can't start thread");
    }
    return REDISMODULE_OK;
}

int PmSleep_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
	REDISMODULE_NOT_USED(argv);
	REDISMODULE_NOT_USED(argc);
	sleep(60);
	RedisModule_ReplyWithSimpleString(ctx, "Sleeped 1 mins");
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
	if (RedisModule_CreateCommand(ctx, "pm.sleep1m", PmSleep_RedisCommand,
                                "readonly", 0, 0, 0) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"hello.block",
        HelloBlock_RedisCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"hello.keys",
        HelloKeys_RedisCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
