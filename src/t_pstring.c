#include "pmedis.h"
static char* safe_strcat(char *__restrict s1, size_t s1_size, const char *__restrict s2, size_t s2_size) {
  size_t res_len = 1 + s1_size + s2_size;
  char* res = (char*)malloc(res_len);
  memset(res, 0, res_len);
  memcpy(res, s1, s1_size);
  memcpy(res+s1_size, s2, s2_size);
  return res;
}


int pmappendCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  size_t key_len, append_val_len, ori_val_len;
  char *ori_val_str;
  size_t target_len;
  if (argc != 3) return RedisModule_WrongArity(ctx);
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);  
  const char *append_val_str = RedisModule_StringPtrLen(argv[2], &append_val_len);
  char *target_str = NULL;

  KVDKStatus s = KVDKGet(engine, key_str, key_len, &ori_val_len, &ori_val_str);

  if (s != Ok && s != NotFound) {
    /* Something Err in KVDK */
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }else if(s == NotFound){
    /* Create the key */
    target_len = append_val_len;
    target_str = (char *) append_val_str;
  }else{
    /* Key exists, check type */
    // TODO: require KVDK support input key, output: key-type
    // TODO: if(type != OBJ_STRING) return RedisModule_ReplyWithError(ctx,"ERR Wrong Type");
    /* Append the value */
    target_len = append_val_len + ori_val_len;
    target_str = safe_strcat(ori_val_str, ori_val_len, append_val_str, append_val_len);
  }
  s = KVDKSet(engine, key_str, key_len, target_str, target_len);
  free(ori_val_str); // free memory allocated by KVDKGet
  free(target_str);

  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, target_len);
}

int pmstrlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len,val_len,str_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }else if(s == NotFound){
    /* Key not exist*/
    str_len = 0;
  }else{
    /* Key exists, check type */
    // TODO: require KVDK support input key, output: key-type
    // TODO: if(type != OBJ_STRING) return RedisModule_ReplyWithError(ctx,"ERR Wrong Type");
    /* Append the value */
    str_len = val_len;
  }
  return RedisModule_ReplyWithLongLong(ctx, str_len);                 
}
int pmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  // Warrning: memory leak
  return RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
}

int pmsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}