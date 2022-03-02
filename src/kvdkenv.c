#include "pmedis.h"

const char *pmem_path = NULL;
KVDKEngine *engine = NULL;
KVDKConfigs *config = NULL;
const char *enum_to_str[] = {FOREACH_ENUM(GENERATE_STRING)};

int GetInt64Value(uint64_t *var, const char *value) {
  if (strstr(value, "<<")) {
    uint64_t left_val, right_val;
    left_val = strtoull(strtok((char *)value, "<<"), NULL, 10);
    right_val = strtoull(strtok(NULL, "<<"), NULL, 10);
    *var = left_val << right_val;
  } else if (strstr(value, ">>")) {
    uint64_t left_val, right_val;
    left_val = strtoull(strtok((char *)value, ">>"), NULL, 10);
    right_val = strtoull(strtok(NULL, ">>"), NULL, 10);
    *var = left_val >> right_val;
  } else if (strstr(value, "*")) {
    *var = 1;
    char *p = strtok((char *)value, "*");
    while (p) {
      (*var) *= strtoull(p, NULL, 10);
      p = strtok(NULL, "*");
    }
  } else {
    *var = strtoull(value, NULL, 10);
  }
  return 1;
}

int GetInt32Value(uint32_t *var, const char *value) {
  if (strstr(value, "<<")) {
    uint32_t left_val, right_val;
    left_val = (uint32_t)strtoul(strtok((char *)value, "<<"), NULL, 10);
    right_val = (uint32_t)strtoul(strtok(NULL, "<<"), NULL, 10);
    *var = left_val << right_val;
  } else if (strstr(value, ">>")) {
    uint32_t left_val, right_val;
    left_val = (uint32_t)strtoul(strtok((char *)value, ">>"), NULL, 10);
    right_val = (uint32_t)strtoul(strtok(NULL, ">>"), NULL, 10);
    *var = left_val >> right_val;
  } else if (strstr(value, "*")) {
    *var = 1;
    char *p = strtok((char *)value, "*");
    while (p) {
      (*var) *= (uint32_t)strtoul(p, NULL, 10);
      p = strtok(NULL, "*");
    }
  } else {
    *var = strtoull(value, NULL, 10);
  }
  return 1;
}

KVDKConfigs *LoadAndCreateConfigs(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  uint64_t pmem_file_size = PMEM_FILE_SIZE, hash_bucket_num = HASH_BUCKET_NUM,
           pmem_segment_blocks = PMEM_SEG_BLOCKS, max_write_threads;
  uint32_t pmem_block_size = PMEM_BLOCK_SIZE,
           hash_bucket_size = HASH_BUCKET_SIZE,
           num_buckets_per_slot = NUM_BUCKETS_PER_SLOT;
  unsigned char populate_pmem_space = POPULATE_PMEM_SPACE;
  /* Log the list of parameters passing loading the module. */
  for (int j = 0; j < argc; j += 2) {
    const char *config_name = RedisModule_StringPtrLen(argv[j], NULL);
    const char *config_value = RedisModule_StringPtrLen(argv[j + 1], NULL);
    printf("Module loaded with ARG_NAME[%d] = %s, ARG_VALUE[%d] = %s\n", j,
           config_name, j + 1, config_value);
    if ((!strcmp(config_name, "pmem_file_size") &&
         GetInt64Value(&pmem_file_size, config_value)) ||
        (!strcmp(config_name, "pmem_segment_blocks") &&
         GetInt64Value(&pmem_segment_blocks, config_value)) ||
        (!strcmp(config_name, "hash_bucket_num") &&
         GetInt64Value(&hash_bucket_num, config_value)) ||
        (!strcmp(config_name, "max_write_threads") &&
         GetInt64Value(&max_write_threads, config_value))) {
      continue;
    } else if (!strcmp(config_name, "populate_pmem_space")) {
      populate_pmem_space = (unsigned char)atoi(config_value);
    } else if ((!strcmp(config_name, "pmem_block_size") &&
                GetInt32Value(&pmem_block_size, config_value)) ||
               (!strcmp(config_name, "hash_bucket_size") &&
                GetInt32Value(&hash_bucket_size, config_value)) ||
               (!strcmp(config_name, "num_buckets_per_slot") &&
                GetInt32Value(&num_buckets_per_slot, config_value))) {
      continue;
    } else if (!strcmp(config_name, "pmem_path")) {
      pmem_path = config_value;
    } else {
      RedisModule_Log(ctx, "warning", "Invalid args for Module pmedis");
      return NULL;
    }
  }

  KVDKConfigs *kvdk_configs = KVDKCreateConfigs();
  KVDKSetConfigs(kvdk_configs, max_write_threads, pmem_file_size,
                  populate_pmem_space, pmem_block_size, pmem_segment_blocks,
                  hash_bucket_size, hash_bucket_num, num_buckets_per_slot);
  return kvdk_configs;
}

int InitKVDK(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  config = LoadAndCreateConfigs(ctx, argv, argc);
  if (config == NULL) {
    return REDISMODULE_ERR;
  }
  if ((pmem_path != NULL && pmem_path[0] == '\0')) {
    return REDISMODULE_ERR;
  }

  // Purge old KVDK instance
  KVDKRemovePMemContents(pmem_path);

  // open engine
  KVDKStatus s = KVDKOpen(pmem_path, config, stdout, &engine);
  if (s != Ok) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}