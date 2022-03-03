# pmedis
A Redis module to provide support for storing Redis native data structures on PMem.
===
## 1. Introduction

## 2. Get Started

### 2.1 Build the .so library file of the module
```bash
git submodule update --init --recursive
bash build.sh
```

### 2.2 Start Redis Server with a redis.conf file
```bash
cd test
${REDIS_BIN}/redis-server redis.conf
```
```
// redis.conf has one line to load the pmedis module with default parameters as follows
loadmodule ./libpmedis.so pmem_path /mnt/pmem1/yj/pmedis max_write_threads 2 pmem_file_size  "1 << 30" populate_pmem_space 1 pmem_block_size 64 pmem_segment_blocks "2 << 20" hash_bucket_size 128 hash_bucket_num "1 << 27" num_buckets_per_slot 1
```