#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib
export PATH=$PATH:/workdir:/workdir/redis-6.2.6/src
rm -rf /pmem/pmedis
rm -rf /ssd/redis-dir
mkdir -p /ssd/redis-dir
redis-server /workdir/pmedis.conf
echo "Starting pmedis-server in docker ..."
echo "Start initialization and wait for 120 secs"
sleep 120
echo "Start benchmarking PMedis and Redis_noAOF ... "
pmedis-benchmark -h 127.0.0.1 -p 56379 -r 2000000 --csv -n 2000000 -d 128 -c 32 -t GET,SET,LPUSH,LPOP,RPOP,HSET,INCR,SADD,SPOP,ZADD,ZPOPMIN
rm -rf /pmem/pmedis
rm -rf /ssd/redis-dir
