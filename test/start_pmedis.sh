#!/bin/bash
if [ $# -lt 1 ]
then
	echo "Miss arguments ... "
elif [ $1 == "try_pmedis" ]
then
	export LD_LIBRARY_PATH=/usr/local/lib
	export PATH=$PATH:/workdir/redis-6.2.6/src
	rm -rf /ssd/redis-dir
	mkdir -p /ssd/redis-dir
	redis-server /workdir/pmedis.conf
	echo "Starting pmedis-server in docker ..."
	if [[ -d "/pmem/pmedis" ]]
	then
		echo "PMedis data exists, start recovery and wait for 15 secs"
		sleep 15
	else
		echo "No PMedis data found, start initialization and wait for 100 secs"
		sleep 100
	fi
	echo "Have fun :)"
	redis-cli -p 56379
elif [ $1 == "bench_noaof" ]
then
	/bin/bash benchmark-pmedis-redis_noaof.sh
elif [ $1 == "bench_aofalways" ]
then
	/bin/bash benchmark-pmedis-redis_aofalways.sh
else
	echo "Wrong arguments provided, expecting [bench_noaof | bench_aofalways]"
fi