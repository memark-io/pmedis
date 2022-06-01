#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib
export PATH=$PATH:/workdir/redis-6.2.6/src
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