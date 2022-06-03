# Benchmark Guide

A patch file for redis-benchmark, [redis-benchmark.patch](../test/redis-benchmark.patch) is prepared for users to build the benchmark tool, **pmedis-benchmark**. For every command supported in the original redis-benchmark.c (in [redis-6.2.6](https://github.com/redis/redis/releases/tag/6.2.6)), **pemdis-benchmark** will add the test of its version in PMedis with *PM.* prefix.

To make it easier to use, we integrated it into the docker image which can be built using the provided [pmedis.dockerfile](../test/pmedis.dockerfile).

## 1. Build the docker image
```
$ docker build --ssh default -t pmedis_example:dev -f pmedis.dockerfile .
```

## 2. Benchmarking PMedis and Redis without AOF

You **must prepare two path** for the benchmark, ``<pmem_path>`` for storing PMem data, ``<ssd_path>`` for storing data (logs, RDB and AOF files) of the vanilla Redis. Then you execute the command below, it is similar to the one used to try PMedis but with the last argument being ``bench_noaof``.
```
$ docker run -it --rm --name pmedis_example -p 56379:56379 -v <pmem_path>:/pmem -v <ssd_path>:/ssd pmedis_example:dev bench_noaof
```
Here's an example of the output of the benchmark.
```
Starting pmedis-server in docker ...
Start initialization and wait for 120 secs
Start benchmarking PMedis and Redis_noAOF ...
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","92293.49","0.181","0.072","0.175","0.191","0.255","158.463"
"pm.SET","100361.30","0.173","0.072","0.175","0.199","0.255","1.231"
"GET","91390.97","0.181","0.072","0.183","0.191","0.231","143.871"
"pm.GET","93558.49","0.180","0.072","0.175","0.191","0.303","1.815"
"INCR","92545.46","0.179","0.072","0.183","0.191","0.239","2.383"
"pm.INCR","94800.21","0.200","0.120","0.175","0.439","0.607","146.815"
"LPUSH","91349.23","0.181","0.064","0.183","0.191","0.239","2.431"
"pm.LPUSH","95556.62","0.174","0.072","0.175","0.183","0.231","1.767"
"LPOP","90921.49","0.182","0.096","0.183","0.191","0.231","157.823"
"pm.LPOP","93971.71","0.178","0.072","0.175","0.191","0.271","7.079"
"RPOP","90175.39","0.182","0.072","0.183","0.199","0.279","1.583"
"pm.RPOP","88683.93","0.188","0.064","0.183","0.215","0.383","159.359"
"SADD","93383.76","0.177","0.072","0.175","0.191","0.231","1.359"
"pm.SADD","100260.68","0.185","0.072","0.175","0.263","0.343","4.287"
"HSET","90929.75","0.186","0.072","0.175","0.215","0.495","184.191"
"pm.HSET","89887.64","0.266","0.080","0.231","0.567","0.703","71.423"
"SPOP","95070.59","0.175","0.064","0.175","0.191","0.263","2.559"
"pm.SPOP","94446.54","0.185","0.080","0.183","0.239","0.375","11.727"
"ZADD","96613.69","0.182","0.072","0.175","0.207","0.279","199.807"
"pm.ZADD","75446.07","0.371","0.152","0.359","0.495","0.527","18.047"
"ZPOPMIN","92383.02","0.181","0.056","0.175","0.191","0.231","200.319"
"pm.ZPOPMIN","25764.56","1.158","0.144","1.087","1.903","2.191","6.727"
```


## 3. Benchmarking PMedis and Redis with AOF on SSD

Similar to the steps above, just change the last argument of the command to ``bench_aofalways``
```
$ docker run -it --rm --name pmedis_example -p 56379:56379 -v <pmem_path>:/pmem -v <ssd_path>:/ssd pmedis_example:dev bench_aofalways
```
Here's an example of the output of the benchmark.
```
Starting pmedis-server in docker ...
Start initialization and wait for 120 secs
Start benchmarking PMedis and Redis_AOF_fsyncalways ...
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","20256.86","1.492","0.664","1.519","1.879","2.039","166.271"
"pm.SET","98755.67","0.177","0.064","0.175","0.215","0.287","2.511"
"GET","91903.32","0.181","0.064","0.175","0.191","0.247","143.615"
"pm.GET","95156.53","0.174","0.072","0.175","0.183","0.231","2.207"
"INCR","21768.94","1.393","0.552","1.439","1.783","1.919","146.687"
"pm.INCR","95877.27","0.200","0.072","0.175","0.447","0.639","3.231"
"LPUSH","20561.75","1.473","0.552","1.503","1.855","1.991","168.959"
"pm.LPUSH","96153.85","0.176","0.072","0.167","0.183","0.279","158.207"
"LPOP","20921.60","1.449","0.624","1.511","1.855","1.975","166.911"
"pm.LPOP","92472.72","0.182","0.072","0.175","0.223","0.527","7.079"
"RPOP","91759.96","0.181","0.064","0.183","0.191","0.239","157.823"
"pm.RPOP","93062.22","0.177","0.072","0.175","0.191","0.231","0.847"
"SADD","20563.02","1.476","0.376","1.527","1.887","2.055","166.015"
"pm.SADD","98619.33","0.188","0.072","0.175","0.279","0.391","3.679"
"HSET","18710.47","1.617","0.672","1.623","2.039","2.367","175.487"
"pm.HSET","97933.59","0.224","0.080","0.207","0.343","0.575","115.519"
"SPOP","26731.18","1.076","0.064","1.207","1.927","2.159","173.567"
"pm.SPOP","96856.99","0.189","0.064","0.183","0.247","0.375","6.615"
"ZADD","18859.21","1.596","0.672","1.615","1.959","2.111","177.791"
"pm.ZADD","73067.37","0.379","0.136","0.367","0.503","0.535","43.775"
"ZPOPMIN","28772.84","0.993","0.088","1.167","1.783","1.927","178.175"
"pm.ZPOPMIN","28346.28","1.053","0.200","1.031","1.623","1.871","171.775"
```