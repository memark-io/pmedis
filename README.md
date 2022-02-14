# pmedis
A Redis module to provide support for storing Redis native data structures on PMem.
===
## 1. Introduction

## 2. Get Started

### 2.1 Build the .so library file of the module
```bash
mkdir build && cd build
cmake ..
make
```

### 2.2 Start Redis CLI, load the module and try the pm.* command
```bash
$ src/redis-cli -p 46379
127.0.0.1:46379> module list
(empty array)
127.0.0.1:46379> pm.hello
(error) ERR unknown command `pm.hello`, with args beginning with:
127.0.0.1:46379> module load ${PMEDIS_BIN}/libpmedis.so
OK
127.0.0.1:46379> pm.hello
Hello, this is Pmedis!
```