#!/bin/bash

set -e

redis-server test/data/example.conf
cmd="redis-cli -p 6378"

sleep 1

# strings in the default database
$cmd -n 0 SET foo fooval
# integers
$cmd -n 0 INCRBY num 42
# TODO: i think redis supports floating point now?
# lists
$cmd -n 0 RPUSH list one two
# empty lists
# i'm not actually sure that empty lists/sets/hashes will get persisted
$cmd -n 0 RPUSH emptylist shortlived
$cmd -n 0 RPOP emptylist
# sets
$cmd -n 0 SADD set one two
# 16 bit int sets
$cmd -n 0 SADD intset16 1 2
# 32 bit int sets
$cmd -n 0 SADD intset32 65536 65537
# 64 bit int sets
$cmd -n 0 SADD intset64 4294967296 4294967297
# hashes
$cmd -n 0 HSET hash str stringval
$cmd -n 0 HINCRBY hash int 42
# sorted sets
$cmd -n 0 ZADD zset 2 lastplace 1 firstplace
# strings in another database
$cmd -n 1 SET bar barval
#TODO: timestamps? have to set them wayyy in the future

$cmd SAVE

kill $(cat test/data/redis.pid)
