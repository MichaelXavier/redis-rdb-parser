#!/bin/bash

set -e

redis-server test/data/example.conf
cmd="redis-cli -p 6378"

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
# empty sets
$cmd -n 0 SADD emptyset shortlived
$cmd -n 0 SPOP emptyset
# hashes
# $cmd -n 0 HSET hash str stringval
# $cmd -n 0 HINCRBY hash int 42
# empty hashes
# $cmd -n 0 HSET emptyhash k shortlived
# $cmd -n 0 HDEL emptyhash k
# sorted sets
# $cmd -n 0 ZADD zset 2 lastplace 1 firstplace
# empty sorted sets
# $cmd -n 0 ZADD emptyzset 1 shortlived
# $cmd -n 0 ZREM emptyzset shortlived
# strings in another database
$cmd -n 1 SET bar barval
#TODO: timestamps? have to se them wayyy in the future

$cmd SAVE

kill $(cat test/data/redis.pid)
