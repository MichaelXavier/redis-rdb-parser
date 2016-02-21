#!/bin/bash

set -e

redis-server test/data/example.conf
cmd="redis-cli -p 6378"

$cmd -n 0 SET foo fooval
$cmd -n 1 SET bar barval
$cmd SAVE

kill $(cat test/data/redis.pid)
