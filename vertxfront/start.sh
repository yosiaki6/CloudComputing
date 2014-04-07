#!/bin/sh

ulimit -n 999999
cd /home/hadoop/CloudComputing/vertxfront
/usr/local/vertx/bin/vertx run src/Server.java -instances $1
