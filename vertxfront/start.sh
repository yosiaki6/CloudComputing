#!/bin/sh

ulimit -n 999999
/usr/local/vertx/bin/vertx run src/Server.java -instances $1
