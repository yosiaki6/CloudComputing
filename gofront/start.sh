#!/bin/sh

export GOROOT=/usr/local/go
export GOPATH=/home/hadoop/gocode
export PATH=$PATH:$GOROOT/bin
ulimit -n 999999
go run run_hbase.go $1 $2 $3

