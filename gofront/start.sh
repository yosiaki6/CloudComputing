#!/bin/sh

export GOROOT=/usr/local/go
export GOPATH=/home/ec2-user/go
export PATH=$PATH:$GOROOT/bin
ulimit -n 9999
go run run_hbase.go

