#!/bin/sh

export GOROOT=/usr/local/go
export GOPATH=/home/ec2-user/go
export PATH=$PATH:$GOROOT/bin
go run run_hbase.go &

