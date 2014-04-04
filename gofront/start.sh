#!/bin/sh

sudo su
export GOROOT=/usr/local/go
export GOPATH=/home/ec2-user/go
export PATH=$PATH:$GOROOT/bin
ulimit -n 99999
go run run_hbase.go
exit
