#!/bin/bash
go build -buildmode=plugin -race ../mrapps/wc.go && \
go build -race mrworker.go && \
./mrworker wc.so