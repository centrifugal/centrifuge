#!/bin/bash

# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

set -e

DST_DIR=./

protoc -I ./ \
  app.proto \
  --go_out=${DST_DIR} \
  --go-grpc_out=${DST_DIR}
