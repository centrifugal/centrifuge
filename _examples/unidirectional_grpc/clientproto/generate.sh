#!/bin/bash

set -e

DST_DIR=./

protoc -I ./ \
  uni.proto \
  --go_out=${DST_DIR} \
  --go-grpc_out=${DST_DIR}
