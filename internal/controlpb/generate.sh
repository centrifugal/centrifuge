#!/bin/bash

# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

which protoc
protoc-gen-go --version

protoc --go_out=. --plugin protoc-gen-go=${GOBIN}/protoc-gen-go control.proto

go run github.com/centrifugal/protocol/cfprotobuf/cmd/cfprotobuf -out control.pb_cfprotobuf.go control.pb.go
