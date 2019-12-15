package centrifuge

import (
	"bytes"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"math"
	"sync"
)

const (
	maxSeq uint32 = math.MaxUint32 // maximum uint32 value
	maxGen uint32 = math.MaxUint32 // maximum uint32 value
)

func nextSeqGen(currentSeq, currentGen uint32) (uint32, uint32) {
	var nextSeq uint32
	nextGen := currentGen
	if currentSeq == maxSeq {
		nextSeq = 0
		nextGen++
	} else {
		nextSeq = currentSeq + 1
	}
	return nextSeq, nextGen
}

func uint64Sequence(currentSeq, currentGen uint32) uint64 {
	return uint64(currentGen)*uint64(math.MaxUint32) + uint64(currentSeq)
}

func unpackUint64(val uint64) (uint32, uint32) {
	return uint32(val), uint32(val >> 32)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

var bufferPool = sync.Pool{
	// New is called when a new instance is needed
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

func JWTKeyFunc(config Config) func(token *jwt.Token) (interface{}, error) {
	return func(token *jwt.Token) (interface{}, error) {
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if config.Secret == "" {
				return nil, fmt.Errorf("config.secret not set")
			}
			return []byte(config.Secret), nil

		case *jwt.SigningMethodRSA:
			if config.Secret == "" {
				return nil, fmt.Errorf("public key nof found in config.secret")
			}
			return jwt.ParseRSAPublicKeyFromPEM([]byte(config.Secret))
		default:
			return nil, fmt.Errorf("unsupported signing method: %v. centrifugo supports HMAC and RSA", token.Header["alg"])
		}
	}
}
