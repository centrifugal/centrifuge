package main

import (
	"flag"
	"log"
	"sync/atomic"
)

var (
	addr    = flag.String("addr", "localhost:8001", "Server address, e.g. centrifuge.io:443")
	useTLS  = flag.Bool("tls", false, "Use TLS")
	cert    = flag.String("cert", "", "CA certificate file")
	channel = flag.String("channel", "index", "channel to subscribe")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ldate)
}

var msgID int64

func nextID() int64 {
	return atomic.AddInt64(&msgID, 1)
}

// Disconnect ...
type Disconnect struct {
	Reason    string `json:"reason"`
	Reconnect bool   `json:"reconnect"`
}

func main() {
	flag.Parse()
	run()
}
