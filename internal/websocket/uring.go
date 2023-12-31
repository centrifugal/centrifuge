package websocket

import (
	"log"
	"net"
	"reflect"
	"sync"

	"github.com/centrifugal/centrifuge/internal/iouring-go"
)

func initIoUring() *iouring.IOURing {
	ring, err := iouring.New(64)
	if err != nil {
		log.Fatalf("Failed to initialize io_uring: %v", err)
	}
	return ring
}

var ringLock sync.Mutex
var ring = initIoUring()

func getFdFromConn(c net.Conn) int {
	v := reflect.Indirect(reflect.ValueOf(c))
	conn := v.FieldByName("conn")
	netFD := reflect.Indirect(conn.FieldByName("fd"))
	pfd := netFD.FieldByName("pfd")
	fd := int(pfd.FieldByName("Sysfd").Int())
	return fd
}
