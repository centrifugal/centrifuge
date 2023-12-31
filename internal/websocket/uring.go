package websocket

import (
	"fmt"
	"log"
	"net"
	"syscall"

	"github.com/centrifugal/centrifuge/internal/iouring-go"
)

func initIoUring() *iouring.IOURing {
	ring, err := iouring.New(64)
	if err != nil {
		log.Fatalf("Failed to initialize io_uring: %v", err)
	}
	return ring
}

var ring = initIoUring()

func extractConnFd(conn net.Conn) (uintptr, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return 0, fmt.Errorf("unsupported Conn type: %T", conn)
	}

	file, err := tcpConn.File()
	if err != nil {
		return 0, err
	}
	fd := file.Fd()

	// It's important to set the conn back to non-blocking mode
	if err := setNonBlocking(fd); err != nil {
		_ = file.Close()
		return 0, err
	}

	_ = file.Close()
	return fd, nil
}

func setNonBlocking(fd uintptr) error {
	// Set the file descriptor to non-blocking mode
	return syscall.SetNonblock(int(fd), true)
}
