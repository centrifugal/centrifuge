//go:build linux
// +build linux

package iouring

import (
	"errors"
	"unsafe"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

func (iour *IOURing) RegisterBuffers(bs [][]byte) error {
	if len(bs) == 0 {
		return errors.New("buffer is empty")
	}

	iovecs := bytes2iovec(bs)
	bp := unsafe.Pointer(&iovecs[0])

	return iouring_syscall.IOURingRegister(iour.fd, iouring_syscall.IORING_REGISTER_BUFFERS, bp, uint32(len(iovecs)))
}

func (iour *IOURing) UnRegisterBuffers() error {
	return iouring_syscall.IOURingRegister(iour.fd, iouring_syscall.IORING_UNREGISTER_BUFFERS, nil, 0)
}
