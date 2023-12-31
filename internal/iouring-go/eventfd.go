//go:build linux
// +build linux

package iouring

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

func (iour *IOURing) registerEventfd() error {
	eventfd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		return os.NewSyscallError("eventfd", err)
	}
	iour.eventfd = eventfd

	return iouring_syscall.IOURingRegister(
		iour.fd,
		iouring_syscall.IORING_REGISTER_EVENTFD,
		unsafe.Pointer(&iour.eventfd), 1,
	)
}
