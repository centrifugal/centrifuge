// +build linux

package iouring_syscall

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// IOUringEnter flags
const (
	IORING_ENTER_FLAGS_GETEVENTS uint32 = 1 << iota
	IORING_ENTER_FLAGS_SQ_WAKEUP
	IORING_ENTER_FLAGS_SQ_WAIT
)

func IOURingEnter(fd int, toSubmit uint32, minComplete uint32, flags uint32, sigset *unix.Sigset_t) (int, error) {
	res, _, errno := syscall.Syscall6(
		SYS_IO_URING_ENTER,
		uintptr(fd),
		uintptr(toSubmit),
		uintptr(minComplete),
		uintptr(flags),
		uintptr(unsafe.Pointer(sigset)),
		0,
	)
	if errno != 0 {
		return 0, os.NewSyscallError("iouring_enter", errno)
	}
	if res < 0 {
		return 0, os.NewSyscallError("iouring_enter", syscall.Errno(-res))
	}

	return int(res), nil
}
