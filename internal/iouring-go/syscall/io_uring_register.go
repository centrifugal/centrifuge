// +build linux

package iouring_syscall

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	IORING_REGISTER_BUFFERS uint8 = iota
	IORING_UNREGISTER_BUFFERS
	IORING_REGISTER_FILES
	IORING_UNREGISTER_FILES
	IORING_REGISTER_EVENTFD
	IORING_UNREGISTER_EVENTFD
	IORING_REGISTER_FILES_UPDATE
	IORING_REGISTER_EVENTFD_ASYNC
	IORING_REGISTER_PROBE
	IORING_REGISTER_PERSONALITY
	IORING_UNREGISTER_PERSONALITY
	IORING_REGISTER_RESTRICTIONS
	IORING_REGISTER_ENABLE_RINGS
)

type IOURingFilesUpdate struct {
	Offset uint32
	recv   uint32
	Fds    *int32
}

func IOURingRegister(fd int, opcode uint8, args unsafe.Pointer, nrArgs uint32) error {
	for {
		_, _, errno := syscall.Syscall6(
			SYS_IO_URING_REGISTER,
			uintptr(fd),
			uintptr(opcode),
			uintptr(args),
			uintptr(nrArgs),
			0,
			0,
		)
		if errno != 0 {
			// EINTR may be returned when blocked
			if errno == syscall.EINTR {
				continue
			}
			return os.NewSyscallError("iouring_register", errno)
		}
		return nil
	}
}
