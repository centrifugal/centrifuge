// +build linux

package iouring

import (
	"syscall"
	"unsafe"
)

var zero uintptr

func bytes2iovec(bs [][]byte) []syscall.Iovec {
	iovecs := make([]syscall.Iovec, len(bs))
	for i, b := range bs {
		iovecs[i].SetLen(len(b))
		if len(b) > 0 {
			iovecs[i].Base = &b[0]
		} else {
			iovecs[i].Base = (*byte)(unsafe.Pointer(&zero))
		}
	}
	return iovecs
}

//go:linkname sockaddr syscall.Sockaddr.sockaddr
func sockaddr(addr syscall.Sockaddr) (unsafe.Pointer, uint32, error)

//go:linkname anyToSockaddr syscall.anyToSockaddr
func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error)
