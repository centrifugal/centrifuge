//go:build linux
// +build linux

package iouring

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

type PrepRequest func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData)

// WithInfo request with extra info
func (prepReq PrepRequest) WithInfo(info interface{}) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		prepReq(sqe, userData)
		userData.SetRequestInfo(info)
	}
}

func (prepReq PrepRequest) WithDrain() PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		prepReq(sqe, userData)
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_IO_DRAIN)
	}
}

func (prepReq PrepRequest) WithCallback(callback RequestCallback) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		prepReq(sqe, userData)
		userData.SetRequestCallback(callback)
	}
}

func (iour *IOURing) Read(file *os.File, b []byte, ch chan<- Result) (Request, error) {
	fd := int(file.Fd())
	if fd < 0 {
		return nil, errors.New("invalid file")
	}

	return iour.SubmitRequest(Read(fd, b), ch)
}

func (iour *IOURing) Write(file *os.File, b []byte, ch chan<- Result) (Request, error) {
	fd := int(file.Fd())
	if fd < 0 {
		return nil, errors.New("invalid file")
	}

	return iour.SubmitRequest(Write(fd, b), ch)
}

func (iour *IOURing) Pread(file *os.File, b []byte, offset uint64, ch chan<- Result) (Request, error) {
	fd := int(file.Fd())
	if fd < 0 {
		return nil, errors.New("invalid file")
	}

	return iour.SubmitRequest(Pread(fd, b, offset), ch)
}

func (iour *IOURing) Pwrite(file *os.File, b []byte, offset uint64, ch chan<- Result) (Request, error) {
	fd := int(file.Fd())
	if fd < 0 {
		return nil, errors.New("invalid file")
	}

	return iour.SubmitRequest(Pwrite(fd, b, offset), ch)
}

func Nop() PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		sqe.PrepOperation(iouring_syscall.IORING_OP_NOP, -1, 0, 0, 0)
	}
}

func Read(fd int, b []byte) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_READ,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			0,
		)
	}
}

func Pread(fd int, b []byte, offset uint64) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_READ,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			uint64(offset),
		)
	}
}

func Write(fd int, b []byte) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_WRITE,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			0,
		)
	}
}

func Pwrite(fd int, b []byte, offset uint64) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_WRITE,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			uint64(offset),
		)
	}
}

func Readv(fd int, bs [][]byte) PrepRequest {
	iovecs := bytes2iovec(bs)

	var bp unsafe.Pointer
	if len(iovecs) > 0 {
		bp = unsafe.Pointer(&iovecs[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffers(bs)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_READV,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(iovecs)),
			0,
		)
	}
}

func Preadv(fd int, bs [][]byte, offset uint64) PrepRequest {
	iovecs := bytes2iovec(bs)

	var bp unsafe.Pointer
	if len(iovecs) > 0 {
		bp = unsafe.Pointer(&iovecs[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffers(bs)

		sqe.PrepOperation(iouring_syscall.IORING_OP_READV,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(iovecs)),
			offset,
		)
	}
}

func Writev(fd int, bs [][]byte) PrepRequest {
	iovecs := bytes2iovec(bs)

	var bp unsafe.Pointer
	if len(iovecs) > 0 {
		bp = unsafe.Pointer(&iovecs[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffers(bs)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_WRITEV,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(iovecs)),
			0,
		)
	}
}

func Pwritev(fd int, bs [][]byte, offset int64) PrepRequest {
	iovecs := bytes2iovec(bs)

	var bp unsafe.Pointer
	if len(iovecs) > 0 {
		bp = unsafe.Pointer(&iovecs[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = fdResolver
		userData.SetRequestBuffers(bs)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_WRITEV,
			int32(fd),
			uint64(uintptr(bp)),
			uint32(len(iovecs)),
			uint64(offset),
		)
	}
}

func Send(sockfd int, b []byte, flags int) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_SEND,
			int32(sockfd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			0,
		)
		sqe.SetOpFlags(uint32(flags))
	}
}

func Recv(sockfd int, b []byte, flags int) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_RECV,
			int32(sockfd),
			uint64(uintptr(bp)),
			uint32(len(b)),
			0,
		)
		sqe.SetOpFlags(uint32(flags))
	}
}

func Sendmsg(sockfd int, p, oob []byte, to syscall.Sockaddr, flags int) (PrepRequest, error) {
	var ptr unsafe.Pointer
	var salen uint32
	if to != nil {
		var err error
		ptr, salen, err = sockaddr(to)
		if err != nil {
			return nil, err
		}
	}

	msg := &syscall.Msghdr{}
	msg.Name = (*byte)(ptr)
	msg.Namelen = uint32(salen)
	var iov syscall.Iovec
	if len(p) > 0 {
		iov.Base = &p[0]
		iov.SetLen(len(p))
	}
	var dummy byte
	if len(oob) > 0 {
		if len(p) == 0 {
			var sockType int
			sockType, err := syscall.GetsockoptInt(sockfd, syscall.SOL_SOCKET, syscall.SO_TYPE)
			if err != nil {
				return nil, err
			}
			// send at least one normal byte
			if sockType != syscall.SOCK_DGRAM {
				iov.Base = &dummy
				iov.SetLen(1)
			}
		}
		msg.Control = &oob[0]
		msg.SetControllen(len(oob))
	}
	msg.Iov = &iov
	msg.Iovlen = 1

	resolver := func(req Request) {
		result := req.(*request)
		result.r0 = int(result.res)
		errResolver(result)
		if result.err != nil {
			return
		}

		if len(oob) > 0 && len(p) == 0 {
			result.r0 = 0
		}
	}

	msgptr := unsafe.Pointer(msg)
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(msg, to)
		userData.request.resolver = resolver
		userData.SetRequestBuffer(p, oob)

		sqe.PrepOperation(iouring_syscall.IORING_OP_SENDMSG, int32(sockfd), uint64(uintptr(msgptr)), 1, 0)
		sqe.SetOpFlags(uint32(flags))
	}, nil
}

func Recvmsg(sockfd int, p, oob []byte, to syscall.Sockaddr, flags int) (PrepRequest, error) {
	var msg syscall.Msghdr
	var rsa syscall.RawSockaddrAny
	msg.Name = (*byte)(unsafe.Pointer(&rsa))
	msg.Namelen = uint32(syscall.SizeofSockaddrAny)
	var iov syscall.Iovec
	if len(p) > 0 {
		iov.Base = &p[0]
		iov.SetLen(len(p))
	}
	var dummy byte
	if len(oob) > 0 {
		if len(p) == 0 {
			var sockType int
			sockType, err := syscall.GetsockoptInt(sockfd, syscall.SOL_SOCKET, syscall.SO_TYPE)
			if err != nil {
				return nil, err
			}
			// receive at least one normal byte
			if sockType != syscall.SOCK_DGRAM {
				iov.Base = &dummy
				iov.SetLen(1)
			}
		}
		msg.Control = &oob[0]
		msg.SetControllen(len(oob))
	}
	msg.Iov = &iov
	msg.Iovlen = 1

	resolver := func(req Request) {
		result := req.(*request)
		result.r0 = int(result.res)
		errResolver(result)
		if result.err != nil {
			return
		}

		if len(oob) > 0 && len(p) == 0 {
			result.r0 = 0
		}
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&msg, &rsa)
		userData.request.resolver = resolver
		userData.SetRequestBuffer(p, oob)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_RECVMSG,
			int32(sockfd),
			uint64(uintptr(unsafe.Pointer(&msg))),
			1,
			0,
		)
		sqe.SetOpFlags(uint32(flags))
	}, nil
}

func Accept(sockfd int) PrepRequest {
	var rsa syscall.RawSockaddrAny
	var len uint32 = syscall.SizeofSockaddrAny

	resolver := func(req Request) {
		result := req.(*request)
		fd := int(result.res)
		errResolver(result)
		if result.err != nil {
			return
		}

		result.r0 = fd
		result.r1, result.err = anyToSockaddr(&rsa)
		if result.err != nil {
			syscall.Close(fd)
			result.r0 = 0
		}
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&len)
		userData.request.resolver = resolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_ACCEPT, int32(sockfd), uint64(uintptr(unsafe.Pointer(&rsa))), 0, uint64(uintptr(unsafe.Pointer(&len))))
	}
}

func Accept4(sockfd int, flags int) PrepRequest {
	var rsa syscall.RawSockaddrAny
	var len uint32 = syscall.SizeofSockaddrAny

	resolver := func(req Request) {
		result := req.(*request)
		fd := int(result.res)
		errResolver(result)
		if result.err != nil {
			return
		}

		if len > syscall.SizeofSockaddrAny {
			panic("RawSockaddrAny too small")
		}

		result.r0 = fd
		result.r1, result.err = anyToSockaddr(&rsa)
		if result.err != nil {
			syscall.Close(fd)
			result.r0 = 0
		}
	}
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&rsa, &len)
		userData.request.resolver = resolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_ACCEPT,
			int32(sockfd),
			uint64(uintptr(unsafe.Pointer(&rsa))),
			0,
			uint64(uintptr(unsafe.Pointer(&len))),
		)
		sqe.SetOpFlags(uint32(flags))
	}
}

func Connect(sockfd int, sa syscall.Sockaddr) (PrepRequest, error) {
	ptr, n, err := sockaddr(sa)
	if err != nil {
		return nil, err
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(sa)
		userData.request.resolver = errResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_CONNECT,
			int32(sockfd),
			uint64(uintptr(ptr)),
			0,
			uint64(n),
		)
	}, nil
}

func Openat(dirfd int, path string, flags uint32, mode uint32) (PrepRequest, error) {
	flags |= syscall.O_LARGEFILE
	b, err := syscall.ByteSliceFromString(path)
	if err != nil {
		return nil, err
	}

	bp := unsafe.Pointer(&b[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&b)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_OPENAT,
			int32(dirfd),
			uint64(uintptr(bp)),
			mode,
			0,
		)
		sqe.SetOpFlags(flags)
	}, nil
}

func Openat2(dirfd int, path string, how *unix.OpenHow) (PrepRequest, error) {
	b, err := syscall.ByteSliceFromString(path)
	if err != nil {
		return nil, err
	}

	bp := unsafe.Pointer(&b[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&b)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_OPENAT2,
			int32(dirfd),
			uint64(uintptr(bp)),
			unix.SizeofOpenHow,
			uint64(uintptr(unsafe.Pointer(how))),
		)
	}, nil
}

func Statx(dirfd int, path string, flags uint32, mask int, stat *unix.Statx_t) (PrepRequest, error) {
	b, err := syscall.ByteSliceFromString(path)
	if err != nil {
		return nil, err
	}

	bp := unsafe.Pointer(&b[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver
		userData.hold(&b, stat)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_STATX,
			int32(dirfd),
			uint64(uintptr(bp)),
			uint32(mask),
			uint64(uintptr(unsafe.Pointer(stat))),
		)
		sqe.SetOpFlags(flags)
	}, nil
}

func Fsync(fd int) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_FSYNC, int32(fd), 0, 0, 0)
	}
}

func Fdatasync(fd int) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_FSYNC, int32(fd), 0, 0, 0)
		sqe.SetOpFlags(iouring_syscall.IORING_FSYNC_DATASYNC)
	}
}

func Fallocate(fd int, mode uint32, off int64, length int64) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_FALLOCATE,
			int32(fd),
			uint64(length),
			uint32(mode),
			uint64(off),
		)
	}
}

func Close(fd int) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_CLOSE, int32(fd), 0, 0, 0)
	}
}

func Madvise(b []byte, advice int) PrepRequest {
	var bp unsafe.Pointer
	if len(b) > 0 {
		bp = unsafe.Pointer(&b[0])
	} else {
		bp = unsafe.Pointer(&_zero)
	}
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver
		userData.SetRequestBuffer(b, nil)

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_MADVISE,
			-1,
			uint64(uintptr(bp)),
			uint32(len(b)),
			0,
		)
		sqe.SetOpFlags(uint32(advice))
	}
}

func EpollCtl(epfd int, op int, fd int, event *syscall.EpollEvent) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = errResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_EPOLL_CTL,
			int32(epfd),
			uint64(uintptr(unsafe.Pointer(event))),
			uint32(op),
			uint64(fd),
		)
	}
}

func Mkdirat(dirFd int, path string, mode uint32) (PrepRequest, error) {
	b, err := syscall.ByteSliceFromString(path)
	if err != nil {
		return nil, err
	}

	bp := unsafe.Pointer(&b[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&b)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_MKDIRAT,
			int32(dirFd),
			uint64(uintptr(bp)),
			mode,
			0,
		)
	}, nil
}

func Unlinkat(fd int, path string, flags int32) (PrepRequest, error) {
	b, err := syscall.ByteSliceFromString(path)
	if err != nil {
		return nil, err
	}

	bp := unsafe.Pointer(&b[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&b)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_UNLINKAT,
			int32(fd),
			uint64(uintptr(bp)),
			0,
			0,
		)
		sqe.SetOpFlags(uint32(flags))
	}, nil
}

func Symlinkat(target string, newDirFd int, linkPath string) (PrepRequest, error) {
	bTarget, err := syscall.ByteSliceFromString(target)
	if err != nil {
		return nil, err
	}
	bLink, err := syscall.ByteSliceFromString(linkPath)
	if err != nil {
		return nil, err
	}

	bpTarget := unsafe.Pointer(&bTarget[0])
	bpLink := unsafe.Pointer(&bLink[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&bTarget, &bLink)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_SYMLINKAT,
			int32(newDirFd),
			uint64(uintptr(bpTarget)),
			0,
			uint64(uintptr(bpLink)),
		)
	}, nil
}

func Renameat2(oldDirFd int, oldPath string, newDirFd int, newPath string, flags int) (PrepRequest, error) {
	bOldPath, err := syscall.ByteSliceFromString(oldPath)
	if err != nil {
		return nil, err
	}
	bNewPath, err := syscall.ByteSliceFromString(newPath)
	if err != nil {
		return nil, err
	}

	bpOldPath := unsafe.Pointer(&bOldPath[0])
	bpNewPath := unsafe.Pointer(&bNewPath[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&bOldPath, &bNewPath)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_RENAMEAT,
			int32(oldDirFd), uint64(uintptr(bpOldPath)),
			uint32(newDirFd), uint64(uintptr(bpNewPath)),
		)
		sqe.SetOpFlags(uint32(flags))
	}, nil
}

func Renameat(oldDirFd int, oldPath string, newDirFd int, newPath string) (PrepRequest, error) {
	return Renameat2(oldDirFd, oldPath, newDirFd, newPath, 0)
}

func Linkat(targetDirFd int, targetPath string, linkDirFd int, linkPath string, flags int) (PrepRequest, error) {
	bTargetPath, err := syscall.ByteSliceFromString(targetPath)
	if err != nil {
		return nil, err
	}
	bLinkPath, err := syscall.ByteSliceFromString(linkPath)
	if err != nil {
		return nil, err
	}

	bpTargetPath := unsafe.Pointer(&bTargetPath[0])
	bpLinkPath := unsafe.Pointer(&bLinkPath[0])
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&bpTargetPath, &bpLinkPath)
		userData.request.resolver = fdResolver

		sqe.PrepOperation(
			iouring_syscall.IORING_OP_LINKAT,
			int32(targetDirFd),
			uint64(uintptr(bpTargetPath)),
			uint32(linkDirFd),
			uint64(uintptr(bpLinkPath)),
		)
		sqe.SetOpFlags(uint32(flags))
	}, nil
}
