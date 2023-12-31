//go:build linux
// +build linux

package iouring

import (
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

func (prepReq PrepRequest) WithTimeout(timeout time.Duration) []PrepRequest {
	linkRequest := func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		prepReq(sqe, userData)
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_IO_LINK)
	}
	return []PrepRequest{linkRequest, linkTimeout(timeout)}
}

func Timeout(t time.Duration) PrepRequest {
	timespec := unix.NsecToTimespec(t.Nanoseconds())

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&timespec)
		userData.request.resolver = timeoutResolver

		sqe.PrepOperation(iouring_syscall.IORING_OP_TIMEOUT, -1, uint64(uintptr(unsafe.Pointer(&timespec))), 1, 0)
	}
}

func TimeoutWithTime(t time.Time) (PrepRequest, error) {
	timespec, err := unix.TimeToTimespec(t)
	if err != nil {
		return nil, err
	}

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.hold(&timespec)
		userData.request.resolver = timeoutResolver

		sqe.PrepOperation(iouring_syscall.IORING_OP_TIMEOUT, -1, uint64(uintptr(unsafe.Pointer(&timespec))), 1, 0)
		sqe.SetOpFlags(iouring_syscall.IORING_TIMEOUT_ABS)
	}, nil
}

func CountCompletionEvent(n uint64) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = timeoutResolver

		sqe.PrepOperation(iouring_syscall.IORING_OP_TIMEOUT, -1, 0, 0, n)
	}
}

func RemoveTimeout(id uint64) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = removeTimeoutResolver

		sqe.PrepOperation(iouring_syscall.IORING_OP_TIMEOUT, -1, id, 0, 0)
	}
}
