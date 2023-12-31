//go:build linux
// +build linux

package iouring

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"

	iouring_syscall "github.com/iceber/iouring-go/syscall"
)

const uint32Size = uint32(unsafe.Sizeof(uint32(0)))

func mmapIOURing(iour *IOURing) (err error) {
	defer func() {
		if err != nil {
			munmapIOURing(iour)
		}
	}()
	iour.sq = new(SubmissionQueue)
	iour.cq = new(CompletionQueue)

	if err = mmapSQ(iour); err != nil {
		return err
	}

	if (iour.params.Features & iouring_syscall.IORING_FEAT_SINGLE_MMAP) != 0 {
		iour.cq.ptr = iour.sq.ptr
	}

	if err = mmapCQ(iour); err != nil {
		return err
	}

	if err = mmapSQEs(iour); err != nil {
		return err
	}
	return nil
}

func mmapSQ(iour *IOURing) (err error) {
	sq := iour.sq
	params := iour.params

	sq.size = params.SQOffset.Array + params.SQEntries*uint32Size
	sq.ptr, err = mmap(iour.fd, sq.size, iouring_syscall.IORING_OFF_SQ_RING)
	if err != nil {
		return fmt.Errorf("mmap sq ring: %w", err)
	}

	sq.head = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.Head)))
	sq.tail = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.Tail)))
	sq.mask = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.RingMask)))
	sq.entries = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.RingEntries)))
	sq.flags = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.Flags)))
	sq.dropped = (*uint32)(unsafe.Pointer(sq.ptr + uintptr(params.SQOffset.Dropped)))

	sq.array = *(*[]uint32)(unsafe.Pointer(&reflect.SliceHeader{
		Data: sq.ptr + uintptr(params.SQOffset.Array),
		Len:  int(params.SQEntries),
		Cap:  int(params.SQEntries),
	}))

	return nil
}

func mmapCQ(iour *IOURing) (err error) {
	params := iour.params
	cq := iour.cq

	cqes := makeCompletionQueueRing(params.Flags)

	cq.size = params.CQOffset.Cqes + params.CQEntries*cqes.entrySz()
	if cq.ptr == 0 {
		cq.ptr, err = mmap(iour.fd, cq.size, iouring_syscall.IORING_OFF_CQ_RING)
		if err != nil {
			return fmt.Errorf("mmap cq ring: %w", err)
		}
	}

	cq.head = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.Head)))
	cq.tail = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.Tail)))
	cq.mask = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.RingMask)))
	cq.entries = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.RingEntries)))
	cq.flags = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.Flags)))
	cq.overflow = (*uint32)(unsafe.Pointer(cq.ptr + uintptr(params.CQOffset.Overflow)))

	cqes.assignQueue(cq.ptr+uintptr(params.CQOffset.Cqes), int(params.CQEntries))
	cq.cqes = cqes

	return nil
}

func mmapSQEs(iour *IOURing) error {
	params := iour.params

	sqes := makeSubmissionQueueRing(params.Flags)

	ptr, err := mmap(iour.fd, params.SQEntries*sqes.entrySz(), iouring_syscall.IORING_OFF_SQES)
	if err != nil {
		return fmt.Errorf("mmap sqe array: %w", err)
	}

	sqes.assignQueue(ptr, int(params.SQEntries))
	iour.sq.sqes = sqes

	return nil
}

func munmapIOURing(iour *IOURing) error {
	if iour.sq != nil && iour.sq.ptr != 0 {
		if iour.sq.sqes.isActive() {
			err := munmap(iour.sq.sqes.mappedPtr(), iour.sq.sqes.ringSz())
			if err != nil {
				return fmt.Errorf("ummap sqe array: %w", err)
			}
			iour.sq.sqes = nil
		}

		if err := munmap(iour.sq.ptr, iour.sq.size); err != nil {
			return fmt.Errorf("munmap sq: %w", err)
		}
		if iour.sq.ptr == iour.cq.ptr {
			iour.cq = nil
		}
		iour.sq = nil
	}

	if iour.cq != nil && iour.cq.ptr != 0 {
		if err := munmap(iour.cq.ptr, iour.cq.size); err != nil {
			return fmt.Errorf("munmap cq: %w", err)
		}
		iour.cq = nil
	}

	return nil
}

func mmap(fd int, length uint32, offset uint64) (uintptr, error) {
	ptr, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP,
		0,
		uintptr(length),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
		uintptr(fd),
		uintptr(offset),
	)
	if errno != 0 {
		return 0, os.NewSyscallError("mmap", errno)
	}
	return uintptr(ptr), nil
}

func munmap(ptr uintptr, length uint32) error {
	_, _, errno := syscall.Syscall(
		syscall.SYS_MUNMAP,
		ptr,
		uintptr(length),
		0,
	)
	if errno != 0 {
		return os.NewSyscallError("munmap", errno)
	}
	return nil
}
