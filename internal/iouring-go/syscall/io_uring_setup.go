//go:build linux
// +build linux

package iouring_syscall

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	IORING_SETUP_IOPOLL uint32 = 1 << iota
	IORING_SETUP_SQPOLL
	IORING_SETUP_SQ_AFF
	IORING_SETUP_CQSIZE
	IORING_SETUP_CLAMP
	IORING_SETUP_ATTACH_WQ
	IORING_SETUP_R_DISABLED
	IORING_SETUP_SUBMIT_ALL

	IORING_SETUP_COOP_TASKRUN

	IORING_SETUP_TASKRUN_FLAG
	IORING_SETUP_SQE128
	IORING_SETUP_CQE32
)

// io_uring features supported by current kernel version
const (
	IORING_FEAT_SINGLE_MMAP uint32 = 1 << iota
	IORING_FEAT_NODROP
	IORING_FEAT_SUBMIT_STABLE
	IORING_FEAT_RW_CUR_POS
	IORING_FEAT_CUR_PERSONALITY
	IORING_FEAT_FAST_POLL
	IORING_FEAT_POLL_32BITS
	IORING_FEAT_SQPOLL_NONFIXED
)

// IOURingParams the flags, sq_thread_cpu, sq_thread_idle and WQFd fields are used to configure the io_uring instance
type IOURingParams struct {
	SQEntries    uint32 // specifies the number of submission queue entries allocated
	CQEntries    uint32 // when IORING_SETUP_CQSIZE flag is specified
	Flags        uint32 // a bit mast of 0 or more of the IORING_SETUP_*
	SQThreadCPU  uint32 // when IORING_SETUP_SQPOLL and IORING_SETUP_SQ_AFF flags are specified
	SQThreadIdle uint32 // when IORING_SETUP_SQPOLL flag is specified
	Features     uint32
	WQFd         uint32 // when IORING_SETUP_ATTACH_WQ flag is specified
	Resv         [3]uint32

	SQOffset SubmissionQueueRingOffset
	CQOffset CompletionQueueRingOffset
}

// SubmissionQueueRingOffset describes the offsets of various ring buffer fields
type SubmissionQueueRingOffset struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	Resv2       uint64
}

// CompletionQueueRingOffset describes the offsets of various ring buffer fields
type CompletionQueueRingOffset struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	Resv1       uint32
	Resv2       uint64
}

func IOURingSetup(entries uint, params *IOURingParams) (int, error) {
	res, _, errno := syscall.RawSyscall(
		SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(params)),
		0,
	)
	if errno != 0 {
		return int(res), os.NewSyscallError("iouring_setup", errno)
	}

	return int(res), nil
}
