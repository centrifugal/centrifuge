//go:build linux
// +build linux

package iouring

import (
	"time"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

type IOURingOption func(*IOURing)

// WithSQPoll a kernel thread is created to perform submission queue polling
// In Version 5.10 and later, allow using this as non-root,
// if the user has the CAP_SYS_NICE capability
func WithSQPoll() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_SQPOLL
	}
}

// WithSQPollThreadCPU the poll thread will be bound to the cpu set, only meaningful when WithSQPoll option
func WithSQPollThreadCPU(cpu uint32) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_SQ_AFF
		iour.params.SQThreadCPU = cpu
	}
}

func WithSQPollThreadIdle(idle time.Duration) IOURingOption {
	return func(iour *IOURing) {
		iour.params.SQThreadIdle = uint32(idle / time.Millisecond)
	}
}

// WithParams use params
func WithParams(params *iouring_syscall.IOURingParams) IOURingOption {
	return func(iour *IOURing) {
		iour.params = params
	}
}

// WithCQSize create the completion queue with size entries
// size must bue greater than entries
func WithCQSize(size uint32) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_CQSIZE
		iour.params.CQEntries = size
	}
}

// WithAttachWQ new iouring instance being create will share the asynchronous worker thread
// backend of the specified io_uring ring, rather than create a new separate thread pool
func WithAttachWQ(iour *IOURing) IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_ATTACH_WQ
		iour.params.WQFd = uint32(iour.fd)
	}
}

func WithAsync() IOURingOption {
	return func(iour *IOURing) {
		iour.async = true
	}
}

// WithDisableRing the io_uring ring starts in a disabled state
// In this state, restrictions can be registered, but submissions are not allowed
// Available since 5.10
func WithDisableRing() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_R_DISABLED
	}
}

// WithDrain every SQE will not be started before previously submitted SQEs have completed
func WithDrain() IOURingOption {
	return func(iour *IOURing) {
		iour.drain = true
	}
}

// WithSQE128 every SQE will have 128B entry size to append IOCTL command
func WithSQE128() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_SQE128
	}
}

// WithCQE32 every CQE will have 32B entry size to append IOCTL return data
func WithCQE32() IOURingOption {
	return func(iour *IOURing) {
		iour.params.Flags |= iouring_syscall.IORING_SETUP_CQE32
	}
}
