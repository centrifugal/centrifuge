//go:build linux
// +build linux

package iouring

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

// iouring operations
const (
	OpNop uint8 = iota
	OpReadv
	OpWritev
	OpFsync
	OpReadFixed
	OpWriteFixed
	OpPollAdd
	OpPollRemove
	OpSyncFileRange
	OpSendmsg
	OpRecvmsg
	OpTimeout
	OpTimeoutRemove
	OpAccept
	OpAsyncCancel
	OpLinkTimeout
	OpConnect
	OpFallocate
	OpOpenat
	OpClose
	OpFilesUpdate
	OpStatx
	OpRead
	OpWrite
	OpFadvise
	OpMadvise
	OpSend
	OpRecv
	OpOpenat2
	OpEpollCtl
	OpSplice
	OpProvideBuffers
	OpRemoveBuffers
	OpTee
	OpShutdown
	OpRenameat
	OpUnlinkat
	OpMkdirat
	OpSymlinkat
	OpLinkat
)

// cancel operation return value
const (
	RequestCanceledSuccessfully = 0
	RequestMaybeCanceled        = 1
)

// timeout operation return value
const (
	TimeoutExpiration = 0
	CountCompletion   = 1
)

var _zero uintptr

type SubmissionQueueRing interface {
	isActive() bool
	entrySz() uint32
	ringSz() uint32
	assignQueue(ptr uintptr, len int)
	mappedPtr() uintptr
	index(index uint32) iouring_syscall.SubmissionQueueEntry
}

func makeSubmissionQueueRing(flags uint32) SubmissionQueueRing {
	if flags&iouring_syscall.IORING_SETUP_SQE128 == 0 {
		return new(SubmissionQueueRing64)
	} else {
		return new(SubmissionQueueRing128)
	}
}

type SubmissionQueueRing64 struct {
	queue []iouring_syscall.SubmissionQueueEntry64
}

func (ring *SubmissionQueueRing64) isActive() bool {
	return ring.queue != nil && len(ring.queue) > 0
}

func (ring *SubmissionQueueRing64) entrySz() uint32 {
	return uint32(unsafe.Sizeof(iouring_syscall.SubmissionQueueEntry64{}))
}

func (ring *SubmissionQueueRing64) ringSz() uint32 {
	return uint32(len(ring.queue)) * ring.entrySz()
}

func (ring *SubmissionQueueRing64) assignQueue(ptr uintptr, len int) {
	ring.queue = *(*[]iouring_syscall.SubmissionQueueEntry64)(
		unsafe.Pointer(&reflect.SliceHeader{
			Data: ptr,
			Len:  len,
			Cap:  len,
		}))
}

func (ring *SubmissionQueueRing64) mappedPtr() uintptr {
	return uintptr(unsafe.Pointer(&ring.queue[0]))
}

func (ring *SubmissionQueueRing64) index(index uint32) iouring_syscall.SubmissionQueueEntry {
	return &ring.queue[index]
}

type SubmissionQueueRing128 struct {
	queue []iouring_syscall.SubmissionQueueEntry128
}

func (ring *SubmissionQueueRing128) isActive() bool {
	return ring.queue != nil && len(ring.queue) > 0
}

func (ring *SubmissionQueueRing128) entrySz() uint32 {
	return uint32(unsafe.Sizeof(iouring_syscall.SubmissionQueueEntry128{}))
}

func (ring *SubmissionQueueRing128) ringSz() uint32 {
	return uint32(len(ring.queue)) * ring.entrySz()
}

func (ring *SubmissionQueueRing128) assignQueue(ptr uintptr, len int) {
	ring.queue = *(*[]iouring_syscall.SubmissionQueueEntry128)(
		unsafe.Pointer(&reflect.SliceHeader{
			Data: ptr,
			Len:  len,
			Cap:  len,
		}))
}

func (ring *SubmissionQueueRing128) mappedPtr() uintptr {
	return uintptr(unsafe.Pointer(&ring.queue[0]))
}

func (ring *SubmissionQueueRing128) index(index uint32) iouring_syscall.SubmissionQueueEntry {
	return &ring.queue[index]
}

type SubmissionQueue struct {
	ptr  uintptr
	size uint32

	head    *uint32
	tail    *uint32
	mask    *uint32
	entries *uint32 // specifies the number of submission queue ring entries
	flags   *uint32 // used by the kernel to communicate stat information to the application
	dropped *uint32 // incrementd for each invalid submission queue entry encountered in the ring buffer

	array []uint32
	sqes  SubmissionQueueRing // submission queue ring

	sqeHead uint32
	sqeTail uint32
}

func (queue *SubmissionQueue) getSQEntry() iouring_syscall.SubmissionQueueEntry {
	head := atomic.LoadUint32(queue.head)
	next := queue.sqeTail + 1

	if (next - head) <= *queue.entries {
		sqe := queue.sqes.index(queue.sqeTail & *queue.mask)
		queue.sqeTail = next
		sqe.Reset()
		return sqe
	}
	return nil
}

func (queue *SubmissionQueue) fallback(i uint32) {
	queue.sqeTail -= i
}

func (queue *SubmissionQueue) cqOverflow() bool {
	return (atomic.LoadUint32(queue.flags) & iouring_syscall.IORING_SQ_CQ_OVERFLOW) != 0
}

func (queue *SubmissionQueue) needWakeup() bool {
	return (atomic.LoadUint32(queue.flags) & iouring_syscall.IORING_SQ_NEED_WAKEUP) != 0
}

// sync internal status with kernel ring state on the SQ side
// return the number of pending items in the SQ ring, for the shared ring.
func (queue *SubmissionQueue) flush() int {
	if queue.sqeHead == queue.sqeTail {
		return int(*queue.tail - *queue.head)
	}

	tail := *queue.tail
	for toSubmit := queue.sqeTail - queue.sqeHead; toSubmit > 0; toSubmit-- {
		queue.array[tail&*queue.mask] = queue.sqeHead & *queue.mask
		tail++
		queue.sqeHead++
	}

	atomic.StoreUint32(queue.tail, tail)
	return int(tail - *queue.head)
}

type CompletionQueueRing interface {
	isActive() bool
	entrySz() uint32
	ringSz() uint32
	assignQueue(ptr uintptr, len int)
	mappedPtr() uintptr
	index(index uint32) iouring_syscall.CompletionQueueEvent
}

func makeCompletionQueueRing(flags uint32) CompletionQueueRing {
	if flags&iouring_syscall.IORING_SETUP_CQE32 == 0 {
		return new(CompletionQueueRing16)
	} else {
		return new(CompletionQueueRing32)
	}
}

type CompletionQueueRing16 struct {
	queue []iouring_syscall.CompletionQueueEvent16
}

func (ring *CompletionQueueRing16) isActive() bool {
	return ring.queue != nil && len(ring.queue) > 0
}

func (ring *CompletionQueueRing16) entrySz() uint32 {
	return uint32(unsafe.Sizeof(iouring_syscall.CompletionQueueEvent16{}))
}

func (ring *CompletionQueueRing16) ringSz() uint32 {
	return uint32(len(ring.queue)) * ring.entrySz()
}

func (ring *CompletionQueueRing16) assignQueue(ptr uintptr, len int) {
	ring.queue = *(*[]iouring_syscall.CompletionQueueEvent16)(
		unsafe.Pointer(&reflect.SliceHeader{
			Data: ptr,
			Len:  len,
			Cap:  len,
		}))
}

func (ring *CompletionQueueRing16) mappedPtr() uintptr {
	return uintptr(unsafe.Pointer(&ring.queue[0]))
}

func (ring *CompletionQueueRing16) index(index uint32) iouring_syscall.CompletionQueueEvent {
	return &ring.queue[index]
}

type CompletionQueueRing32 struct {
	queue []iouring_syscall.CompletionQueueEvent32
}

func (ring *CompletionQueueRing32) isActive() bool {
	return ring.queue != nil && len(ring.queue) > 0
}

func (ring *CompletionQueueRing32) entrySz() uint32 {
	return uint32(unsafe.Sizeof(iouring_syscall.CompletionQueueEvent32{}))
}

func (ring *CompletionQueueRing32) ringSz() uint32 {
	return uint32(len(ring.queue)) * ring.entrySz()
}

func (ring *CompletionQueueRing32) assignQueue(ptr uintptr, len int) {
	ring.queue = *(*[]iouring_syscall.CompletionQueueEvent32)(
		unsafe.Pointer(&reflect.SliceHeader{
			Data: ptr,
			Len:  len,
			Cap:  len,
		}))
}

func (ring *CompletionQueueRing32) mappedPtr() uintptr {
	return uintptr(unsafe.Pointer(&ring.queue[0]))
}

func (ring *CompletionQueueRing32) index(index uint32) iouring_syscall.CompletionQueueEvent {
	return &ring.queue[index]
}

type CompletionQueue struct {
	ptr  uintptr
	size uint32

	head     *uint32
	tail     *uint32
	mask     *uint32
	entries  *uint32
	flags    *uint32
	overflow *uint32

	cqes CompletionQueueRing
}

func (queue *CompletionQueue) peek() (cqe iouring_syscall.CompletionQueueEvent) {
	head := *queue.head
	if head != atomic.LoadUint32(queue.tail) {
		//	if head < atomic.LoadUint32(queue.tail) {
		cqe = queue.cqes.index(head & *queue.mask)
	}
	return
}

func (queue *CompletionQueue) advance(num uint32) {
	if num != 0 {
		atomic.AddUint32(queue.head, num)
	}
}
