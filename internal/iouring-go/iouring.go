//go:build linux
// +build linux

package iouring

import (
	"errors"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

// IOURing contains iouring_syscall submission and completion queue.
// It's safe for concurrent use by multiple goroutines.
type IOURing struct {
	params *iouring_syscall.IOURingParams
	fd     int

	eventfd int
	cqeSign chan struct{}

	sq *SubmissionQueue
	cq *CompletionQueue

	async    bool
	drain    bool
	Flags    uint32
	Features uint32

	submitLock sync.Mutex

	userDataLock sync.RWMutex
	userDatas    map[uint64]*UserData

	fileRegister FileRegister

	fdclosed bool
	closer   chan struct{}
	closed   chan struct{}
}

// New return a IOURing instance by IOURingOptions
func New(entries uint, opts ...IOURingOption) (*IOURing, error) {
	iour := &IOURing{
		params:    &iouring_syscall.IOURingParams{},
		userDatas: make(map[uint64]*UserData),
		cqeSign:   make(chan struct{}, 1),
		closer:    make(chan struct{}),
		closed:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(iour)
	}

	var err error
	iour.fd, err = iouring_syscall.IOURingSetup(entries, iour.params)
	if err != nil {
		return nil, err
	}

	if err := mmapIOURing(iour); err != nil {
		munmapIOURing(iour)
		return nil, err
	}

	iour.fileRegister = &fileRegister{
		iouringFd:    iour.fd,
		sparseIndexs: make(map[int]int),
	}
	iour.Flags = iour.params.Flags
	iour.Features = iour.params.Features

	if err := iour.registerEventfd(); err != nil {
		iour.Close()
		return nil, err
	}

	if err := registerIOURing(iour); err != nil {
		iour.Close()
		return nil, err
	}

	go iour.run()
	return iour, nil
}

// Size iouring submission queue size
func (iour *IOURing) Size() int {
	return int(*iour.sq.entries)
}

// Close IOURing
func (iour *IOURing) Close() error {
	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	select {
	case <-iour.closer:
	default:
		close(iour.closer)
	}

	if iour.eventfd > 0 {
		if err := removeIOURing(iour); err != nil {
			return err
		}
		syscall.Close(iour.eventfd)
		iour.eventfd = -1
	}

	<-iour.closed

	if err := munmapIOURing(iour); err != nil {
		return err
	}

	if !iour.fdclosed {
		if err := syscall.Close(iour.fd); err != nil {
			return os.NewSyscallError("close", err)
		}
		iour.fdclosed = true
	}

	return nil
}

// IsClosed IOURing is closed
func (iour *IOURing) IsClosed() (closed bool) {
	select {
	case <-iour.closer:
		closed = true
	default:
	}
	return
}

func (iour *IOURing) getSQEntry() iouring_syscall.SubmissionQueueEntry {
	for {
		sqe := iour.sq.getSQEntry()
		if sqe != nil {
			return sqe
		}
		runtime.Gosched()
	}
}

func (iour *IOURing) doRequest(sqe iouring_syscall.SubmissionQueueEntry, request PrepRequest, ch chan<- Result) (*UserData, error) {
	userData := makeUserData(iour, ch)

	request(sqe, userData)
	userData.setOpcode(sqe.Opcode())

	sqe.SetUserData(userData.id)

	userData.request.fd = int(sqe.Fd())
	if sqe.Fd() >= 0 {
		if index, ok := iour.fileRegister.GetFileIndex(int32(sqe.Fd())); ok {
			sqe.SetFdIndex(int32(index))
		} else if iour.Flags&iouring_syscall.IORING_SETUP_SQPOLL != 0 &&
			iour.Features&iouring_syscall.IORING_FEAT_SQPOLL_NONFIXED == 0 {
			/*
				Before version 5.10 of the Linux kernel, to successful use SQPoll, the application
				must register a set of files to be used for IO through iour.RegisterFiles.
				Failure to do so will result in submitted IO being errored with EBADF

				The presence of this feature can be detected by the IORING_FEAT_SQPOLL_NONFIXED
				In Version 5.10 and later, it is no longer necessary to register files to use SQPoll
			*/

			return nil, ErrUnregisteredFile
		}
	}

	if iour.async {
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_ASYNC)
	}
	if iour.drain {
		sqe.SetFlags(iouring_syscall.IOSQE_FLAGS_IO_DRAIN)
	}
	return userData, nil
}

// SubmitRequest by Request function and io result is notified via channel
// return request id, can be used to cancel a request
func (iour *IOURing) SubmitRequest(request PrepRequest, ch chan<- Result) (Request, error) {
	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	if iour.IsClosed() {
		return nil, ErrIOURingClosed
	}

	sqe := iour.getSQEntry()
	userData, err := iour.doRequest(sqe, request, ch)
	if err != nil {
		iour.sq.fallback(1)
		return nil, err
	}

	iour.userDataLock.Lock()
	iour.userDatas[userData.id] = userData
	iour.userDataLock.Unlock()

	if _, err = iour.submit(); err != nil {
		iour.userDataLock.Lock()
		delete(iour.userDatas, userData.id)
		iour.userDataLock.Unlock()
		return nil, err
	}

	return userData.request, nil
}

// SubmitRequests by Request functions and io results are notified via channel
func (iour *IOURing) SubmitRequests(requests []PrepRequest, ch chan<- Result) (RequestSet, error) {
	// TODO(iceber): no length limit
	if len(requests) > int(*iour.sq.entries) {
		return nil, errors.New("too many requests")
	}

	iour.submitLock.Lock()
	defer iour.submitLock.Unlock()

	if iour.IsClosed() {
		return nil, ErrIOURingClosed
	}

	var sqeN uint32
	userDatas := make([]*UserData, 0, len(requests))
	for _, request := range requests {
		sqe := iour.getSQEntry()
		sqeN++

		userData, err := iour.doRequest(sqe, request, ch)
		if err != nil {
			iour.sq.fallback(sqeN)
			return nil, err
		}
		userDatas = append(userDatas, userData)
	}

	// must be located before the lock operation to
	// avoid the compiler's adjustment of the code order.
	// issue: https://github.com/Iceber/iouring-go/issues/8
	rset := newRequestSet(userDatas)

	iour.userDataLock.Lock()
	for _, data := range userDatas {
		iour.userDatas[data.id] = data
	}
	iour.userDataLock.Unlock()

	if _, err := iour.submit(); err != nil {
		iour.userDataLock.Lock()
		for _, data := range userDatas {
			delete(iour.userDatas, data.id)
		}
		iour.userDataLock.Unlock()

		return nil, err
	}

	return rset, nil
}

func (iour *IOURing) needEnter(flags *uint32) bool {
	if (iour.Flags & iouring_syscall.IORING_SETUP_SQPOLL) == 0 {
		return true
	}

	if iour.sq.needWakeup() {
		*flags |= iouring_syscall.IORING_SQ_NEED_WAKEUP
		return true
	}
	return false
}

func (iour *IOURing) submit() (submitted int, err error) {
	submitted = iour.sq.flush()

	var flags uint32
	if !iour.needEnter(&flags) || submitted == 0 {
		return
	}

	if (iour.Flags & iouring_syscall.IORING_SETUP_IOPOLL) != 0 {
		flags |= iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
	}

	submitted, err = iouring_syscall.IOURingEnter(iour.fd, uint32(submitted), 0, flags, nil)
	return
}

/*
func (iour *IOURing) submitAndWait(waitCount uint32) (submitted int, err error) {
	submitted = iour.sq.flush()

	var flags uint32
	if !iour.needEnter(&flags) && waitCount == 0 {
		return
	}

	if waitCount != 0 || (iour.Flags&iouring_syscall.IORING_SETUP_IOPOLL) != 0 {
		flags |= iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS
	}

	submitted, err = iouring_syscall.IOURingEnter(iour.fd, uint32(submitted), waitCount, flags, nil)
	return
}
*/

func (iour *IOURing) getCQEvent(wait bool) (cqe iouring_syscall.CompletionQueueEvent, err error) {
	var tryPeeks int
	for {
		if cqe = iour.cq.peek(); cqe != nil {
			// Copy CQE.
			cqe = cqe.Clone()
			iour.cq.advance(1)
			return
		}

		if !wait && !iour.sq.cqOverflow() {
			err = syscall.EAGAIN
			return
		}

		if iour.sq.cqOverflow() {
			_, err = iouring_syscall.IOURingEnter(iour.fd, 0, 0, iouring_syscall.IORING_ENTER_FLAGS_GETEVENTS, nil)
			if err != nil {
				return
			}
			continue
		}

		if tryPeeks++; tryPeeks < 3 {
			runtime.Gosched()
			continue
		}

		select {
		case <-iour.cqeSign:
		case <-iour.closer:
			return nil, ErrIOURingClosed
		}
	}
}

func (iour *IOURing) run() {
	for {
		cqe, err := iour.getCQEvent(true)
		if cqe == nil || err != nil {
			if err == ErrIOURingClosed {
				close(iour.closed)
				return
			}
			log.Println("runComplete error: ", err)
			continue
		}

		// log.Println("cqe user data", (cqe.UserData))

		iour.userDataLock.Lock()
		userData := iour.userDatas[cqe.UserData()]
		if userData == nil {
			iour.userDataLock.Unlock()
			log.Println("runComplete: notfound user data ", uintptr(cqe.UserData()))
			continue
		}
		delete(iour.userDatas, cqe.UserData())
		iour.userDataLock.Unlock()

		userData.request.complate(cqe)

		// ignore link timeout
		if userData.opcode == iouring_syscall.IORING_OP_LINK_TIMEOUT {
			continue
		}

		if userData.resulter != nil {
			userData.resulter <- userData.request
		}
	}
}

// Result submit cancel request
func (iour *IOURing) submitCancel(id uint64) (Request, error) {
	if iour == nil {
		return nil, ErrRequestCompleted
	}

	return iour.SubmitRequest(cancelRequest(id), nil)
}

func cancelRequest(id uint64) PrepRequest {
	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *UserData) {
		userData.request.resolver = cancelResolver
		sqe.PrepOperation(iouring_syscall.IORING_OP_ASYNC_CANCEL, -1, id, 0, 0)
	}
}
