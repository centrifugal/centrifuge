//go:build linux
// +build linux

package iouring

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

type ResultResolver func(req Request)

type RequestCallback func(result Result) error

type Request interface {
	Result

	Cancel() (Request, error)
	Done() <-chan struct{}

	GetRes() (int, error)
	// Can Only be used in ResultResolver
	SetResult(r0, r1 interface{}, err error) error
}

type Result interface {
	Fd() int
	Opcode() uint8
	GetRequestBuffer() (b0, b1 []byte)
	GetRequestBuffers() [][]byte
	GetRequestInfo() interface{}
	FreeRequestBuffer()

	Err() error
	ReturnValue0() interface{}
	ReturnValue1() interface{}
	ReturnExtra1() uint64
	ReturnExtra2() uint64
	ReturnFd() (int, error)
	ReturnInt() (int, error)

	Callback() error
}

var _ Request = &request{}

type request struct {
	iour *IOURing

	id     uint64
	opcode uint8
	res    int32

	once      sync.Once
	resolving bool
	resolver  ResultResolver

	callback RequestCallback

	fd int
	b0 []byte
	b1 []byte
	bs [][]byte

	err  error
	r0   interface{}
	r1   interface{}
	ext1 uint64
	ext2 uint64

	requestInfo interface{}

	set  *requestSet
	done chan struct{}
}

func (req *request) resolve() {
	if req.resolver == nil {
		return
	}

	select {
	case <-req.done:
	default:
		return
	}

	req.once.Do(func() {
		req.resolving = true
		req.resolver(req)
		req.resolving = false

		req.resolver = nil
	})
}

func (req *request) complate(cqe iouring_syscall.CompletionQueueEvent) {
	req.res = cqe.Result()
	req.ext1 = cqe.Extra1()
	req.ext2 = cqe.Extra2()
	req.iour = nil
	close(req.done)

	if req.set != nil {
		req.set.complateOne()
		req.set = nil
	}
}

func (req *request) isDone() bool {
	select {
	case <-req.done:
		return true
	default:
	}
	return false
}

func (req *request) Callback() error {
	if !req.isDone() {
		return ErrRequestCompleted
	}

	if req.callback == nil {
		return ErrNoRequestCallback
	}

	return req.callback(req)
}

// Cancel request if request is not completed
func (req *request) Cancel() (Request, error) {
	if req.isDone() {
		return nil, ErrRequestCompleted
	}

	return req.iour.submitCancel(req.id)
}

func (req *request) Done() <-chan struct{} {
	return req.done
}

func (req *request) Opcode() uint8 {
	return req.opcode
}

func (req *request) Fd() int {
	return req.fd
}

func (req *request) GetRequestBuffer() (b0, b1 []byte) {
	return req.b0, req.b1
}

func (req *request) GetRequestBuffers() [][]byte {
	return req.bs
}

func (req *request) GetRequestInfo() interface{} {
	return req.requestInfo
}

func (req *request) Err() error {
	req.resolve()
	return req.err
}

func (req *request) ReturnValue0() interface{} {
	req.resolve()
	return req.r0
}

func (req *request) ReturnValue1() interface{} {
	req.resolve()
	return req.r1
}

func (req *request) ReturnExtra1() uint64 {
	return req.ext1
}

func (req *request) ReturnExtra2() uint64 {
	return req.ext2
}

func (req *request) ReturnFd() (int, error) {
	return req.ReturnInt()
}

func (req *request) ReturnInt() (int, error) {
	req.resolve()

	if req.err != nil {
		return -1, req.err
	}

	fd, ok := req.r0.(int)
	if !ok {
		return -1, errors.New("req value is not int")
	}

	return fd, nil
}

func (req *request) FreeRequestBuffer() {
	req.b0 = nil
	req.b1 = nil
	req.bs = nil
}

func (req *request) GetRes() (int, error) {
	if !req.isDone() {
		return 0, ErrRequestNotCompleted
	}

	return int(req.res), nil
}

func (req *request) SetResult(r0, r1 interface{}, err error) error {
	if !req.isDone() {
		return ErrRequestNotCompleted
	}
	if !req.resolving {
		return errors.New("request is not resolving")
	}

	req.r0, req.r1, req.err = r0, r1, err
	return nil
}

type RequestSet interface {
	Len() int
	Done() <-chan struct{}
	Requests() []Request
	ErrResults() []Result
}

var _ RequestSet = &requestSet{}

type requestSet struct {
	requests []Request

	complates int32
	done      chan struct{}
}

func newRequestSet(userData []*UserData) *requestSet {
	set := &requestSet{
		requests: make([]Request, len(userData)),
		done:     make(chan struct{}),
	}

	for i, data := range userData {
		set.requests[i] = data.request
		data.request.set = set
	}
	return set
}

func (set *requestSet) complateOne() {
	if atomic.AddInt32(&set.complates, 1) == int32(len(set.requests)) {
		close(set.done)
	}
}

func (set *requestSet) Len() int {
	return len(set.requests)
}

func (set *requestSet) Done() <-chan struct{} {
	return set.done
}

func (set *requestSet) Requests() []Request {
	return set.requests
}

func (set *requestSet) ErrResults() (results []Result) {
	for _, req := range set.requests {
		if req.Err() != nil {
			results = append(results, req)
		}
	}
	return
}

func errResolver(req Request) {
	result := req.(*request)
	if result.res < 0 {
		result.err = syscall.Errno(-result.res)
		if result.err == syscall.ECANCELED {
			// request is canceled
			result.err = ErrRequestCanceled
		}
	}
}

func fdResolver(req Request) {
	result := req.(*request)
	if errResolver(result); result.err != nil {
		return
	}
	result.r0 = int(result.res)
}

func timeoutResolver(req Request) {
	result := req.(*request)
	if errResolver(result); result.err != nil {
		// if timeout got completed through expiration of the timer
		// result.res is -ETIME and result.err is syscall.ETIME
		if result.err == syscall.ETIME {
			result.err = nil
			result.r0 = TimeoutExpiration
		}
		return
	}

	// if timeout got completed through requests completing
	// result.res is 0
	if result.res == 0 {
		result.r0 = CountCompletion
	}
}

func removeTimeoutResolver(req Request) {
	result := req.(*request)
	if errResolver(result); result.err != nil {
		switch result.err {
		case syscall.EBUSY:
			// timeout request was found bu expiration was already in progress
			result.err = ErrRequestCompleted
		case syscall.ENOENT:
			// timeout request not found
			result.err = ErrRequestNotFound
		}
		return
	}

	// timeout request is found and cacelled successfully
	// result.res value is 0
}

func cancelResolver(req Request) {
	result := req.(*request)
	if errResolver(result); result.err != nil {
		switch result.err {
		case syscall.ENOENT:
			result.err = ErrRequestNotFound
		case syscall.EALREADY:
			result.err = nil
			result.r0 = RequestMaybeCanceled
		}
		return
	}

	if result.res == 0 {
		result.r0 = RequestCanceledSuccessfully
	}
}
