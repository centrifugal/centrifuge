// +build linux

package iouring

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const initEpollEvents = 1

type iourPoller struct {
	sync.Mutex

	fd     int
	iours  map[int]*IOURing
	events []unix.EpollEvent
}

var (
	poller         *iourPoller
	initpollerLock sync.Mutex
)

func initpoller() error {
	// fast path
	if poller != nil {
		return nil
	}

	initpollerLock.Lock()
	defer initpollerLock.Unlock()
	if poller != nil {
		return nil
	}

	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return os.NewSyscallError("epoll_create1", err)
	}

	poller = &iourPoller{
		fd:     epfd,
		iours:  make(map[int]*IOURing),
		events: make([]unix.EpollEvent, initEpollEvents),
	}

	go poller.run()
	return nil
}

func registerIOURing(iour *IOURing) error {
	if err := initpoller(); err != nil {
		return err
	}

	if err := unix.EpollCtl(poller.fd, unix.EPOLL_CTL_ADD, iour.eventfd,
		&unix.EpollEvent{Fd: int32(iour.eventfd), Events: unix.EPOLLIN | unix.EPOLLET},
	); err != nil {
		return os.NewSyscallError("epoll_ctl_add", err)
	}

	poller.Lock()
	poller.iours[iour.eventfd] = iour
	poller.Unlock()
	return nil
}

func removeIOURing(iour *IOURing) error {
	poller.Lock()
	delete(poller.iours, iour.eventfd)
	poller.Unlock()

	return os.NewSyscallError("epoll_ctl_del",
		unix.EpollCtl(poller.fd, unix.EPOLL_CTL_DEL, iour.eventfd, nil))
}

func (poller *iourPoller) run() {
	for {
		n, err := unix.EpollWait(poller.fd, poller.events, -1)
		if err != nil {
			continue
		}

		for i := 0; i < n; i++ {
			fd := int(poller.events[i].Fd)
			poller.Lock()
			iour, ok := poller.iours[fd]
			poller.Unlock()
			if !ok {
				continue
			}

			select {
			case iour.cqeSign <- struct{}{}:
			default:
			}
		}

		poller.adjust()
	}
}

func (poller *iourPoller) adjust() {
	poller.Lock()
	l := len(poller.iours) - len(poller.events)
	poller.Unlock()

	if l <= 0 {
		return
	}

	events := make([]unix.EpollEvent, l*2)
	poller.events = append(poller.events, events...)
}
