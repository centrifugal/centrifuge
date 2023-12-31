//go:build linux
// +build linux

package iouring

import (
	"errors"
	"os"
	"sync"
	"unsafe"

	iouring_syscall "github.com/centrifugal/centrifuge/internal/iouring-go/syscall"
)

func (iour *IOURing) GetFixedFileIndex(file *os.File) (int, bool) {
	return iour.fileRegister.GetFileIndex(int32(file.Fd()))
}

func (iour *IOURing) RegisterFile(file *os.File) error {
	return iour.fileRegister.RegisterFile(int32(file.Fd()))
}

func (iour *IOURing) RegisterFiles(files []*os.File) error {
	fds := make([]int32, 0, len(files))
	for _, file := range files {
		fds = append(fds, int32(file.Fd()))
	}

	return iour.fileRegister.RegisterFiles(fds)
}

func (iour *IOURing) UnregisterFile(file *os.File) error {
	return iour.fileRegister.UnregisterFile(int32(file.Fd()))
}

func (iour *IOURing) UnregisterFiles(files []*os.File) error {
	fds := make([]int32, 0, len(files))
	for _, file := range files {
		fds = append(fds, int32(file.Fd()))
	}

	return iour.fileRegister.UnregisterFiles(fds)
}

func (iour *IOURing) FileRegister() FileRegister {
	return iour.fileRegister
}

type FileRegister interface {
	GetFileIndex(fd int32) (int, bool)
	RegisterFile(fd int32) error
	RegisterFiles(fds []int32) error
	UnregisterFile(fd int32) error
	UnregisterFiles(fds []int32) error
}

type fileRegister struct {
	lock      sync.Mutex
	iouringFd int

	fds          []int32
	sparseIndexs map[int]int

	registered bool
	indexs     sync.Map
}

func (register *fileRegister) GetFileIndex(fd int32) (int, bool) {
	if fd < 0 {
		return -1, false
	}

	i, ok := register.indexs.Load(fd)
	if !ok {
		return -1, false
	}
	return i.(int), true
}

func (register *fileRegister) register() error {
	if err := iouring_syscall.IOURingRegister(
		register.iouringFd,
		iouring_syscall.IORING_REGISTER_FILES,
		unsafe.Pointer(&register.fds[0]),
		uint32(len(register.fds)),
	); err != nil {
		return err
	}

	for i, fd := range register.fds {
		register.indexs.Store(fd, i)
	}
	register.registered = true
	return nil
}

func (register *fileRegister) unregister() error {
	return iouring_syscall.IOURingRegister(register.iouringFd, iouring_syscall.IORING_UNREGISTER_FILES, nil, 0)
}

func (register *fileRegister) RegisterFiles(fds []int32) error {
	if len(fds) == 0 {
		return errors.New("file set is empty")
	}

	vfds := make([]int32, 0, len(fds))
	for _, fd := range fds {
		if fd < 0 {
			continue
		}

		if _, ok := register.indexs.Load(fd); ok {
			continue
		}
		vfds = append(vfds, fd)
	}

	if len(vfds) == 0 {
		return nil
	}
	fds = vfds

	register.lock.Lock()
	defer register.lock.Unlock()

	if !register.registered {
		register.fds = fds
		return register.register()
	}

	var fdi int
	indexs := make(map[int32]int, len(fds))
	updatedSpares := make(map[int]int, len(fds))
	for i, spares := range register.sparseIndexs {
		updatedSpares[i] = spares
		for si := 0; si < spares; si++ {
			for ; fdi < len(fds); fdi++ {
				register.fds[i+si] = fds[fdi]
				indexs[fds[fdi]] = i + si

				fdi++
				updatedSpares[i]--
				break
			}

			if fdi >= len(fds) {
				goto update
			}
		}
	}
	register.fds = append(register.fds, fds[fdi:]...)

update:
	if err := register.fresh(0, len(register.fds)); err != nil {
		return err
	}

	for i, spares := range updatedSpares {
		if spares > 0 {
			register.sparseIndexs[i] = spares
			continue
		}
		delete(register.sparseIndexs, i)
	}
	for i, fd := range register.fds {
		register.indexs.Store(fd, i)
	}

	return nil
}

func (register *fileRegister) RegisterFile(fd int32) error {
	if fd < 0 {
		return nil
	}

	if _, ok := register.GetFileIndex(fd); ok {
		return nil
	}

	register.lock.Lock()
	defer register.lock.Unlock()

	if !register.registered {
		register.fds = []int32{fd}
		return register.register()
	}

	var fdi int
	var spares int
	for fdi, spares = range register.sparseIndexs {
		break
	}
	register.fds[fdi] = fd

	if err := register.fresh(fdi, 1); err != nil {
		return err
	}

	if spares == 1 {
		delete(register.sparseIndexs, fdi)
	} else {
		register.sparseIndexs[fdi]--
	}

	register.indexs.Store(fd, fdi)
	return nil
}

func (register *fileRegister) UnregisterFile(fd int32) error {
	if fd < 0 {
		return nil
	}

	register.lock.Lock()
	defer register.lock.Unlock()

	fdi, ok := register.deleteFile(fd)
	if !ok {
		return nil
	}

	return register.fresh(fdi, 1)
}

func (register *fileRegister) UnregisterFiles(fds []int32) error {
	register.lock.Lock()
	defer register.lock.Unlock()

	var unregistered bool
	for _, fd := range fds {
		if fd < 0 {
			continue
		}

		_, ok := register.deleteFile(fd)
		if !ok {
			continue
		}
		unregistered = true
	}
	if unregistered {
		return nil
	}

	return register.fresh(0, len(register.fds))
}

func (register *fileRegister) deleteFile(fd int32) (fdi int, ok bool) {
	var v interface{}
	/*
		go version >= 1.15

		v, ok = register.index.LoadAndDelete(fd)
	*/
	v, ok = register.indexs.Load(fd)
	if !ok {
		return
	}
	register.indexs.Delete(fd)

	fdi = v.(int)
	register.fds[fdi] = -1

	var updated bool
	for i, sparse := range register.sparseIndexs {
		if i+sparse == fdi {
			register.sparseIndexs[i]++
			updated = true
			break
		}
	}

	if !updated {
		register.sparseIndexs[fdi] = 1
	}
	return
}

func (register *fileRegister) fresh(offset int, length int) error {
	update := iouring_syscall.IOURingFilesUpdate{
		Offset: uint32(offset),
		Fds:    &register.fds[offset],
	}
	return iouring_syscall.IOURingRegister(
		register.iouringFd,
		iouring_syscall.IORING_REGISTER_FILES_UPDATE,
		unsafe.Pointer(&update),
		uint32(len(register.fds)),
	)
}
