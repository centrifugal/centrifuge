// +build linux

package iouring

import (
	"unsafe"
)

type UserData struct {
	id uint64

	resulter chan<- Result
	opcode   uint8

	holds   []interface{}
	request *request
}

func (data *UserData) SetResultResolver(resolver ResultResolver) {
	data.request.resolver = resolver
}

func (data *UserData) SetRequestInfo(info interface{}) {
	data.request.requestInfo = info
}

func (data *UserData) SetRequestCallback(callback RequestCallback) {
	data.request.callback = callback
}

func (data *UserData) SetRequestBuffer(b0, b1 []byte) {
	data.request.b0, data.request.b1 = b0, b1
}

func (data *UserData) SetRequestBuffers(bs [][]byte) {
	data.request.bs = bs
}

func (data *UserData) Hold(vars ...interface{}) {
	data.holds = append(data.holds, vars)
}

func (data *UserData) hold(vars ...interface{}) {
	data.holds = vars
}

func (data *UserData) setOpcode(opcode uint8) {
	data.opcode = opcode
	data.request.opcode = opcode
}

// TODO(iceber): use sync.Poll
func makeUserData(iour *IOURing, ch chan<- Result) *UserData {
	userData := &UserData{
		resulter: ch,
		request:  &request{iour: iour, done: make(chan struct{})},
	}

	userData.id = uint64(uintptr(unsafe.Pointer(userData)))
	userData.request.id = userData.id
	return userData
}
