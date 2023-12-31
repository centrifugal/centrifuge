package main

import (
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/iceber/iouring-go"
	iouring_syscall "github.com/iceber/iouring-go/syscall"
)

const (
	nvmeOpCodeAdminIdentify = uint8(0x06)

	nvmeIdentifyCnsCtrl = uint32(0x01)

	entries uint = 64

	nrBits   = 8
	typeBits = 8
	sizeBits = 14

	nrShift   = 0
	typeShift = nrShift + nrBits
	sizeShift = typeShift + typeBits
	dirShift  = sizeShift + sizeBits

	iocRead  = uint64(2)
	iocWrite = uint64(1)
	iocWrRd  = iocWrite | iocRead

	iocInOut = iocWrRd << dirShift

	iocNVMeType = 'N' << typeShift

	uringCmdAdmin = iocInOut | iocNVMeType | (0x82 << nrShift) | uint64(unsafe.Sizeof(AdminCmd{})<<sizeShift)
)

type PassthruCmd struct {
	OpCode     uint8
	Flags      uint8
	_          uint16 // Reserved
	NSId       uint32
	CDW2       uint32
	CDW3       uint32
	Meta       uintptr
	Data       uintptr
	MetaLength uint32
	DataLength uint32
	CDW10      uint32
	CDW11      uint32
	CDW12      uint32
	CDW13      uint32
	CDW14      uint32
	CDW15      uint32
}

type AdminCmd struct {
	PassthruCmd

	TimeoutMSec uint32
	Result      uint32
}

func PrepNVMeIdCtrl(fd int, b []byte) iouring.PrepRequest {
	// id-ctrl command don't use the cntId, nsid and nvmSetId
	const (
		cntId    uint32 = 0
		nsid     uint32 = 0
		nvmSetId uint32 = 0
	)

	return func(sqe iouring_syscall.SubmissionQueueEntry, userData *iouring.UserData) {
		userData.SetRequestInfo(b)
		userData.SetRequestCallback(func(result iouring.Result) error {
			if err := result.Err(); err != nil {
				fmt.Printf("nvme admin passthru failed: %v", err)
			}

			buf := result.GetRequestInfo().([]byte)
			fmt.Printf("VID: %x%x\n", buf[1], buf[0])
			fmt.Printf("Controller Model Number: %s\n", buf[4:44])

			return nil
		})

		// nvme command passthru will not use the user data pointer because nvme driver uses
		// nvme command structure same with IOCtl, at the last 80byte of SQE128 entry, and
		// also use the data pointer and length field in the NVMe command structure.
		sqe.PrepOperation(
			iouring_syscall.IORING_OP_URING_CMD,
			int32(fd),
			0,
			0,
			uringCmdAdmin,
		)

		nvmeCmd := sqe.CMD(AdminCmd{}).(*AdminCmd)
		nvmeCmd.OpCode = nvmeOpCodeAdminIdentify
		nvmeCmd.NSId = nsid
		nvmeCmd.CDW10 = cntId<<16 | nvmeIdentifyCnsCtrl
		nvmeCmd.CDW11 = nvmSetId

		nvmeCmd.Data, nvmeCmd.DataLength = uintptr(unsafe.Pointer(&b[0])), uint32(len(b))
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s nvme_chr_device\n", os.Args[0])
		return
	}

	// nvme passthru command use only SQE128 and CQE32 flags
	iour, err := iouring.New(entries, iouring.WithSQE128(), iouring.WithCQE32())
	if err != nil {
		fmt.Printf("new IOURing error: %v", err)
		return
	}
	defer func() { _ = iour.Close() }()

	dev, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("Open device file failed: %v\n", err)
		return
	}
	defer func() { _ = dev.Close() }()

	ch := make(chan iouring.Result)
	prepRequest := PrepNVMeIdCtrl(int(dev.Fd()), make([]byte, 4096))

	now := time.Now()
	if _, err = iour.SubmitRequest(prepRequest, ch); err != nil {
		fmt.Printf("request error: %v\n", err)
		return
	}

	fmt.Printf("waiting response callback\n")

	select {
	case result := <-ch:
		_ = result.Callback()
		fmt.Printf("nvme id-ctrl successful: %v\n", time.Now().Sub(now))
	}
}
