package main

import (
	"fmt"
	"os"

	"github.com/iceber/iouring-go"
)

const entries uint = 64

var (
	str1 = "str1 str1 str1 str1\n"
	str2 = "str2 str2 str2 str2 str2\n"
)

func main() {
	iour, err := iouring.New(entries)
	if err != nil {
		panic(fmt.Sprintf("new IOURing error: %v", err))
	}
	defer iour.Close()

	file, err := os.Create("./tmp")
	if err != nil {
		panic(err)
	}

	prepWrite1 := iouring.Write(int(file.Fd()), []byte(str1)).WithInfo("write str1")
	prepWrite2 := iouring.Pwrite(int(file.Fd()), []byte(str2), uint64(len(str1))).WithInfo("write str2")

	buffer := make([]byte, len(str1)+len(str2))
	prepRead1 := iouring.Read(int(file.Fd()), buffer).WithInfo("read fd to buffer")
	prepRead2 := iouring.Write(int(os.Stdout.Fd()), buffer).WithInfo("read buffer to stdout")

	ch := make(chan iouring.Result, 4)
	_, err = iour.SubmitLinkRequests(
		[]iouring.PrepRequest{
			prepWrite1,
			prepWrite2,
			prepRead1,
			prepRead2,
		},
		ch,
	)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 4; i++ {
		result := <-ch
		info := result.GetRequestInfo().(string)
		fmt.Println(info)
		if err := result.Err(); err != nil {
			fmt.Printf("error: %v\n", err)
		}
	}
}
