package main

import (
	"fmt"
	"os"

	"github.com/iceber/iouring-go"
)

var blockSize int64 = 32 * 1024

func main() {
	if len(os.Args) <= 1 {
		fmt.Printf("Usage: %s file1 file2 ...\n", os.Args[0])
	}

	iour, err := iouring.New(10)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	compCh := make(chan iouring.Result, 1)

	go func() {
		for _, filename := range os.Args[1:] {
			file, err := os.Open(filename)
			if err != nil {
				panic(fmt.Sprintf("open %s error: %v", filename, err))
			}
			if err := read(iour, file, compCh); err != nil {
				panic(fmt.Sprintf("submit read %s request error: %v", filename, err))
			}
		}
	}()

	files := len(os.Args) - 1

	var prints int
	for result := range compCh {
		filename := result.GetRequestInfo().(string)
		if err := result.Err(); err != nil {
			fmt.Printf("read %s error: %v\n", filename, result.Err())
		}

		fmt.Printf("%s: \n", filename)
		for _, buffer := range result.GetRequestBuffers() {
			fmt.Printf("%s", buffer)
		}
		fmt.Println()

		prints++
		if prints == files {
			break
		}
	}

	fmt.Println("cat successful")
}

func read(iour *iouring.IOURing, file *os.File, ch chan iouring.Result) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()

	blocks := int(size / blockSize)
	if size%blockSize != 0 {
		blocks++
	}

	buffers := make([][]byte, blocks)
	for i := 0; i < blocks; i++ {
		buffers[i] = make([]byte, blockSize)
	}
	if size%blockSize != 0 {
		buffers[blocks-1] = buffers[blocks-1][:size%blockSize]
	}

	prepRequest := iouring.Readv(int(file.Fd()), buffers).WithInfo(file.Name())
	_, err = iour.SubmitRequest(prepRequest, ch)
	return err
}
