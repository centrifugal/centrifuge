package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iceber/iouring-go"
	"golang.org/x/sys/unix"
)

func unlinkPath(iour *iouring.IOURing, path string) {
	pathDir, err := os.Open(filepath.Dir(path))
	if err != nil {
		fmt.Printf("open path error: %v\n", err)
		return
	}
	defer pathDir.Close()

	// checking if the path is a directory or a path, to set the good flags
	fileInfo, err := os.Stat(path)
	if err != nil {
		fmt.Printf("file stat error: %v\n", err)
		return
	}
	var flags int32 = 0 // no flags for files
	if fileInfo.IsDir() {
		flags = unix.AT_REMOVEDIR
	}

	pr, err := iouring.Unlinkat(int(pathDir.Fd()), filepath.Base(path), flags)
	if err != nil {
		fmt.Printf("prep request error: %v\n", err)
		return
	}
	request, err := iour.SubmitRequest(pr, nil)
	<-request.Done()
	if err := request.Err(); err != nil {
		fmt.Printf("submited iouring request error: %v\n", err)
		return
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s path [path2] [path3] ...\n", os.Args[0])
		return
	}

	iour, err := iouring.New(1)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	for _, filename := range os.Args[1:] {
		unlinkPath(iour, filename)
	}
}
