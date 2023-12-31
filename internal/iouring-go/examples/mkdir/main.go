package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iceber/iouring-go"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s dir_path\n", os.Args[0])
		return
	}
	path := os.Args[1]

	iour, err := iouring.New(1)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	pathDir, err := os.Open(filepath.Dir(path))
	if err != nil {
		fmt.Printf("open directory error: %v\n", err)
		return
	}
	defer pathDir.Close()

	pr, err := iouring.Mkdirat(int(pathDir.Fd()), filepath.Base(path), 0)
	if err != nil {
		fmt.Printf("prep request error: %v\n", err)
		return
	}
	request, err := iour.SubmitRequest(pr, nil)
	<-request.Done()
	if err := request.Err(); err != nil {
		fmt.Printf("submit iouring request error: %v\n", err)
		return
	}
}
