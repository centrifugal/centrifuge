package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iceber/iouring-go"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s target link\n", os.Args[0])
		return
	}
	target := os.Args[1]
	link := os.Args[2]

	iour, err := iouring.New(1)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	linkDir, err := os.Open(filepath.Dir(link))
	if err != nil {
		fmt.Printf("open directory error: %v\n", err)
		return
	}
	defer linkDir.Close()

	pr, err := iouring.Symlinkat(target, int(linkDir.Fd()), filepath.Base(link))
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
