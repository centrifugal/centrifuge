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

	targetDir, err := os.Open(filepath.Dir(target))
	if err != nil {
		fmt.Printf("open old directory error: %v\n", err)
		return
	}
	linkDir, err := os.Open(filepath.Dir(link))
	if err != nil {
		fmt.Printf("open new directory error: %v\n", err)
		return
	}

	pr, err := iouring.Linkat(
		int(targetDir.Fd()), filepath.Base(target),
		int(linkDir.Fd()), filepath.Base(link), 0)
	if err != nil {
		fmt.Printf("prep request error: %v\n", err)
		return
	}
	request, err := iour.SubmitRequest(pr, nil)
	<-request.Done()
	if err := request.Err(); err != nil {
		return
	}
}
