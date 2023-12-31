package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iceber/iouring-go"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s old_path new_path\n", os.Args[0])
		return
	}
	oldPath := os.Args[1]
	newPath := os.Args[2]

	iour, err := iouring.New(1)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	oldPathDir, err := os.Open(filepath.Dir(oldPath))
	if err != nil {
		fmt.Printf("open old directory error: %v\n", err)
		return
	}
	newPathDir, err := os.Open(filepath.Dir(newPath))
	if err != nil {
		fmt.Printf("open new directory error: %v\n", err)
		return
	}

	pr, err := iouring.Renameat(
		int(oldPathDir.Fd()), filepath.Base(oldPath),
		int(newPathDir.Fd()), filepath.Base(newPath))
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
