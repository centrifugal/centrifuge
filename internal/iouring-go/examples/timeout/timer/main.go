package main

import (
	"fmt"
	"time"

	"github.com/iceber/iouring-go"
)

func main() {
	iour, err := iouring.New(3)
	if err != nil {
		panic(fmt.Sprintf("new IOURing error: %v", err))
	}
	defer iour.Close()

	now := time.Now()

	prep1 := iouring.Timeout(2 * time.Second)
	prep2 := iouring.Timeout(5 * time.Second)
	ch := make(chan iouring.Result, 1)
	if _, err := iour.SubmitRequests([]iouring.PrepRequest{prep1, prep2}, ch); err != nil {
		panic(err)
	}

	for i := 0; i < 2; i++ {
		result := <-ch
		if err := result.Err(); err != nil {
			fmt.Println("error: ", err)
		}
		fmt.Println(time.Now().Sub(now))
	}
}
