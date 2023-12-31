package main

import (
	"fmt"
	"time"

	"github.com/iceber/iouring-go"
)

func main() {
	iour, err := iouring.New(10)
	if err != nil {
		panic(fmt.Sprintf("new IOURing error: %v", err))
	}
	defer iour.Close()

	now := time.Now()

	rs := iouring.Timeout(2 * time.Second).WithTimeout(1 * time.Second)
	rs1 := iouring.Timeout(5 * time.Second).WithTimeout(4 * time.Second)
	rs = append(rs, rs1...)

	ch := make(chan iouring.Result, 1)
	if _, err := iour.SubmitLinkRequests(rs, ch); err != nil {
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
