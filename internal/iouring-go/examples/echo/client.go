package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <host:port> <string>\n", os.Args[0])
		return
	}
	serverAddr, msg := os.Args[1], os.Args[2]

	c, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	n, err := c.Write([]byte(msg))
	if err != nil {
		panic(err)
	}
	fmt.Println("write byte: ", n)

	buf := make([]byte, 1024)
	if _, err = c.Read(buf); err != nil {
		panic(err)
	}

	fmt.Printf("echo: %s\n", buf)
}
