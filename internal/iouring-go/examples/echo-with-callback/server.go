package main

import (
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/iceber/iouring-go"
)

const readSize = 1024

var (
	iour     *iouring.IOURing
	resulter chan iouring.Result
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <host:port>\n", os.Args[0])
		return
	}

	var err error
	iour, err = iouring.New(1024)
	if err != nil {
		panic(err)
	}
	defer iour.Close()

	resulter = make(chan iouring.Result, 10)

	fd := listenSocket(os.Args[1])
	prepAccept := iouring.Accept(fd).WithCallback(accept)
	if _, err := iour.SubmitRequest(prepAccept, resulter); err != nil {
		panicf("submit accept request error: %v", err)
	}

	fmt.Println("echo server running...")
	for result := range resulter {
		result.Callback()
	}
}

func accept(result iouring.Result) error {
	prepAccept := iouring.Accept(result.Fd()).WithCallback(accept)
	if _, err := iour.SubmitRequest(prepAccept, resulter); err != nil {
		panicf("submit accept request error: %v", err)
	}

	if err := result.Err(); err != nil {
		panicf("accept error: %v", err)
	}

	connFd := result.ReturnValue0().(int)
	sockaddr := result.ReturnValue1().(*syscall.SockaddrInet4)

	clientAddr := fmt.Sprintf("%s:%d", net.IPv4(sockaddr.Addr[0], sockaddr.Addr[1], sockaddr.Addr[2], sockaddr.Addr[3]), sockaddr.Port)
	fmt.Printf("Client Conn: %s\n", clientAddr)

	buffer := make([]byte, readSize)
	prep := iouring.Read(connFd, buffer).WithInfo(clientAddr).WithCallback(read)
	if _, err := iour.SubmitRequest(prep, resulter); err != nil {
		panicf("submit read request error: %v", err)
	}
	return nil
}

func read(result iouring.Result) error {
	clientAddr := result.GetRequestInfo().(string)
	if err := result.Err(); err != nil {
		panicf("[%s] read error: %v", clientAddr, err)
	}

	num := result.ReturnValue0().(int)
	buf, _ := result.GetRequestBuffer()
	content := buf[:num]

	connPrintf(clientAddr, "read byte: %v\ncontent: %s\n", num, content)

	prep := iouring.Write(result.Fd(), content).WithInfo(clientAddr).WithCallback(write)
	if _, err := iour.SubmitRequest(prep, resulter); err != nil {
		panicf("[%s] submit write request error: %v", clientAddr, err)
	}
	return nil
}

func write(result iouring.Result) error {
	clientAddr := result.GetRequestInfo().(string)
	if err := result.Err(); err != nil {
		panicf("[%s] write error: %v", clientAddr, err)
	}
	connPrintf(clientAddr, "write successful\n")

	prep := iouring.Close(result.Fd()).WithInfo(clientAddr).WithCallback(close)
	if _, err := iour.SubmitRequest(prep, resulter); err != nil {
		panicf("[%s] submit write request error: %v", clientAddr, err)
	}
	return nil
}

func close(result iouring.Result) error {
	clientAddr := result.GetRequestInfo().(string)
	if err := result.Err(); err != nil {
		panicf("[%s] close error: %v", clientAddr, err)
	}
	connPrintf(clientAddr, "close successful\n")
	return nil
}

func listenSocket(addr string) int {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		panic(err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err)
	}

	sockaddr := &syscall.SockaddrInet4{Port: tcpAddr.Port}
	copy(sockaddr.Addr[:], tcpAddr.IP.To4())
	if err := syscall.Bind(fd, sockaddr); err != nil {
		panic(err)
	}

	if err := syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		panic(err)
	}
	return fd
}

func panicf(format string, a ...interface{}) {
	panic(fmt.Sprintf(format, a...))
}

func connPrintf(addr string, format string, a ...interface{}) {
	prefix := fmt.Sprintf("[%s]", addr)
	fmt.Printf(prefix+format, a...)
}
