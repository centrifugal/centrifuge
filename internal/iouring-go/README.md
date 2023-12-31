# What is io_uring
[io_uring](http://kernel.dk/io_uring.pdf) 

[io_uring-wahtsnew](https://kernel.dk/io_uring-whatsnew.pdf) 

[LWN io_uring](https://lwn.net/Kernel/Index/#io_uring) 

[Lord of the io_uring](https://unixism.net/loti/)

[【译】高性能异步 IO —— io_uring (Effecient IO with io_uring)](http://icebergu.com/archives/linux-iouring)

[Go 与异步 IO - io_uring 的思考](http://icebergu.com/archives/go-iouring)

# Features
- [x] register a file set for io_uring instance
- [x] support file IO
- [x] support socket IO
- [x] support IO timeout
- [x] link request
- [x] set timer
- [x] add request extra info, could get it from the result
- [ ] set logger
- [ ] register buffers and IO with buffers
- [ ] support SQPoll 

# OS Requirements
* Linux Kernel >= 5.6


# Installation
```
go get github.com/iceber/iouring-go
```
[doc](https://pkg.go.dev/github.com/iceber/iouring-go)

# Quickstart
```golang
package main

import (
        "fmt"
        "os"

        "github.com/iceber/iouring-go"
)

var str = "io with iouring"

func main() {
        iour, err := iouring.New(1)
        if err != nil {
                panic(fmt.Sprintf("new IOURing error: %v", err))
        }
        defer iour.Close()

        file, err := os.Create("./tmp.txt")
        if err != nil {
                panic(err)
        }

        ch := make(chan iouring.Result, 1)

        prepRequest := iouring.Write(int(file.Fd()), []byte(str))
        if _, err := iour.SubmitRequest(prepRequest, ch); err != nil {
                panic(err)
        }

        result := <-ch
        i, err := result.ReturnInt()
        if err != nil {
                fmt.Println("write error: ", err)
                return
        }

        fmt.Printf("write byte: %d\n", i)
}
```

# Request With Extra Info
```golang
prepRequest := iouring.Write(int(file.Fd()), []byte(str)).WithInfo(file.Name())

request, err := iour.SubmitRequest(prepRequest, nil)
if err != nil {
    panic(err)
}

<- request.Done()
info, ok := request.GetRequestInfo().(string)
```

# Cancel Request
```golang
prepR := iouring.Timeout(5 * time.Second)
request, err := iour.SubmitRequest(prepR, nil)
if err != nil {
    panic(err)
}

if _, err := request.Cancel(); err != nil{
    fmt.Printf("cancel request error: %v\n", err)
    return
}

<- request.Done()
if err := request.Err(); err != nil{
    if err == iouring.ErrRequestCanceled{
        fmt.Println("request is canceled"0
        return
    }
    fmt.Printf("request error: %v\n", err)
    return
}
```


# Submit multitude request

```golang
var offset uint64
buf1 := make([]byte, 1024)
prep1:= iouring.Pread(fd, buf1, offset)

offset += 1024
buf2 := make([]byte, 1024)
prep2:= iouring.Pread(fd, buf1, offset)

requests, err := iour.SubmitRequests([]iouring.PrepRequest{prep1,prep2}, nil)
if err != nil{
    panic(err)
}
<- requests.Done()
fmt.Println("requests are completed")
```
requests is concurrent execution

# Link request
```golang
var offset uint64
buf := make([]byte, 1024)
prep1 := iouring.Pread(fd, buf1, offset)
prep2 := iouring.Write(int(os.Stdout.Fd()), buf)

iour.SubmitLinkRequests([]iouring.PrepRequest{prep1, prep2}, nil)
```

# Examples
[cat](https://github.com/Iceber/iouring-go/tree/main/examples/cat)

[concurrent-cat](https://github.com/Iceber/iouring-go/tree/main/examples/concurrent-cat)

[cp](https://github.com/Iceber/iouring-go/tree/main/examples/cp)

[request-with-timeout](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/request-with-timeout)

[link-request](https://github.com/Iceber/iouring-go/tree/main/examples/link)

[link-with-timeout](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/link-with-timeout)

[timer](https://github.com/Iceber/iouring-go/tree/main/examples/timeout/timer)

[echo](https://github.com/Iceber/iouring-go/tree/main/examples/echo)

[echo-with-callback](https://github.com/Iceber/iouring-go/tree/main/examples/echo-with-callback)

# TODO
* add tests
* arguments type (eg. int and int32)
* set logger
