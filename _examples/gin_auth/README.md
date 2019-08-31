Example demonstrates simple chat with JSON protocol sharing session auth with [gin-gonic](https://github.com/gin-gonic/gin).

Client uses Websocket by default, but you can simply uncomment one line in `chat.html` to use SockJS instead. 

To start example run the following command from example directory:

```
GO111MODULE=on go run main.go
```

Then go to http://localhost:8080/login to see it in action.

Once logged in, it will redirect you to the /chat page where you can see that you are logged in through gin.
There is only one email/pass combination : `email@email.com:password`

[gin-gonic]: https://github.com/gin-gonic/gin

_Credits to the example by FZambia from whom I took most of the centrifuge code_