module github.com/centrifugal/centrifuge/_examples

go 1.14

replace github.com/centrifugal/centrifuge => ../

require (
	github.com/centrifugal/centrifuge v0.7.0
	github.com/centrifugal/protocol v0.3.0
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5
	github.com/gin-contrib/sessions v0.0.3
	github.com/gin-gonic/gin v1.5.0
	github.com/gobwas/ws v1.0.3
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/sessions v1.2.0
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/nats-io/nats-server/v2 v2.1.4 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/prometheus/client_golang v0.9.2
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	nhooyr.io/websocket v1.8.4
)
