module github.com/centrifugal/centrifuge/_examples

go 1.14

replace github.com/centrifugal/centrifuge => ../

require (
	github.com/centrifugal/centrifuge v0.8.2
	github.com/centrifugal/protocol v0.3.3
	github.com/cristalhq/jwt/v3 v3.0.0
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5
	github.com/gin-contrib/sessions v0.0.3
	github.com/gin-gonic/gin v1.6.3
	github.com/gobwas/ws v1.0.3
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/sessions v1.2.0
	github.com/klauspost/compress v1.10.8 // indirect
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/nats-io/nats-server/v2 v2.1.4 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/stretchr/testify v1.5.1
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sys v0.0.0-20200610111108-226ff32320da // indirect
	google.golang.org/protobuf v1.24.0 // indirect
	nhooyr.io/websocket v1.8.6
)
