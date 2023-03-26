module github.com/centrifugal/centrifuge/_examples

go 1.16

replace github.com/centrifugal/centrifuge => ../

require (
	github.com/FZambia/tarantool v0.2.2
	github.com/centrifugal/centrifuge v0.8.2
	github.com/centrifugal/protocol v0.10.0
	github.com/cristalhq/jwt/v3 v3.0.0
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5
	github.com/gin-contrib/sessions v0.0.3
	github.com/gin-gonic/gin v1.7.7
	github.com/gobwas/ws v1.0.3
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/sessions v1.2.0
	github.com/gorilla/websocket v1.5.0
	github.com/lucas-clemente/quic-go v0.27.2
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/nats-io/nats-server/v2 v2.8.1 // indirect
	github.com/nats-io/nats.go v1.14.0
	github.com/prometheus/client_golang v1.14.0
	github.com/stretchr/testify v1.8.2
	github.com/vmihailenco/msgpack/v5 v5.3.1
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8 // indirect
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.30.0
	nhooyr.io/websocket v1.8.6
)
