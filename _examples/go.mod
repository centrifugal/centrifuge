module github.com/centrifugal/centrifuge/_examples

go 1.14

replace (
	github.com/centrifugal/centrifuge => ../
	github.com/lucas-clemente/quic-go v0.24.0 => github.com/alta/quic-go v0.0.0-20210923171602-7151b11990d2
)

require (
	github.com/FZambia/tarantool v0.2.2
	github.com/centrifugal/centrifuge v0.8.2
	github.com/centrifugal/protocol v0.7.3
	github.com/cristalhq/jwt/v3 v3.0.0
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5
	github.com/gin-contrib/sessions v0.0.3
	github.com/gin-gonic/gin v1.7.3
	github.com/gobwas/ws v1.0.3
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/sessions v1.2.0
	github.com/gorilla/websocket v1.4.2
	github.com/lucas-clemente/quic-go v0.24.0
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/nats-io/nats-server/v2 v2.2.0 // indirect
	github.com/nats-io/nats.go v1.12.3
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.13.0
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/vmihailenco/msgpack/v5 v5.3.1
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.27.1
	nhooyr.io/websocket v1.8.6
)
