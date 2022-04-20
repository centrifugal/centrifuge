module github.com/centrifugal/centrifuge/_examples

go 1.14

replace github.com/centrifugal/centrifuge => ../

require (
	github.com/FZambia/tarantool v0.2.2
	github.com/centrifugal/centrifuge v0.8.2
	github.com/centrifugal/protocol v0.8.5-0.20220420090656-66d25d71ef6f
	github.com/cristalhq/jwt/v3 v3.0.0
	github.com/dchest/uniuri v0.0.0-20200228104902-7aecb25e1fe5
	github.com/gin-contrib/sessions v0.0.3
	github.com/gin-gonic/gin v1.7.3
	github.com/gobwas/ws v1.0.3
	github.com/golang/protobuf v1.5.0
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/sessions v1.2.0
	github.com/gorilla/websocket v1.4.2
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/nats-server/v2 v2.7.2 // indirect
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/vmihailenco/msgpack/v5 v5.3.1
	golang.org/x/crypto v0.0.0-20220210151621-f4118a5b28e2 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158 // indirect
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8 // indirect
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.27.1
	nhooyr.io/websocket v1.8.6
)
