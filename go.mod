module github.com/centrifugal/centrifuge

go 1.12

replace github.com/centrifugal/protocol => ../fastprotocol

require (
	github.com/FZambia/eagle v0.0.1
	github.com/FZambia/sentinel v1.1.0
	github.com/centrifugal/protocol v0.5.0
	github.com/gogo/protobuf v1.3.2
	github.com/gomodule/redigo v1.8.4
	github.com/google/uuid v1.2.0
	github.com/gorilla/websocket v1.4.2
	github.com/igm/sockjs-go/v3 v3.0.0
	github.com/mna/redisc v1.1.7
	github.com/prometheus/client_golang v1.6.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)
