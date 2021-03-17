package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/centrifugal/centrifuge/_examples/unidirectional_grpc/clientproto"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "localhost:10000", "The server address in the format of host:port")
)

func handlePush(push *clientproto.Push) {
	log.Printf("push received (type %d, channel %s, data %s", push.Type, push.Channel, fmt.Sprintf("%#v", string(push.Data)))
	switch push.Type {
	case clientproto.PushType_PUSH_TYPE_CONNECT:
		var connectPush clientproto.Connect
		err := proto.Unmarshal(push.Data, &connectPush)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("connected to a server with ID: %s", connectPush.Client)
	case clientproto.PushType_PUSH_TYPE_PUBLICATION:
		var publicationPush clientproto.Publication
		err := proto.Unmarshal(push.Data, &publicationPush)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("new publication from channel %s: %s", push.Channel, fmt.Sprintf("%#v", string(publicationPush.Data)))
	case clientproto.PushType_PUSH_TYPE_DISCONNECT:
		var disconnectPush clientproto.Disconnect
		err := proto.Unmarshal(push.Data, &disconnectPush)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("disconnected from a server: %s", disconnectPush.Reason)
	default:
		log.Println("push type handling not implemented")
	}
}

func handleStream(stream clientproto.CentrifugeUni_ConsumeClient) {
	for {
		streamData, err := stream.Recv()
		if err != nil {
			log.Printf("error recv: %v", err)
			return
		}
		var push clientproto.Push
		if len(streamData.GetData()) == 0 {
			// Could be ping.
			continue
		}
		err = proto.Unmarshal(streamData.GetData(), &push)
		if err != nil {
			log.Printf("error unmarshal push: %v", err)
			return
		}
		handlePush(&push)
	}
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := clientproto.NewCentrifugeUniClient(conn)

	numFailureAttempts := 0
	for {
		time.Sleep(time.Duration(numFailureAttempts) * time.Second)
		log.Println("establishing a unidirectional stream")
		stream, err := client.Consume(context.Background(), &clientproto.ConnectRequest{})
		if err != nil {
			log.Printf("error establishing stream: %v", err)
			numFailureAttempts++
			continue
		}
		log.Println("stream established")
		numFailureAttempts = 0
		handleStream(stream)
	}
}
