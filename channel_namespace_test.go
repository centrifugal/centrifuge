package centrifuge

//func getConnTokenHS(user string, exp int64) string {
//	return getConnToken(user, exp, nil)
//}
//
//func getSubscribeTokenHS(channel string, client string, exp int64) string {
//	return getSubscribeToken(channel, client, exp, nil)
//}
//
//func TestClientConnectNoCredentialsNoTokenInsecure(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.ClientInsecure = true
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
//	require.Nil(t, disconnect)
//	require.Nil(t, replies[0].Error)
//	result := extractConnectResult(replies, client.Transport().Protocol())
//	require.NotEmpty(t, result.Client)
//	require.Empty(t, client.UserID())
//}

//func TestClientConnectNoCredentialsNoTokenAnonymous(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.ClientAnonymous = true
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
//	require.Nil(t, disconnect)
//	require.Nil(t, replies[0].Error)
//	result := extractConnectResult(replies, client.Transport().Protocol())
//	require.NotEmpty(t, result.Client)
//	require.Empty(t, client.UserID())
//}

//
//func TestClientConnectWithMalformedToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: "bad bad token",
//	}, rw)
//	require.NotNil(t, disconnect)
//	require.Equal(t, disconnect, DisconnectInvalidToken)
//}
//
//func TestClientConnectWithValidTokenHMAC(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: getConnTokenHS("42", 0),
//	}, rw)
//	require.Nil(t, disconnect)
//	result := extractConnectResult(replies, client.Transport().Protocol())
//	require.Equal(t, client.ID(), result.Client)
//	require.Equal(t, false, result.Expires)
//}
//
//func TestClientConnectWithValidTokenRSA(t *testing.T) {
//	privateKey, pubKey := generateTestRSAKeys(t)
//
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenRSAPublicKey = pubKey
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: getConnToken("42", 0, privateKey),
//	}, rw)
//	require.Nil(t, disconnect)
//	result := extractConnectResult(replies, client.Transport().Protocol())
//	require.Equal(t, client.ID(), result.Client)
//	require.Equal(t, false, result.Expires)
//}
//
//func TestClientConnectWithExpiringToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	node.On().ClientConnecting(func(ctx context.Context, info TransportInfo, event ConnectEvent) ConnectReply {
//		return ConnectReply{
//			ClientSideRefresh: true,
//		}
//	})
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: getConnTokenHS("42", time.Now().Unix()+10),
//	}, rw)
//	require.Nil(t, disconnect)
//	result := extractConnectResult(replies, client.Transport().Protocol())
//	require.Equal(t, true, result.Expires)
//	require.True(t, result.TTL > 0)
//	require.True(t, client.authenticated)
//}
//
//func TestClientConnectWithExpiredToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: getConnTokenHS("42", 1525541722),
//	}, rw)
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorTokenExpired.toProto(), replies[0].Error)
//	require.False(t, client.authenticated)
//}

//
//func TestClientSideTokenRefresh(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	node.On().ClientConnecting(func(ctx context.Context, info TransportInfo, event ConnectEvent) ConnectReply {
//		return ConnectReply{
//			ClientSideRefresh: true,
//		}
//	})
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: getConnTokenHS("42", 1525541722),
//	}, rw)
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorTokenExpired.toProto(), replies[0].Error)
//
//	refreshResp, disconnect := client.refreshCmd(&protocol.RefreshRequest{
//		Token: getConnTokenHS("42", 2525637058),
//	})
//	require.Nil(t, disconnect)
//	require.NotEmpty(t, client.ID())
//	require.True(t, refreshResp.Result.Expires)
//	require.True(t, refreshResp.Result.TTL > 0)
//}
//
//func TestClientSideTokenRefresh_CustomToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	node.On().ClientConnecting(func(ctx context.Context, info TransportInfo, event ConnectEvent) ConnectReply {
//		require.True(t, strings.HasPrefix(event.Token, "custom"))
//		return ConnectReply{
//			ClientSideRefresh: true,
//			Credentials: &Credentials{
//				UserID:   "12",
//				ExpireAt: time.Now().Unix() + 10,
//			},
//		}
//	})
//
//	transport := newTestTransport()
//	client, _ := newClient(context.Background(), node, transport)
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//	disconnect := client.connectCmd(&protocol.ConnectRequest{
//		Token: "custom token",
//	}, rw)
//	require.Nil(t, disconnect)
//	require.Nil(t, replies[0].Error)
//
//	refreshResp, disconnect := client.refreshCmd(&protocol.RefreshRequest{
//		Token: getConnTokenHS("42", 2525637058),
//	})
//	require.Equal(t, DisconnectInvalidToken, disconnect)
//
//	client.On().Refresh(func(event RefreshEvent) RefreshReply {
//		require.True(t, strings.HasPrefix(event.Token, "custom"))
//		return RefreshReply{
//			ExpireAt: time.Now().Unix() + 10,
//		}
//	})
//	refreshResp, disconnect = client.refreshCmd(&protocol.RefreshRequest{
//		Token: "custom token new",
//	})
//	require.Nil(t, disconnect)
//	require.NotEmpty(t, client.ID())
//	require.True(t, refreshResp.Result.Expires)
//	require.True(t, refreshResp.Result.TTL > 0)
//}

//func TestClientUserPersonalChannel(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	config := node.Config()
//	config.UserSubscribeToPersonal = true
//	config.Namespaces = []ChannelNamespace{
//		{
//			Name:           "user",
//			ChannelOptions: ChannelOptions{},
//		},
//	}
//	_ = node.Reload(config)
//
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	var tests = []struct {
//		Name      string
//		Namespace string
//		Error     *Error
//	}{
//		{"ok_no_namespace", "", nil},
//		{"ok_with_namespace", "user", nil},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.Name, func(t *testing.T) {
//			config := node.Config()
//			config.UserSubscribeToPersonal = true
//			config.UserPersonalChannelNamespace = tt.Namespace
//			err := node.Reload(config)
//			require.NoError(t, err)
//			transport := newTestTransport()
//			transport.sink = make(chan []byte, 100)
//			ctx := context.Background()
//			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//			client, _ := newClient(newCtx, node, transport)
//			var replies []*protocol.Reply
//			rw := testReplyWriter(&replies)
//			client.handleCommand(&protocol.Command{
//				ID: 1,
//			}, rw.write, rw.flush)
//			if tt.Error != nil {
//				require.Equal(t, tt.Error, replies[0].Error)
//			} else {
//				done := make(chan struct{})
//				go func() {
//					for data := range transport.sink {
//						if strings.Contains(string(data), "test message") {
//							close(done)
//						}
//					}
//				}()
//
//				_, err := node.Publish(node.PersonalChannel("42"), []byte(`{"text": "test message"}`))
//				require.NoError(t, err)
//
//				select {
//				case <-time.After(time.Second):
//					require.Fail(t, "timeout receiving publication")
//				case <-done:
//				}
//			}
//		})
//	}
//}
//

//func TestClientSubscribePrivateChannelNoToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	connectClient(t, client)
//
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//
//	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)
//}
//
//func TestClientSubscribePrivateChannelWithToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	connectClient(t, client)
//
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//
//	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//		Token:   getSubscribeTokenHS("$wrong_channel", "wrong client", 0),
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)
//
//	replies = nil
//	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//		Token:   getSubscribeTokenHS("$wrong_channel", client.ID(), 0),
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)
//
//	replies = nil
//	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//		Token:   getSubscribeTokenHS("$test1", client.ID(), 0),
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Nil(t, replies[0].Error)
//}
//

//func TestClientSubscribePrivateChannelWithExpiringToken(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.TokenHMACSecretKey = "secret"
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	connectClient(t, client)
//
//	var replies []*protocol.Reply
//	rw := testReplyWriter(&replies)
//
//	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//		Token:   getSubscribeTokenHS("$test1", client.ID(), 10),
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Equal(t, ErrorTokenExpired.toProto(), replies[0].Error)
//
//	replies = nil
//	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
//		Channel: "$test1",
//		Token:   getSubscribeTokenHS("$test1", client.ID(), time.Now().Unix()+10),
//	}, rw, false)
//	require.Nil(t, subCtx.disconnect)
//	require.Nil(t, replies[0].Error, "token is valid and not expired yet")
//	res := extractSubscribeResult(replies, client.Transport().Protocol())
//	require.True(t, res.Expires, "expires flag must be set")
//	require.True(t, res.TTL > 0, "positive TTL must be set")
//}
//

//func TestClientPublish(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	connectClient(t, client)
//
//	publishResp, disconnect := client.publishCmd(&protocol.PublishRequest{
//		Channel: "test",
//		Data:    []byte(`{}`),
//	})
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorPermissionDenied.toProto(), publishResp.Error)
//
//	config := node.Config()
//	config.Publish = true
//	_ = node.Reload(config)
//
//	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
//		Channel: "test",
//		Data:    []byte(`{}`),
//	})
//	require.Nil(t, disconnect)
//	require.Nil(t, publishResp.Error)
//
//	config = node.Config()
//	config.SubscribeToPublish = true
//	_ = node.Reload(config)
//
//	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
//		Channel: "test",
//		Data:    []byte(`{}`),
//	})
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorPermissionDenied.toProto(), publishResp.Error)
//
//	subscribeClient(t, client, "test")
//	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
//		Channel: "test",
//		Data:    []byte(`{}`),
//	})
//	require.Nil(t, disconnect)
//	require.Nil(t, publishResp.Error)
//}

//func TestClientHistoryDisabled(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.HistorySize = 10
//	config.HistoryLifetime = 60
//	config.HistoryDisableForClient = true
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	_, _ = node.Publish("test", []byte(`{}`))
//
//	connectClient(t, client)
//	subscribeClient(t, client, "test")
//
//	historyResp, disconnect := client.historyCmd(&protocol.HistoryRequest{
//		Channel: "test",
//	})
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorNotAvailable.toProto(), historyResp.Error)
//}
//
//func TestClientPresenceDisabled(t *testing.T) {
//	node := nodeWithMemoryEngine()
//	defer func() { _ = node.Shutdown(context.Background()) }()
//
//	config := node.Config()
//	config.Presence = true
//	config.PresenceDisableForClient = true
//	_ = node.Reload(config)
//
//	transport := newTestTransport()
//	ctx := context.Background()
//	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
//	client, _ := newClient(newCtx, node, transport)
//
//	_, _ = node.Publish("test", []byte(`{}`))
//
//	connectClient(t, client)
//	subscribeClient(t, client, "test")
//
//	presenceResp, disconnect := client.presenceCmd(&protocol.PresenceRequest{
//		Channel: "test",
//	})
//	require.Nil(t, disconnect)
//	require.Equal(t, ErrorNotAvailable.toProto(), presenceResp.Error)
//}
//
