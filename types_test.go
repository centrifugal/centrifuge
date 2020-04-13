package centrifuge

//func TestPublicationFromProto(t *testing.T) {
//	pub := publicationFromProto(&protocol.Publication{
//		Offset: 16,
//		Data:   []byte(`{}`),
//	})
//	require.Equal(t, uint64(16), pub.Offset)
//	require.Equal(t, []byte("{}"), pub.Data)
//}
//
//func TestClientInfoFromProto(t *testing.T) {
//	info := clientInfoFromProto(&protocol.ClientInfo{
//		User:     "user",
//		Client:   "client",
//		ConnInfo: []byte("{}"),
//		ChanInfo: []byte("{}"),
//	})
//	require.Equal(t, "user", info.User)
//	require.Equal(t, "client", info.Client)
//	require.Equal(t, []byte("{}"), info.ConnInfo)
//	require.Equal(t, []byte("{}"), info.ChanInfo)
//}
