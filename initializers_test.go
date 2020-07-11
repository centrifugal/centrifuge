package centrifuge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func InitializeSubscribedClient(t *testing.T, node *Node, userID, chanID string) *Client {
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)

	newCtx := SetCredentials(context.Background(), &Credentials{UserID: userID})

	client, err := newClient(newCtx, node, transport)
	require.NoError(t, err)

	connectClient(t, client)
	require.Contains(t, node.hub.users, userID)

	subscribeClient(t, client, chanID)
	require.Contains(t, client.channels, chanID)

	return client
}
