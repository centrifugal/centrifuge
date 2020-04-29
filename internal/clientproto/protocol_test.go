package clientproto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/protocol"
)

func TestPushHelpers(t *testing.T) {
	msg := NewMessagePush(protocol.Raw("{}"))
	require.NotNil(t, msg)
	msg = NewJoinPush("test", protocol.Raw("{}"))
	require.NotNil(t, msg)
	msg = NewLeavePush("test", protocol.Raw("{}"))
	require.NotNil(t, msg)
	msg = NewPublicationPush("test", protocol.Raw("{}"))
	require.NotNil(t, msg)
	msg = NewSubPush("test", protocol.Raw("{}"))
	require.NotNil(t, msg)
	msg = NewUnsubPush("test", protocol.Raw("{}"))
	require.NotNil(t, msg)
}
