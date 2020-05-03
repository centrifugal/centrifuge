package controlpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandProtoExtra(t *testing.T) {
	msg := &Command{
		UID:    "test",
		Method: MethodTypeDisconnect,
		Params: Raw("{}"),
	}

	d, b := msg.Method.EnumDescriptor()
	require.Equal(t, fileDescriptorControl, d)
	require.Equal(t, []int{0}, b)

	require.Equal(t, "test", msg.GetUID())
	require.Equal(t, MethodTypeDisconnect, msg.GetMethod())
	require.NotZero(t, msg.String())
}

func TestNodeProtoExtra(t *testing.T) {
	msg := &Node{
		UID:         "test",
		Name:        "test name",
		Version:     "v1.0.0",
		NumChannels: 2,
		NumClients:  3,
		NumUsers:    1,
		Uptime:      12,
		Metrics: &Metrics{
			Interval: 60,
			Items: map[string]float64{
				"item": 1,
			},
		},
	}
	require.Equal(t, "test", msg.GetUID())
	require.Equal(t, "test name", msg.GetName())
	require.Equal(t, "v1.0.0", msg.GetVersion())
	require.Equal(t, uint32(2), msg.GetNumChannels())
	require.Equal(t, uint32(1), msg.GetNumUsers())
	require.Equal(t, uint32(3), msg.GetNumClients())
	require.Equal(t, uint32(12), msg.GetUptime())
	require.NotNil(t, msg.GetMetrics())
	require.NotZero(t, msg.String())
}

func TestDisconnectProtoExtra(t *testing.T) {
	msg := &Disconnect{
		User: "test",
	}
	require.Equal(t, "test", msg.GetUser())
	require.NotZero(t, msg.String())
}

func TestUnsubscribeProtoExtra(t *testing.T) {
	msg := &Unsubscribe{
		User:    "test",
		Channel: "test channel",
	}
	require.Equal(t, "test", msg.GetUser())
	require.Equal(t, "test channel", msg.GetChannel())
	require.NotZero(t, msg.String())
}
