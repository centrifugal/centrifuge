package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelNotFound(t *testing.T) {
	c := DefaultConfig
	_, ok := c.channelOpts("xxx")
	require.False(t, ok)
}

func TestConfigValidateDefault(t *testing.T) {
	err := DefaultConfig.Validate()
	require.NoError(t, err)
}

func TestConfigValidateInvalidNamespaceName(t *testing.T) {
	c := DefaultConfig
	c.Namespaces = []ChannelNamespace{
		{
			Name:           "invalid name",
			ChannelOptions: ChannelOptions{},
		},
	}
	err := c.Validate()
	require.Error(t, err)
}

func TestConfigValidateDuplicateNamespaceName(t *testing.T) {
	c := DefaultConfig
	c.Namespaces = []ChannelNamespace{
		{
			Name:           "name",
			ChannelOptions: ChannelOptions{},
		},
		{
			Name:           "name",
			ChannelOptions: ChannelOptions{},
		},
	}
	err := c.Validate()
	require.Error(t, err)
}

func TestConfigValidateNoPersonalNamespace(t *testing.T) {
	c := DefaultConfig
	c.Namespaces = []ChannelNamespace{}
	c.UserSubscribeToPersonal = true
	c.UserPersonalChannelNamespace = "name"
	err := c.Validate()
	require.Error(t, err)
}

func TestConfigValidateMalformedReceiverTopLevel(t *testing.T) {
	c := DefaultConfig
	c.HistoryRecover = true
	err := c.Validate()
	require.Error(t, err)
}

func TestConfigValidateMalformedReceiverInNamespace(t *testing.T) {
	c := DefaultConfig
	c.Namespaces = []ChannelNamespace{
		{
			Name: "name",
			ChannelOptions: ChannelOptions{
				HistoryRecover: true,
			},
		},
	}
	err := c.Validate()
	require.Error(t, err)
}
