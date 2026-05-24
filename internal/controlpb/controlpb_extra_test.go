package controlpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeProtoExtra(t *testing.T) {
	msg := &Node{
		Uid:         "test",
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
	require.Equal(t, "test", msg.GetUid())
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

// TestFilterNodeRoundTrip locks in the wire layout of FilterNode (used as
// label_filter on Subscribe / Unsubscribe / Disconnect / Refresh). Any field
// renumbering or type change here would silently break cluster-wide
// destructive ops with a label filter.
func TestFilterNodeRoundTrip(t *testing.T) {
	original := &FilterNode{
		Op: "and",
		Nodes: []*FilterNode{
			{Op: "", Key: "region", Cmp: "eq", Val: "us-east"},
			{Op: "or", Nodes: []*FilterNode{
				{Op: "", Key: "tier", Cmp: "in", Vals: []string{"premium", "enterprise"}},
				{Op: "not", Nodes: []*FilterNode{
					{Op: "", Key: "blocked", Cmp: "ex"},
				}},
			}},
		},
	}

	for name, build := range map[string]func() ([]byte, error){
		"Subscribe":   func() ([]byte, error) { return (&Subscribe{LabelFilter: original}).MarshalVT() },
		"Unsubscribe": func() ([]byte, error) { return (&Unsubscribe{LabelFilter: original}).MarshalVT() },
		"Disconnect":  func() ([]byte, error) { return (&Disconnect{LabelFilter: original}).MarshalVT() },
		"Refresh":     func() ([]byte, error) { return (&Refresh{LabelFilter: original}).MarshalVT() },
	} {
		t.Run(name, func(t *testing.T) {
			data, err := build()
			require.NoError(t, err)

			var got *FilterNode
			switch name {
			case "Subscribe":
				m := &Subscribe{}
				require.NoError(t, m.UnmarshalVT(data))
				got = m.LabelFilter
			case "Unsubscribe":
				m := &Unsubscribe{}
				require.NoError(t, m.UnmarshalVT(data))
				got = m.LabelFilter
			case "Disconnect":
				m := &Disconnect{}
				require.NoError(t, m.UnmarshalVT(data))
				got = m.LabelFilter
			case "Refresh":
				m := &Refresh{}
				require.NoError(t, m.UnmarshalVT(data))
				got = m.LabelFilter
			}
			require.Equal(t, original.Op, got.Op)
			require.Len(t, got.Nodes, 2)
			require.Equal(t, "region", got.Nodes[0].Key)
			require.Equal(t, "us-east", got.Nodes[0].Val)
			require.Equal(t, "or", got.Nodes[1].Op)
			require.Equal(t, []string{"premium", "enterprise"}, got.Nodes[1].Nodes[0].Vals)
			require.Equal(t, "not", got.Nodes[1].Nodes[1].Op)
			require.Equal(t, "blocked", got.Nodes[1].Nodes[1].Nodes[0].Key)
		})
	}
}
