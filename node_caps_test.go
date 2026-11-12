package centrifuge

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/controlpb"
	"github.com/stretchr/testify/require"
)

func TestNodeRegistry_Caps(t *testing.T) {
	r := newNodeRegistry("self")
	require.False(t, r.haveAll(nodeCapLineageObs), "no nodes known")
	r.add(&controlpb.Node{Uid: "self", Caps: nodeCaps})
	r.add(&controlpb.Node{Uid: "b", Caps: nodeCaps})
	require.True(t, r.haveAll(nodeCapLineageObs))

	// An older node joins: not every node strips observations.
	r.add(&controlpb.Node{Uid: "old"})
	require.False(t, r.haveAll(nodeCapLineageObs))
	require.True(t, r.lastDeparture().IsZero())

	r.remove("old")
	require.True(t, r.haveAll(nodeCapLineageObs))
	require.False(t, r.lastDeparture().IsZero(), "a departure is recorded")

	// Info updates without metrics keep the caps.
	r.add(&controlpb.Node{Uid: "b", Caps: nodeCaps, Metrics: &controlpb.Metrics{}})
	r.add(&controlpb.Node{Uid: "b", Caps: nodeCaps})
	require.True(t, r.haveAll(nodeCapLineageObs))
}
