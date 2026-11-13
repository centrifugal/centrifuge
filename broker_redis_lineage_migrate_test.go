//go:build integration

package centrifuge

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// A slot moved to another node of a Redis Cluster. The gate adopts it under
// the target server's lineage, so its streams keep their epochs - when the
// target has one: on a server no node used yet (a node just added) the
// migrated streams start new epochs. The INFO mode can't tell a migration
// from a new lineage: migrated streams always start new epochs. Opt-in:
// CENTRIFUGE_REDIS_MIGRATE_TEST=1 (ports 7001-7003).
func TestRedisLineage_SlotMigrationModes(t *testing.T) {
	if os.Getenv("CENTRIFUGE_REDIS_MIGRATE_TEST") == "" {
		t.Skip("set CENTRIFUGE_REDIS_MIGRATE_TEST=1")
	}
	for _, tc := range []struct {
		name        string
		useInfo     bool
		targetInUse bool
	}{
		{"gate_target_in_use", false, true},
		{"gate_fresh_target", false, false},
		{"info", true, true},
	} {
		useInfo := tc.useInfo
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			addrs := []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}
			b := newLineageTestBrokerPrefix(t, lineageRedisTest{"cluster", true, 7001, true}, useInfo, getUniquePrefix())
			s := b.shards[0].shard
			wantMode := lineageGate
			if useInfo {
				wantMode = lineageInfo
			}
			mode, err := b.lin(s).mode(ctx, useInfo)
			require.NoError(t, err)
			if mode != wantMode {
				t.Skipf("mode %s, want %s", mode, wantMode)
			}

			ch := "lineage_migrate_" + randString(8)
			before := lineagePublish(t, b, ch, 3)
			require.Equal(t, uint64(3), before.Offset)

			slot := int64(redisKeySlot(string(b.historyMetaKey(s, ch))))
			from := slotOwnerForTest(t, addrs, slot)
			to := addrs[0]
			if to == from {
				to = addrs[1]
			}
			if tc.targetInUse {
				// Traffic to the target server first: it has a lineage.
				for i := 0; ; i++ {
					other := "lineage_migrate_target_" + strconv.Itoa(i)
					if slotOwnerForTest(t, addrs, int64(redisKeySlot(string(b.historyMetaKey(s, other))))) == to {
						lineagePublish(t, b, other, 1)
						break
					}
				}
			}
			migrateSlotForTest(t, addrs, slot, from, to)
			require.Equal(t, to, slotOwnerForTest(t, addrs, slot))

			var after StreamPosition
			require.Eventually(t, func() bool {
				res, err := b.Publish(ch, []byte(`{}`), PublishOptions{HistorySize: 10, HistoryTTL: time.Minute})
				if err != nil {
					return false
				}
				after = res.StreamPosition
				return true
			}, 10*time.Second, 50*time.Millisecond)
			if useInfo {
				require.NotEqual(t, before.Epoch, after.Epoch, "the INFO mode restarts a migrated stream")
				require.Equal(t, uint64(1), after.Offset)
			} else if tc.targetInUse {
				require.Equal(t, before.Epoch, after.Epoch, "the gate adopts a migrated slot")
				require.Equal(t, uint64(4), after.Offset)
			} else {
				// A server no node has used yet holds no lineage of its own to
				// adopt the slot under: a migration looks like a restart there.
				require.NotEqual(t, before.Epoch, after.Epoch, "a fresh target restarts migrated streams")
				require.Equal(t, uint64(1), after.Offset)
			}
		})
	}
}

func slotOwnerForTest(t *testing.T, addrs []string, slot int64) string {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addrs[0]}, ForceSingleClient: true})
	require.NoError(t, err)
	defer c.Close()
	raw, err := c.Do(context.Background(), c.B().ClusterSlots().Build()).ToArray()
	require.NoError(t, err)
	for _, r := range raw {
		e, _ := r.ToArray()
		lo, _ := e[0].AsInt64()
		hi, _ := e[1].AsInt64()
		if slot >= lo && slot <= hi {
			m, _ := e[2].ToArray()
			port, _ := m[1].AsInt64()
			return "127.0.0.1:" + strconv.FormatInt(port, 10)
		}
	}
	t.Fatal("no owner")
	return ""
}

// migrateSlotForTest moves a slot's keys from one node to another with
// MIGRATE and assigns the slot to it on every node.
func migrateSlotForTest(t *testing.T, addrs []string, slot int64, from, to string) {
	t.Helper()
	ctx := context.Background()
	direct := map[string]rueidis.Client{}
	ids := map[string]string{}
	for _, a := range addrs {
		c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{a}, ForceSingleClient: true})
		require.NoError(t, err)
		defer c.Close()
		direct[a] = c
		id, err := c.Do(ctx, c.B().ClusterMyid().Build()).ToString()
		require.NoError(t, err)
		ids[a] = id
	}
	src, dst := direct[from], direct[to]
	require.NoError(t, dst.Do(ctx, dst.B().ClusterSetslot().Slot(slot).Importing().NodeId(ids[from]).Build()).Error())
	require.NoError(t, src.Do(ctx, src.B().ClusterSetslot().Slot(slot).Migrating().NodeId(ids[to]).Build()).Error())
	host, portStr, _ := net.SplitHostPort(to)
	port, _ := strconv.ParseInt(portStr, 10, 64)
	for {
		keys, err := src.Do(ctx, src.B().ClusterGetkeysinslot().Slot(slot).Count(100).Build()).AsStrSlice()
		require.NoError(t, err)
		if len(keys) == 0 {
			break
		}
		require.NoError(t, src.Do(ctx, src.B().Migrate().Host(host).Port(port).Key("").DestinationDb(0).Timeout(5000).Keys(keys...).Build()).Error())
	}
	for _, a := range append([]string{to, from}, addrs...) {
		require.NoError(t, direct[a].Do(ctx, direct[a].B().ClusterSetslot().Slot(slot).Node().NodeId(ids[to]).Build()).Error())
	}
}
