package centrifuge

import (
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetWatermarks(t *testing.T) {
	w := newOffsetWatermarks()
	track := func() bool { return true }

	for off := uint64(1); off <= 3; off++ {
		e, check := w.observe("ch", "E", off, int64(off*100), track)
		require.Equal(t, "E", e)
		require.Nil(t, check)
	}

	// Offset 2 again: a suspect, checked against the publication at 3.
	e, check := w.observe("ch", "E", 2, 999, track)
	require.Equal(t, "E", e, "delivered as is until verified")
	require.NotNil(t, check)
	require.Equal(t, watermarkCheck{ch: "ch", epoch: "E", offset: 3, time: 300, got: 2}, *check)
	// One check at a time.
	_, again := w.observe("ch", "E", 2, 999, track)
	require.Nil(t, again)

	// A late message after all: nothing lost.
	w.settle(check, false)
	e, check = w.observe("ch", "E", 4, 400, track)
	require.Equal(t, "E", e)
	require.Nil(t, check)

	// A confirmed loss: the epoch's later publications are delivered as lost.
	_, check = w.observe("ch", "E", 4, 401, track)
	require.NotNil(t, check)
	w.settle(check, true)
	e, check = w.observe("ch", "E", 5, 500, track)
	require.Equal(t, "E"+lostEpochSuffix, e)
	require.Nil(t, check)

	// The new epoch after the reset is delivered as is.
	e, check = w.observe("ch", "E2", 1, 600, track)
	require.Equal(t, "E2", e)
	require.Nil(t, check)

	// Publications without offsets aren't tracked.
	e, check = w.observe("other", "E", 0, 0, track)
	require.Equal(t, "E", e)
	require.Nil(t, check)
}

func TestOffsetWatermarks_UntrackedAndForget(t *testing.T) {
	w := newOffsetWatermarks()
	none := func() bool { return false }
	_, _ = w.observe("ch", "E", 5, 1, none)
	_, check := w.observe("ch", "E", 1, 1, none)
	require.Nil(t, check, "no local subscribers: nothing tracked")
	require.Empty(t, w.shard("ch").m)

	track := func() bool { return true }
	_, _ = w.observe("ch", "E", 5, 1, track)
	w.forget("ch")
	_, check = w.observe("ch", "E", 1, 1, track)
	require.Nil(t, check, "forgotten on unsubscribe: starts again")
}

func BenchmarkOffsetWatermarks_Observe(b *testing.B) {
	w := newOffsetWatermarks()
	track := func() bool { return true }
	chs := make([]string, 1024)
	for i := range chs {
		chs[i] = "channel:" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var off uint64
		for pb.Next() {
			off++
			_, _ = w.observe(chs[off%1024], "epoch", off, int64(off), track)
		}
	})
}

// Memory per tracked channel.
func TestOffsetWatermarks_Memory(t *testing.T) {
	if os.Getenv("CENTRIFUGE_WATERMARK_MEMORY") == "" {
		t.Skip("set CENTRIFUGE_WATERMARK_MEMORY=1")
	}
	const n = 1_000_000
	chs := make([]string, n)
	for i := range chs {
		chs[i] = "channel:" + strconv.Itoa(i)
	}
	epoch := "Xb3kQ1"
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	w := newOffsetWatermarks()
	for i, ch := range chs {
		w.observe(ch, epoch, uint64(i+1), int64(i), func() bool { return true })
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("bytes per channel: %.1f", float64(after.HeapAlloc-before.HeapAlloc)/n)
	runtime.KeepAlive(w)
}
