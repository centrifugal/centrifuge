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
		e, lost := w.observe("ch", "E", off, track)
		require.Equal(t, "E", e)
		require.False(t, lost)
	}

	// The server gave offset 2 again: lost, once.
	e, lost := w.observe("ch", "E", 2, track)
	require.Equal(t, "E"+lostEpochSuffix, e)
	require.True(t, lost)

	// Every later publication of the lost epoch is delivered as lost.
	e, lost = w.observe("ch", "E", 4, track)
	require.Equal(t, "E"+lostEpochSuffix, e)
	require.False(t, lost)

	// The new epoch after the reset is delivered as is.
	e, lost = w.observe("ch", "E2", 1, track)
	require.Equal(t, "E2", e)
	require.False(t, lost)

	// The same offset twice is a reuse too.
	_, lost = w.observe("ch", "E2", 1, track)
	require.True(t, lost)

	// Publications without offsets aren't tracked.
	e, lost = w.observe("other", "E", 0, track)
	require.Equal(t, "E", e)
	require.False(t, lost)
}

func TestOffsetWatermarks_UntrackedAndForget(t *testing.T) {
	w := newOffsetWatermarks()
	none := func() bool { return false }
	_, _ = w.observe("ch", "E", 5, none)
	_, lost := w.observe("ch", "E", 1, none)
	require.False(t, lost, "no local subscribers: nothing tracked")
	require.Empty(t, w.shard("ch").m)

	track := func() bool { return true }
	_, _ = w.observe("ch", "E", 5, track)
	w.forget("ch")
	_, lost = w.observe("ch", "E", 1, track)
	require.False(t, lost, "forgotten on unsubscribe: starts again")
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
			_, _ = w.observe(chs[off%1024], "epoch", off, track)
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
		w.observe(ch, epoch, uint64(i+1), func() bool { return true })
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("bytes per channel: %.1f", float64(after.HeapAlloc-before.HeapAlloc)/n)
	runtime.KeepAlive(w)
}
