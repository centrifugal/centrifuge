package recovery

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestPubSubSync(t *testing.T) {
	psSync := NewPubSubSync()
	require.Empty(t, psSync.subSync)

	channels := []string{"ch1", "ch2", "ch3"}

	var wg sync.WaitGroup
	wg.Add(len(channels))
	for _, channel := range channels {
		go func(channel string) {
			defer wg.Done()
			done := make(chan struct{}, 1)
			wait := make(chan struct{}, 1)

			psSync.StartBuffering(channel)
			go func() {
				for i := 0; i < 10; i++ {
					psSync.SyncPublication(channel, &protocol.Publication{}, func() {})
				}
				close(wait)
			}()
			go func() {
				<-wait
				pubs := psSync.LockBufferAndReadBuffered(channel)
				require.Equal(t, 10, len(pubs))
				psSync.StopBuffering(channel)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				require.Fail(t, "timeout")
			}
		}(channel)
	}
	wg.Wait()

	require.Empty(t, psSync.subSync)
}

// TestPubSubSyncNonSynchronized reproduces a deadlock reported in
// https://github.com/centrifugal/centrifugo/issues/486 when run under
// stress (multiple run).
func TestPubSubSyncNonSynchronized(t *testing.T) {
	psSync := NewPubSubSync()
	require.Empty(t, psSync.subSync)

	channels := []string{"ch1", "ch2", "ch3"}

	var wg sync.WaitGroup
	wg.Add(len(channels))
	for _, channel := range channels {
		go func(channel string) {
			defer wg.Done()
			done := make(chan struct{}, 1)

			psSync.StartBuffering(channel)
			go func() {
				for i := 0; i < 10; i++ {
					select {
					case <-done:
						return
					default:
					}
					psSync.SyncPublication(channel, &protocol.Publication{}, func() {})
				}
			}()
			go func() {
				psSync.LockBufferAndReadBuffered(channel)
				psSync.StopBuffering(channel)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				require.Fail(t, "timeout")
			}
		}(channel)
	}
	wg.Wait()

	require.Empty(t, psSync.subSync)
}

func TestPubSubSyncNotBuffering(t *testing.T) {
	psSync := NewPubSubSync()

	synced := 0
	psSync.SyncPublication("ch", &protocol.Publication{Offset: 1}, func() { synced++ })
	require.Equal(t, 1, synced, "publication must pass through when channel is not buffering")

	require.Nil(t, psSync.LockBufferAndReadBuffered("ch"))
	psSync.StopBuffering("ch") // No-op for a channel which is not buffering.
	require.Empty(t, psSync.subSync)

	psSync.StartBuffering("ch")
	psSync.SyncPublication("ch", &protocol.Publication{Offset: 2}, func() { synced++ })
	require.Equal(t, 1, synced, "publication must be buffered while subscribing")
	pubs := psSync.LockBufferAndReadBuffered("ch")
	require.Len(t, pubs, 1)
	require.Equal(t, uint64(2), pubs[0].Offset)
	psSync.StopBuffering("ch")

	psSync.SyncPublication("ch", &protocol.Publication{Offset: 3}, func() { synced++ })
	require.Equal(t, 2, synced, "publication must pass through after buffering stopped")
	require.Empty(t, psSync.subSync)
}

func BenchmarkPubSubSync(b *testing.B) {
	psSync := NewPubSubSync()
	var channels []string
	for i := 0; i < 1; i++ {
		channels = append(channels, "server-side-"+strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(len(channels))
		for _, channel := range channels {
			go func(channel string) {
				defer wg.Done()
				wait := make(chan struct{}, 1)
				psSync.StartBuffering(channel)
				go func() {
					psSync.SyncPublication(channel, &protocol.Publication{}, func() {})
					close(wait)
				}()
				<-wait
				_ = psSync.LockBufferAndReadBuffered(channel)
				psSync.StopBuffering(channel)
			}(channel)
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportAllocs()
}
