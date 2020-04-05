package recovery

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestPubSubSyncer(t *testing.T) {
	syncer := NewPubSubSync()
	require.Empty(t, syncer.subSync)

	channels := []string{"ch1", "ch2", "ch3"}

	var wg sync.WaitGroup
	wg.Add(len(channels))
	for _, channel := range channels {
		go func(channel string) {
			defer wg.Done()
			done := make(chan struct{}, 1)
			wait := make(chan struct{}, 1)

			syncer.StartBuffering(channel)
			go func() {
				for i := 0; i < 10; i++ {
					syncer.SyncPublication(channel, &protocol.Publication{}, func() {})
				}
				close(wait)
			}()
			go func() {
				<-wait
				syncer.LockBuffer(channel)
				pubs := syncer.ReadBuffered(channel)
				require.Equal(t, 10, len(pubs))
				syncer.StopBuffering(channel)
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

	require.Empty(t, syncer.subSync)
}

func BenchmarkPubSubSync(b *testing.B) {
	syncer := NewPubSubSync()
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
				syncer.StartBuffering(channel)
				go func() {
					syncer.SyncPublication(channel, &protocol.Publication{}, func() {})
					close(wait)
				}()
				<-wait
				syncer.LockBuffer(channel)
				_ = syncer.ReadBuffered(channel)
				syncer.StopBuffering(channel)
			}(channel)
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportAllocs()
}
