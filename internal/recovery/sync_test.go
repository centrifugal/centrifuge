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
				psSync.LockBuffer(channel)
				pubs := psSync.ReadBuffered(channel)
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
				psSync.LockBuffer(channel)
				_ = psSync.ReadBuffered(channel)
				psSync.StopBuffering(channel)
			}(channel)
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportAllocs()
}
