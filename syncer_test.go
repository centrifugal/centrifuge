package centrifuge

import (
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestPubSubSyncer(t *testing.T) {
	syncer := newPubSubSyncer()
	require.Empty(t, syncer.subSync)

	channels := []string{"ch1", "ch2", "ch3"}

	var wg sync.WaitGroup
	wg.Add(len(channels))
	for _, channel := range channels {
		go func(channel string) {
			defer wg.Done()
			done := make(chan struct{}, 1)
			cont := make(chan struct{}, 1)

			syncer.setInSubscribe(channel, true)
			require.True(t, syncer.isInSubscribe(channel))
			go func() {
				for i := 0; i < 10; i++ {
					if syncer.isInSubscribe(channel) {
						syncer.lockPublications(channel)
						if syncer.isInSubscribe(channel) {
							syncer.appendToBufferedPubs(channel, &protocol.Publication{})
						}
						syncer.unlockPublications(channel, false)
					}
				}
				close(cont)
			}()
			go func() {
				<-cont
				syncer.lockPublications(channel)
				pubs := syncer.readBufferedPubs(channel)
				require.NotEmpty(t, pubs)
				syncer.setInSubscribe(channel, false)
				syncer.unlockPublications(channel, true)
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

	for _, channel := range channels {
		require.Empty(t, syncer.subSync[channel].pubBuffer)
	}
}
