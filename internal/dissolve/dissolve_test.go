package dissolve

import (
	"testing"
	"time"
)

func TestDissolver(t *testing.T) {
	d := New(1000, 16)
	d.Run()
	defer d.Close()
	ch := make(chan struct{}, 1)
	go func() {
		d.Submit(func() error {
			ch <- struct{}{}
			return nil
		})
	}()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func BenchmarkEnqueueAddConsume(b *testing.B) {
	d := New(1000, 16)
	d.Run()
	defer d.Close()
	ch := make(chan struct{}, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Submit(func() error {
			ch <- struct{}{}
			return nil
		})
		<-ch
	}
	b.StopTimer()
	b.ReportAllocs()
}
