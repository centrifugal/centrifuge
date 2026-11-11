package websocket

import (
	"testing"
	"time"
	"unsafe"
)

func TestPingAllowance(t *testing.T) {
	c := &Conn{}
	for i := 0; i < pingBurst; i++ {
		if !c.pingAllowed() {
			t.Fatalf("ping %d of the burst refused", i)
		}
	}
	if c.pingAllowed() {
		t.Fatal("ping beyond the burst allowed")
	}
	// A second later one ping's allowance has refilled, and only one.
	c.pingTat -= int64(time.Second)
	if !c.pingAllowed() {
		t.Fatal("refilled ping refused")
	}
	if c.pingAllowed() {
		t.Fatal("more than the refill allowed")
	}
	// A peer idle for long enough gets the whole burst back, no more.
	c.pingTat -= int64(time.Hour)
	for i := 0; i < pingBurst; i++ {
		if !c.pingAllowed() {
			t.Fatalf("ping %d of the refilled burst refused", i)
		}
	}
	if c.pingAllowed() {
		t.Fatal("idle time must not accumulate beyond the burst")
	}
}

// Every connection allocates a Conn. 288 bytes is a Go allocation size class:
// growing past it moves every connection into the 320-byte class.
func TestConnStaysInItsSizeClass(t *testing.T) {
	if size := unsafe.Sizeof(Conn{}); size > 288 {
		t.Fatalf("Conn is %d bytes, beyond the 288-byte size class", size)
	}
}
