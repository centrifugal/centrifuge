package iouring

import (
	"fmt"
	"os"
	"testing"
)

func testSubmitRequests(t *testing.T, nreqs uint) {
	f, err := os.Open("/dev/zero") // For read access.
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	iour, err := New(nreqs)
	if err != nil {
		t.Fatal(err)
	}
	defer iour.Close()

	fd := int(f.Fd())
	for iter := 0; iter < 2; iter++ {
		var offset uint64
		preqs := make([]PrepRequest, nreqs)
		bufs := make([][]byte, nreqs)
		for i := range preqs {
			bufs[i] = make([]byte, 2)
			preqs[i] = Pread(fd, bufs[i], offset)
			offset += 2
		}

		requests, err := iour.SubmitRequests(preqs, nil)
		if err != nil {
			t.Fatal(err)
		}
		<-requests.Done()
		errResults := requests.ErrResults()
		if errResults != nil {
			t.Fatal(errResults[0].Err())
		}
	}
}

func TestSubmitRequests(t *testing.T) {
	for i := uint(0); i < 8; i++ {
		nreqs := uint(1 << i)
		t.Run(fmt.Sprintf("%d", nreqs), func(t *testing.T) { testSubmitRequests(t, nreqs) })
	}
}
