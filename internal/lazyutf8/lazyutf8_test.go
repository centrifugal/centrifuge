package lazyutf8

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidMatchesUTF8Valid(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"ascii", []byte("plain ascii payload")},
		{"cyrillic", []byte("Съешь же ещё этих мягких французских булок")},
		{"emoji", []byte("status 🟢 and 🔴")},
		{"lone continuation byte", []byte("ok\x80bad")},
		{"truncated rune", []byte("ok\xd0")},
		{"0xff", []byte("\xff")},
		{"valid then invalid", []byte("тест\xff")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := New(tc.data)
			if got, want := v.Valid(), utf8.Valid(tc.data); got != want {
				t.Fatalf("Valid() = %v, utf8.Valid says %v", got, want)
			}
		})
	}
}

// The point of the type: the payload is read once however many times it is
// asked about.
func TestScansAtMostOnce(t *testing.T) {
	v := New([]byte("Съешь же ещё этих мягких французских булок"))
	if v.Scanned() {
		t.Fatal("scanned before being asked")
	}
	first := v.Valid()
	if !v.Scanned() {
		t.Fatal("not marked scanned after being asked")
	}
	for range 10 {
		if v.Valid() != first {
			t.Fatal("answer changed between calls")
		}
	}
}

// And the other half: a caller that never asks never pays.
func TestNeverScansWhenNotAsked(t *testing.T) {
	v := New([]byte(strings.Repeat("тест", 4096)))
	if v.Scanned() {
		t.Fatal("scanned without being asked")
	}
}

func TestZeroValueIsValid(t *testing.T) {
	var v Validator
	if !v.Valid() {
		t.Fatal("the zero Validator should report valid, as utf8.Valid(nil) does")
	}
	if v.Bytes() != nil {
		t.Fatal("the zero Validator should hold no payload")
	}
}

func TestBytesReturnsThePayload(t *testing.T) {
	data := []byte("payload")
	v := New(data)
	if got := v.Bytes(); &got[0] != &data[0] {
		t.Fatal("Bytes did not return the payload it was given")
	}
}

func BenchmarkValidASCII(b *testing.B) {
	v := New([]byte(strings.Repeat(`{"id":12345,"name":"item"},`, 600)))
	b.ReportAllocs()
	for b.Loop() {
		sink = v.Valid()
	}
}

func BenchmarkValidMultibyteFirstCall(b *testing.B) {
	data := []byte(strings.Repeat(`{"name":"тест 🟢"},`, 800))
	b.ReportAllocs()
	for b.Loop() {
		v := New(data)
		sink = v.Valid()
	}
}

func BenchmarkValidMultibyteRepeat(b *testing.B) {
	v := New([]byte(strings.Repeat(`{"name":"тест 🟢"},`, 800)))
	sink = v.Valid()
	b.ReportAllocs()
	for b.Loop() {
		sink = v.Valid()
	}
}

var sink bool
