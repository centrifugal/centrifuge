// Package lazyutf8 answers whether a payload is valid UTF-8, scanning it at
// most once.
package lazyutf8

import "unicode/utf8"

// Validator reports whether the payload it holds is valid UTF-8, scanning it
// on the first call and remembering the answer.
//
// It exists because a payload's UTF-8 validity is asked for more than once
// while a publication is broadcast — once per distinct encoding combination
// among subscribers, and separately for the broker's and the node's previous
// publication — and the answer never changes. Payloads carrying non-ASCII text
// make that scan expensive: utf8.Valid has an ASCII fast path, so 16 KiB of
// ASCII JSON costs around 170ns, while the same size carrying Cyrillic or
// emoji costs around 4.5µs, which is comparable to building the delta itself.
//
// Callers that may not need the answer at all should hold a Validator and ask
// only when something needs it, so a channel with no JSON subscribers never
// pays for the scan.
//
// The zero Validator reports true, which is the right answer for no payload.
// It must not be copied after first use and is not safe for concurrent use: it
// is meant to live on the stack for the duration of one broadcast.
type Validator struct {
	data   []byte
	known  bool
	result bool
}

// New returns a Validator for data. It does not read data.
func New(data []byte) Validator {
	return Validator{data: data}
}

// Bytes returns the payload.
func (v *Validator) Bytes() []byte {
	return v.data
}

// Valid reports whether the payload is valid UTF-8, scanning it the first time
// it is asked and reusing the answer afterwards.
func (v *Validator) Valid() bool {
	if !v.known {
		v.result = utf8.Valid(v.data)
		v.known = true
	}
	return v.result
}

// Scanned reports whether the payload has been scanned yet. Tests use it to
// check that a scan was avoided, or not repeated.
func (v *Validator) Scanned() bool {
	return v.known
}
