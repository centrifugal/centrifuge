package recovery

import (
	"github.com/centrifugal/protocol"
)

// UniquePublications returns slice of unique Publications.
func UniquePublications(s []*protocol.Publication) []*protocol.Publication {
	keys := make(map[uint64]struct{})
	list := []*protocol.Publication{}
	for _, entry := range s {
		val := (uint64(entry.Seq))<<32 | uint64(entry.Gen)
		if _, value := keys[val]; !value {
			keys[val] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
}
