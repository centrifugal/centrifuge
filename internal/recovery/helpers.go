package recovery

import (
	"slices"
	"sort"

	"github.com/centrifugal/protocol"
)

// uniqueNonFilteredPublications returns slice of unique Publications which were not filtered.
func uniqueNonFilteredPublications(s []*protocol.Publication) ([]*protocol.Publication, uint64, []uint64) {
	keys := make(map[uint64]struct{})
	list := make([]*protocol.Publication, 0, len(s))
	var skippedOffsets []uint64
	var maxSeenOffset uint64
	for _, entry := range s {
		if entry.Offset > maxSeenOffset {
			maxSeenOffset = entry.Offset
		}
		if entry.Time == -1 { // Special value -1 indicates filtered publication, see in hub.go.
			skippedOffsets = append(skippedOffsets, entry.Offset)
			continue
		}
		val := entry.Offset
		if _, value := keys[val]; !value {
			keys[val] = struct{}{}
			list = append(list, entry)
		}
	}
	return list, maxSeenOffset, skippedOffsets
}

// MergePublications allows to merge recovered pubs with buffered pubs
// collected during extracting recovered so result is ordered and with
// duplicates removed. readEpoch and readOffset are the stream position the
// recovered pubs were read at: the buffered pubs must continue it without a gap
// (one lost by PUB/SUB while subscribing), unless the read knew nothing about the
// stream (empty epoch). It reports false if the result has a gap.
func MergePublications(recoveredPubs []*protocol.Publication, bufferedPubs []*protocol.Publication, readEpoch string, readOffset uint64) ([]*protocol.Publication, uint64, bool) {
	var maxSeenOffset uint64
	if len(bufferedPubs) > 0 {
		recoveredPubs = append(recoveredPubs, bufferedPubs...)
	}
	sort.Slice(recoveredPubs, func(i, j int) bool {
		return recoveredPubs[i].Offset < recoveredPubs[j].Offset
	})
	// Always strip filtered (Time == -1) publications and record their offsets:
	// both the recovered set (server/client tags filter) and the buffered set may
	// contain filtered markers, and the returned set must never carry a marker.
	var skippedOffsets []uint64
	recoveredPubs, maxSeenOffset, skippedOffsets = uniqueNonFilteredPublications(recoveredPubs)
	if len(bufferedPubs) > 0 && readEpoch != "" && !continuesFrom(readOffset, recoveredPubs, skippedOffsets) {
		return nil, 0, false
	}
	if len(bufferedPubs) > 0 {
		if len(recoveredPubs) > 1 {
			prevOffset := recoveredPubs[0].Offset
			for _, p := range recoveredPubs[1:] {
				pubOffset := p.Offset
				expectedOffset := prevOffset + 1
				isWrongOffset := pubOffset != expectedOffset
				if isWrongOffset {
					if len(skippedOffsets) == 0 {
						return nil, 0, false
					}
					// All offsets from expectedOffset till pubOffset-1 must be in skippedOffsets.
					// Otherwise, we have a gap in recovered publications.
					for o := expectedOffset; o < pubOffset; o++ {
						if !slices.Contains(skippedOffsets, o) {
							return nil, 0, false
						}
					}
					// All offsets are present in skippedOffsets, can continue.
				}
				prevOffset = pubOffset
			}
		}
	}
	return recoveredPubs, maxSeenOffset, true
}

// continuesFrom reports whether the offsets after offset, of pubs and of filtered
// pubs, follow it without a gap.
func continuesFrom(offset uint64, pubs []*protocol.Publication, skippedOffsets []uint64) bool {
	var offsets []uint64
	for _, p := range pubs {
		if p.Offset > offset {
			offsets = append(offsets, p.Offset)
		}
	}
	for _, o := range skippedOffsets {
		if o > offset {
			offsets = append(offsets, o)
		}
	}
	slices.Sort(offsets)
	offsets = slices.Compact(offsets)
	for i, o := range offsets {
		if o != offset+uint64(i)+1 {
			return false
		}
	}
	return true
}
