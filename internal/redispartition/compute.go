package redispartition

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
)

// ComputeTags computes a tag set for numPartitions from scratch via
// simulated annealing. Output is deterministic for a given numPartitions
// — the seed is derived from numPartitions, so two independent runs
// produce byte-identical output. Used only by cmd/precompute; runtime
// code uses FindTags instead.
//
// Note: the cold path allocates O(numPartitions × 16384) ints and is not
// suitable for runtime invocation with large values.
func ComputeTags(numPartitions int) ([]string, error) {
	if numPartitions < 1 || numPartitions > totalSlots {
		return nil, fmt.Errorf("numPartitions must be between 1 and %d, got %d", totalSlots, numPartitions)
	}

	slots := findBalancedSlots(numPartitions)
	tags, err := findStringTags(slots)
	if err != nil {
		return nil, err
	}

	sort.Ints(slots)
	result := make([]string, numPartitions)
	for i, s := range slots {
		result[i] = tags[s]
	}
	return result, nil
}

func viol(c, lo, hi int) int {
	if c > hi {
		return c - hi
	}
	if c < lo {
		return lo - c
	}
	return 0
}

// findBalancedSlots uses simulated annealing to find numPartitions slots
// that distribute evenly across cluster sizes 1..numPartitions.
func findBalancedSlots(numPartitions int) []int {
	// Deterministic seed: same numPartitions always produces the same tags.
	// 100003 is an arbitrary prime used to scatter consecutive partition
	// counts into distant seeds. Do not change — every entry in
	// precomputed.go was generated against this seed; altering it would
	// invalidate the bundled tables.
	rng := rand.New(rand.NewSource(int64(numPartitions) * 100003)) //nolint:gosec // Determinism is required; not security-sensitive.

	maxCluster := numPartitions

	// Precompute node mappings for each cluster size.
	nodeOf := make([][]int, maxCluster+1)
	for k := 1; k <= maxCluster; k++ {
		m := make([]int, totalSlots)
		sn := totalSlots / k
		r := totalSlots % k
		b := r * (sn + 1)
		for s := 0; s < totalSlots; s++ {
			if s < b {
				m[s] = s / (sn + 1)
			} else {
				m[s] = r + (s-b)/sn
			}
		}
		nodeOf[k] = m
	}

	// Target counts per node for each cluster size.
	type target struct{ lo, hi int }
	targets := make([]target, maxCluster+1)
	for k := 1; k <= maxCluster; k++ {
		lo := numPartitions / k
		hi := lo
		if numPartitions%k != 0 {
			hi = lo + 1
		}
		targets[k] = target{lo, hi}
	}

	// Initialize with evenly-spaced slots for a good starting point.
	slots := make([]int, numPartitions)
	slotSet := make(map[int]bool, numPartitions)
	for i := 0; i < numPartitions; i++ {
		s := i*totalSlots/numPartitions + totalSlots/(2*numPartitions)
		slots[i] = s
		slotSet[s] = true
	}

	// Build counts[k][node] = how many partitions land on each node.
	buildCounts := func() [][]int {
		counts := make([][]int, maxCluster+1)
		for k := 1; k <= maxCluster; k++ {
			arr := make([]int, k)
			for _, s := range slots {
				arr[nodeOf[k][s]]++
			}
			counts[k] = arr
		}
		return counts
	}

	evaluate := func(counts [][]int) int {
		v := 0
		for k := 1; k <= maxCluster; k++ {
			t := targets[k]
			for n := 0; n < k; n++ {
				v += viol(counts[k][n], t.lo, t.hi)
			}
		}
		return v
	}

	computeDelta := func(counts [][]int, oldSlot, newSlot int) int {
		d := 0
		for k := 1; k <= maxCluster; k++ {
			on := nodeOf[k][oldSlot]
			nn := nodeOf[k][newSlot]
			if on == nn {
				continue
			}
			t := targets[k]
			co, cn := counts[k][on], counts[k][nn]
			d += viol(co-1, t.lo, t.hi) + viol(cn+1, t.lo, t.hi) -
				viol(co, t.lo, t.hi) - viol(cn, t.lo, t.hi)
		}
		return d
	}

	applyMove := func(counts [][]int, oldSlot, newSlot int) {
		for k := 1; k <= maxCluster; k++ {
			on := nodeOf[k][oldSlot]
			nn := nodeOf[k][newSlot]
			if on != nn {
				counts[k][on]--
				counts[k][nn]++
			}
		}
	}

	counts := buildCounts()
	score := evaluate(counts)
	best := score
	bestSlots := make([]int, numPartitions)
	copy(bestSlots, slots)

	// Simulated annealing. With cool=0.999975 the temperature decays to
	// effectively zero well before iter exhaustion, so the search is
	// near-greedy after roughly the first million iterations. These
	// constants were tuned empirically to converge for numPartitions up
	// to 4096.
	iters := 5_000_000
	temp := 60.0
	cool := 0.999975

	for it := 0; it < iters; it++ {
		idx := rng.Intn(numPartitions)
		oldSlot := slots[idx]
		newSlot := rng.Intn(totalSlots)
		if slotSet[newSlot] {
			continue
		}

		d := computeDelta(counts, oldSlot, newSlot)

		if d <= 0 || rng.Float64() < math.Exp(-float64(d)/math.Max(temp, 0.001)) {
			applyMove(counts, oldSlot, newSlot)
			delete(slotSet, oldSlot)
			slotSet[newSlot] = true
			slots[idx] = newSlot
			score += d
			if score < best {
				best = score
				copy(bestSlots, slots)
				if best == 0 {
					return bestSlots
				}
			}
		}
		temp *= cool
	}

	return bestSlots
}

// findStringTags finds short alphanumeric strings that hash to each target slot.
func findStringTags(slots []int) (map[int]string, error) {
	needed := make(map[int]bool, len(slots))
	for _, s := range slots {
		needed[s] = true
	}
	result := make(map[int]string, len(slots))

	chars := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	buf := make([]byte, 0, 8)

	var generate func(length, pos int) bool
	generate = func(length, pos int) bool {
		if pos == length {
			s := int(crc16(buf)) % totalSlots
			if needed[s] {
				if _, found := result[s]; !found {
					result[s] = string(buf)
					delete(needed, s)
					if len(needed) == 0 {
						return true
					}
				}
			}
			return false
		}
		for _, c := range chars {
			buf = append(buf, c)
			if generate(length, pos+1) {
				return true
			}
			buf = buf[:len(buf)-1]
		}
		return false
	}

	// Skip length 1: only 36 single-char tags exist, far short of the
	// number of distinct slots typically required.
	for ln := 2; ln <= 8; ln++ {
		if len(needed) == 0 {
			break
		}
		buf = buf[:0]
		generate(ln, 0)
	}

	if len(needed) > 0 {
		return nil, fmt.Errorf("failed to find tags for %d slots", len(needed))
	}
	return result, nil
}
