package partition

import (
	"github.com/centrifugal/centrifuge/internal/convert"
)

func crc16(data []byte) uint16 {
	var crc uint16 = 0x0000
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if (crc & 0x8000) != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc & 0xFFFF
}

// Compute computes the partition number for a given channel name.
// It uses the CRC16-CCITT-FALSE algorithm and takes the modulo with numPartitions.
func Compute(channel string, numPartitions uint16) uint16 {
	crc := crc16(convert.StringToBytes(channel))
	partition := crc % numPartitions
	return partition
}

// computeSlot computes the Redis slot for a given key, considering the hash tag if present.
func computeSlot(key string) uint16 {
	// Extract hash tag if present
	tag := getHashTag(key)
	if tag == "" {
		tag = key
	}
	crc := crc16([]byte(tag))
	slot := crc % 16384
	return slot
}

// getHashTag extracts the hash tag from a Redis key if it exists.
func getHashTag(key string) string {
	start := -1
	for i, c := range key {
		if c == '{' {
			start = i
		} else if c == '}' && start != -1 && i > start+1 {
			return key[start+1 : i]
		}
	}
	return ""
}
