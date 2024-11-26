package partition

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionerNew(t *testing.T) {
	t.Skip()
	for i := 0; i < 16; i++ {
		channelName := fmt.Sprintf("channel%d", i)
		channelWithHashTag := channelName + "{" + strconv.Itoa(int(Compute(channelName, 8))) + "}"
		fmt.Printf("Channel: %s\n", channelWithHashTag)
		fmt.Printf("Slot: %d\n", computeSlot(channelWithHashTag))
	}
}

func TestComputePartition(t *testing.T) {
	partition := Compute("channel1", 8)
	require.Equal(t, uint16(0), partition)
	partition = Compute("channel2", 8)
	require.Equal(t, uint16(3), partition)
}
