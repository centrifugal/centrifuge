package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringInSlice(t *testing.T) {
	assert.True(t, stringInSlice("test", []string{"boom", "test"}))
	assert.False(t, stringInSlice("test", []string{"boom", "testing"}))
}
