package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogLevelToString(t *testing.T) {
	level := LogLevelToString(LogLevelDebug)
	require.Equal(t, "debug", level)
}

type testHandler struct {
	count int
}

func (h *testHandler) Handle(_ LogEntry) {
	h.count++
}

func TestLogger(t *testing.T) {
	h := testHandler{}
	l := newLogger(LogLevelError, h.Handle)
	require.NotNil(t, l)
	l.log(newLogEntry(LogLevelDebug, "test"))
	require.Equal(t, 0, h.count)
	l.log(newLogEntry(LogLevelError, "test"))
	require.Equal(t, 1, h.count)
	require.False(t, l.enabled(LogLevelDebug))
	require.True(t, l.enabled(LogLevelError))
}

func TestNewLogEntry(t *testing.T) {
	entry := newLogEntry(LogLevelDebug, "test")
	require.Equal(t, LogLevelDebug, entry.Level)
	require.Equal(t, "test", entry.Message)
	require.Nil(t, entry.Fields)

	entry = newLogEntry(LogLevelError, "test", map[string]any{"one": true})
	require.Equal(t, LogLevelError, entry.Level)
	require.Equal(t, "test", entry.Message)
	require.NotNil(t, entry.Fields)
	require.Equal(t, true, entry.Fields["one"].(bool))
}
