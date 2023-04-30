package centrifuge

import "time"

const streamingResponseWriteTimeout = time.Second

type responseController interface {
	Flush() error
	SetWriteDeadline(time time.Time) error
}
