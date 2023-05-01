//go:build !go1.20

package centrifuge

import (
	"net/http"
	"time"
)

// legacyResponseController will be removed as Go 1.21 released. We need to support Go 1.19 till that moment.
type legacyResponseController struct {
	w http.ResponseWriter
}

func (rc *legacyResponseController) Flush() error {
	rc.w.(http.Flusher).Flush()
	return nil
}

func (rc *legacyResponseController) SetWriteDeadline(time time.Time) error {
	return nil
}

func newResponseController(w http.ResponseWriter) responseController {
	return &legacyResponseController{w}
}
