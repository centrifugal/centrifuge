//go:build go1.20

package centrifuge

import (
	"net/http"
)

func newResponseController(w http.ResponseWriter) responseController {
	return http.NewResponseController(w)
}
