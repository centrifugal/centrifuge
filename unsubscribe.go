package centrifuge

type unsubscribeAdvice struct {
	// Code is unsubscribe code.
	Code uint32 `json:"code"`
	// Reason is a short description of unsubscribe code for humans.
	Reason string `json:"reason,omitempty"`
}

// Known unsubscribe codes. Codes sent to client connection must be kept
// in range [2000, 2999]. Unsubscribe codes >= 2500 coming from server to client
// result into resubscribe attempt.
// Codes [0, 2099] and [2500, 2599] are reserved for Centrifuge library internal use
// and must not be used by applications to create custom unsubscribe advices.
const (
	// UnsubscribeCodeClient set when unsubscribe event was initiated
	// by an explicit client-side unsubscribe call.
	UnsubscribeCodeClient uint32 = 0
	// UnsubscribeCodeDisconnect set when unsubscribe event was initiated
	// by a client disconnect process.
	UnsubscribeCodeDisconnect uint32 = 1
	// UnsubscribeCodeServer set when unsubscribe event was initiated
	// by an explicit server-side unsubscribe call.
	UnsubscribeCodeServer uint32 = 2000
	// UnsubscribeCodeInsufficient set when client unsubscribed from
	// a channel due to insufficient state in a stream. We expect client to
	// resubscribe after receiving this since it's still may be possible to
	// recover a state since known StreamPosition.
	UnsubscribeCodeInsufficient uint32 = 2500
)

// Reason for internally used unsubscribe codes.
func unsubscribeReason(code uint32) string {
	switch code {
	case UnsubscribeCodeClient:
		return "client unsubscribed"
	case UnsubscribeCodeDisconnect:
		return "client disconnected"
	case UnsubscribeCodeServer:
		return "server unsubscribe"
	case UnsubscribeCodeInsufficient:
		return "insufficient state"
	default:
		return "?"
	}
}
