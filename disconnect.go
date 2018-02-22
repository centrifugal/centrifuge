package centrifuge

var (
	disconnectReasonNormal      = "normal"
	disconnectReasonShutdown    = "shutdown"
	disconnectReasonInvalidSign = "invalid sign"
	disconnectReasonBadRequest  = "bad request"
	disconnectReasonServerError = "internal server error"
)

// Disconnect allows to configure how client will be disconnected from server.
type Disconnect struct {
	Reason    string `json:"reason"`
	Reconnect bool   `json:"reconnect"`
}

// Some predefined disconnect structures. Though it's always
// possible to create Disconnect with any field values on the fly.
var (
	// DisconnectNormal ...
	DisconnectNormal = &Disconnect{
		Reason:    disconnectReasonNormal,
		Reconnect: true,
	}
	// DisconnectShutdown ...
	DisconnectShutdown = &Disconnect{
		Reason:    disconnectReasonShutdown,
		Reconnect: true,
	}
	// DisconnectInvalidSign ...
	DisconnectInvalidSign = &Disconnect{
		Reason:    disconnectReasonInvalidSign,
		Reconnect: false,
	}
	// DisconnectBadRequest ...
	DisconnectBadRequest = &Disconnect{
		Reason:    disconnectReasonBadRequest,
		Reconnect: false,
	}
	// DisconnectServerError ...
	DisconnectServerError = &Disconnect{
		Reason:    disconnectReasonServerError,
		Reconnect: true,
	}
)
