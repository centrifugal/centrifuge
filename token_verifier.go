package centrifuge

type tokenVerifier interface {
	VerifyConnectToken(token string) (connectToken, error)
	VerifySubscribeToken(token string) (subscribeToken, error)
	Reload(config Config)
}

type connectToken struct {
	// Client tells library an ID of connecting user.
	UserID string
	// ExpireAt allows to set time in future when connection must be validated.
	// In this case OnRefresh callback must be set by application.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Presence information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
}

type subscribeToken struct {
	// Client tells library an ID of connecting user.
	Client string
	// ExpireAt allows to set time in future when connection must be validated.
	// In this case OnRefresh callback must be set by application.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Presence information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
	// Used in private channel
	Channel string
}
