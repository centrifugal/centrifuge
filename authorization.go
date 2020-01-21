package centrifuge

type Authorization interface {
	VerifyConnectToken(token string) (Token, error)
	VerifySubscribeToken(token string) (Token, error)
	Reload(config Config)
}

type Token struct {
	// UserID tells library an ID of connecting user.
	UserID string
	// ExpireAt allows to set time in future when connection must be validated.
	// In this case OnRefresh callback must be set by application.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Presense information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
	// Used in private channel
	Channel string
}
