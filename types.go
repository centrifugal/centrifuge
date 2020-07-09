package centrifuge

// Publication ...
type Publication struct {
	Offset uint64      `json:"offset"`
	Data   []byte      `json:"data"`
	Info   *ClientInfo `json:"info,omitempty"`
}

// ClientInfo ...
type ClientInfo struct {
	ClientID string `json:"client"`
	UserID   string `json:"user"`
	ConnInfo []byte `json:"conn_info"`
	ChanInfo []byte `json:"chan_info"`
}
