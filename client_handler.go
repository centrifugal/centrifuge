package centrifuge

import (
	"context"
)

type ClientHandler struct {
	node          *Node
	ruleContainer *ChannelRuleContainer
	tokenVerifier TokenVerifier
}

func NewClientHandler(node *Node, ruleContainer *ChannelRuleContainer, tokenVerifier TokenVerifier) *ClientHandler {
	return &ClientHandler{node: node, ruleContainer: ruleContainer, tokenVerifier: tokenVerifier}
}

func (n *ClientHandler) OnConnecting(_ context.Context, _ TransportInfo, e ConnectEvent) ConnectReply {
	var (
		credentials *Credentials
		channels    []string
	)

	if e.Token != "" {
		token, err := n.tokenVerifier.VerifyConnectToken(e.Token)
		if err != nil {
			if err == ErrTokenExpired {
				return ConnectReply{Error: ErrorTokenExpired}
			}
			n.node.logger.log(newLogEntry(LogLevelInfo, "invalid connection token", map[string]interface{}{"error": err.Error(), "client": e.ClientID}))
			return ConnectReply{Disconnect: DisconnectInvalidToken}
		}

		credentials = &Credentials{
			UserID:   token.UserID,
			ExpireAt: token.ExpireAt,
			Info:     token.Info,
		}

		if n.ruleContainer.config.ClientInsecure {
			credentials.ExpireAt = 0
		}

		channels = append(channels, token.Channels...)

		if n.ruleContainer.config.UserSubscribeToPersonal && token.UserID != "" {
			channels = append(channels, n.ruleContainer.personalChannel(token.UserID))
		}
	}

	if credentials == nil && (n.ruleContainer.config.ClientAnonymous || n.ruleContainer.config.ClientInsecure) {
		credentials = &Credentials{
			UserID: "",
		}
	}

	return ConnectReply{
		Credentials:       credentials,
		Channels:          channels,
		ClientSideRefresh: true,
	}
}

func (n *ClientHandler) OnRefresh(c *Client, e RefreshEvent) RefreshReply {
	token, err := n.tokenVerifier.VerifyConnectToken(e.Token)
	if err != nil {
		if err == ErrTokenExpired {
			return RefreshReply{Expired: true}
		}
		n.node.logger.log(newLogEntry(LogLevelInfo, "invalid refresh token", map[string]interface{}{"error": err.Error(), "client": c.ID()}))
		return RefreshReply{Disconnect: DisconnectInvalidToken}
	}
	return RefreshReply{
		ExpireAt: token.ExpireAt,
		Info:     token.Info,
	}
}

func (n *ClientHandler) OnSubRefresh(c *Client, e SubRefreshEvent) SubRefreshReply {
	token, err := n.tokenVerifier.VerifySubscribeToken(e.Token)
	if err != nil {
		if err == ErrTokenExpired {
			return SubRefreshReply{Expired: true}
		}
		c.node.logger.log(newLogEntry(LogLevelInfo, "invalid subscription refresh token", map[string]interface{}{"error": err.Error(), "client": c.ID(), "user": c.UserID()}))
		return SubRefreshReply{Disconnect: DisconnectInvalidToken}
	}
	if c.ID() != token.Client || e.Channel != token.Channel {
		return SubRefreshReply{Disconnect: DisconnectInvalidToken}
	}
	return SubRefreshReply{
		ExpireAt: token.ExpireAt,
		Info:     token.Info,
	}
}

func (n *ClientHandler) OnSubscribe(c *Client, e SubscribeEvent) SubscribeReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "subscribe channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{Error: toClientErr(err)}
	}

	if chOpts.ServerSide {
		n.node.logger.log(newLogEntry(LogLevelInfo, "attempt to subscribe on server side channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{Error: ErrorPermissionDenied}
	}

	if !n.ruleContainer.userAllowed(e.Channel, c.UserID()) {
		n.node.logger.log(newLogEntry(LogLevelInfo, "user is not allowed to subscribe on channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{Error: ErrorPermissionDenied}
	}

	if !chOpts.Anonymous && c.user == "" && !n.ruleContainer.config.ClientInsecure {
		n.node.logger.log(newLogEntry(LogLevelInfo, "anonymous user is not allowed to subscribe on channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{Error: ErrorPermissionDenied}
	}

	var (
		channelInfo []byte
		expireAt    int64
	)

	if n.ruleContainer.isTokenChannel(e.Channel) {
		if e.Token == "" {
			n.node.logger.log(newLogEntry(LogLevelInfo, "subscription token required", map[string]interface{}{"client": c.uid, "user": c.UserID()}))
			return SubscribeReply{Error: ErrorPermissionDenied}
		}
		token, err := n.tokenVerifier.VerifySubscribeToken(e.Token)
		if err != nil {
			if err == ErrTokenExpired {
				return SubscribeReply{Error: ErrorTokenExpired}
			}
			n.node.logger.log(newLogEntry(LogLevelInfo, "invalid subscription token", map[string]interface{}{"error": err.Error(), "client": c.ID(), "user": c.UserID()}))
			return SubscribeReply{Error: ErrorPermissionDenied}
		}
		if c.ID() != token.Client || e.Channel != token.Channel {
			return SubscribeReply{Error: ErrorPermissionDenied}
		}
		expireAt = token.ExpireAt
		if token.ExpireTokenOnly {
			expireAt = 0
		}
		channelInfo = token.Info
	}

	return SubscribeReply{
		ExpireAt:          expireAt,
		ChannelInfo:       channelInfo,
		ClientSideRefresh: true,
	}
}

func (n *ClientHandler) OnPublish(c *Client, e PublishEvent) PublishReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "publish channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return PublishReply{Error: toClientErr(err)}
	}

	if chOpts.SubscribeToPublish {
		if _, ok := c.Channels()[e.Channel]; !ok {
			return PublishReply{Error: ErrorPermissionDenied}
		}
	}

	if !chOpts.Publish && !n.ruleContainer.config.ClientInsecure {
		return PublishReply{Error: ErrorPermissionDenied}
	}

	return PublishReply{}
}

func (n *ClientHandler) OnPresence(c *Client, e PresenceEvent) PresenceReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "presence channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return PresenceReply{Error: toClientErr(err)}
	}
	if chOpts.PresenceDisableForClient {
		return PresenceReply{Error: ErrorNotAvailable}
	}
	if _, ok := c.Channels()[e.Channel]; !ok {
		return PresenceReply{Error: ErrorPermissionDenied}
	}
	return PresenceReply{}
}

func (n *ClientHandler) OnPresenceStats(c *Client, e PresenceStatsEvent) PresenceStatsReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "presence stats channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return PresenceStatsReply{Error: toClientErr(err)}
	}
	if chOpts.PresenceDisableForClient {
		return PresenceStatsReply{Error: ErrorNotAvailable}
	}
	if _, ok := c.Channels()[e.Channel]; !ok {
		return PresenceStatsReply{Error: ErrorPermissionDenied}
	}
	return PresenceStatsReply{}
}

func (n *ClientHandler) OnHistory(c *Client, e HistoryEvent) HistoryReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "history channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return HistoryReply{Error: toClientErr(err)}
	}
	if chOpts.HistoryDisableForClient {
		return HistoryReply{Error: ErrorNotAvailable}
	}
	if _, ok := c.Channels()[e.Channel]; !ok {
		return HistoryReply{Error: ErrorPermissionDenied}
	}
	return HistoryReply{}
}
