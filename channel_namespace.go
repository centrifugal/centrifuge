package centrifuge

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/centrifugal/protocol"
)

type TokenVerifierConfig struct {
	// HMACSecretKey is a secret key used to validate connection and subscription
	// tokens generated using HMAC. Zero value means that HMAC tokens won't be allowed.
	HMACSecretKey string
	// RSAPublicKey is a public key used to validate connection and subscription
	// tokens generated using RSA. Zero value means that RSA tokens won't be allowed.
	RSAPublicKey *rsa.PublicKey
}

type TokenVerifier struct {
	connectTokenVerifier   connectTokenVerifier
	subscribeTokenVerifier subscribeTokenVerifier
}

func NewTokenVerifier(config TokenVerifierConfig) *TokenVerifier {
	return &TokenVerifier{
		connectTokenVerifier:   NewConnectTokenVerifier(config.HMACSecretKey, config.RSAPublicKey),
		subscribeTokenVerifier: NewSubscribeTokenVerifier(config.HMACSecretKey, config.RSAPublicKey),
	}
}

func (n *TokenVerifier) verifyConnectToken(token string) (connectToken, error) {
	return n.connectTokenVerifier.VerifyConnectToken(token)
}

func (n *TokenVerifier) verifySubscribeToken(token string) (subscribeToken, error) {
	return n.subscribeTokenVerifier.VerifySubscribeToken(token)
}

// ChannelNamespace allows to create channels with different channel options.
type ChannelNamespace struct {
	// Name is a unique namespace name.
	Name string `json:"name"`

	// Options for namespace determine channel options for channels
	// belonging to this namespace.
	NamespaceChannelOptions `mapstructure:",squash"`
}

type ChannelRuleConfig struct {
	// NamespaceChannelOptions embedded on top level.
	NamespaceChannelOptions

	// Namespaces – list of namespaces for custom channel options.
	Namespaces []ChannelNamespace

	// TokenChannelPrefix is a prefix in channel name which indicates that
	// channel is private.
	TokenChannelPrefix string
	// ChannelNamespaceBoundary is a string separator which must be put after
	// namespace part in channel name.
	ChannelNamespaceBoundary string
	// ChannelUserBoundary is a string separator which must be set before allowed
	// users part in channel name.
	ChannelUserBoundary string
	// ChannelUserSeparator separates allowed users in user part of channel name.
	ChannelUserSeparator string
	// UserSubscribeToPersonal enables automatic subscribing to personal channel by user.
	// Only users with user ID defined will subscribe to personal channels, anonymous
	// users are ignored.
	UserSubscribeToPersonal bool
	// UserPersonalChannelPrefix defines prefix to be added to user personal channel.
	UserPersonalChannelNamespace string
	// ClientInsecure turns on insecure mode for client connections - when it's
	// turned on then no authentication required at all when connecting to Centrifugo,
	// anonymous access and publish allowed for all channels, no connection expire
	// performed. This can be suitable for demonstration or personal usage.
	ClientInsecure bool
	// ClientAnonymous when set to true, allows connect requests without specifying
	// a token or setting Credentials in authentication middleware. The resulting
	// user will have empty string for user ID, meaning user can only subscribe
	// to anonymous channels.
	ClientAnonymous bool
}

// DefaultRuleConfig ...
var DefaultRuleConfig = ChannelRuleConfig{
	TokenChannelPrefix:       "$", // so private channel will look like "$gossips"
	ChannelNamespaceBoundary: ":", // so namespace "public" can be used as "public:news"
	ChannelUserBoundary:      "#", // so user limited channel is "user#2694" where "2696" is user ID
	ChannelUserSeparator:     ",", // so several users limited channel is "dialog#2694,3019"
}

// ChannelOptions represent channel specific configuration for namespace
// or global channel options if set on top level of configuration.
type NamespaceChannelOptions struct {
	ChannelOptions

	// ServerSide marks all channels in namespace as server side, when on then client
	// subscribe requests to these channels will be rejected with PermissionDenied error.
	ServerSide bool `mapstructure:"server_side" json:"server_side"`

	// Publish enables possibility for clients to publish messages into channels.
	// Once enabled client can publish into channel and that publication will be
	// broadcasted to all current channel subscribers. You can control publishing
	// on server-side setting On().Publish callback to client connection.
	Publish bool `json:"publish"`

	// SubscribeToPublish turns on an automatic check that client subscribed
	// on channel before allow it to publish into that channel.
	SubscribeToPublish bool `mapstructure:"subscribe_to_publish" json:"subscribe_to_publish"`

	// Anonymous enables anonymous access (with empty user ID) to channel.
	// In most situations your application works with authenticated users so
	// every user has its own unique user ID. But if you provide real-time
	// features for public access you may need unauthenticated access to channels.
	// Turn on this option and use empty string as user ID.
	Anonymous bool `json:"anonymous"`
}

// Validate validates config and returns error if problems found
func (c *ChannelRuleConfig) Validate() error {
	errPrefix := "config error: "
	pattern := "^[-a-zA-Z0-9_.]{2,}$"
	patternRegexp, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	if c.HistoryRecover && (c.HistorySize == 0 || c.HistoryLifetime == 0) {
		return errors.New("both history size and history lifetime required for history recovery")
	}

	usePersonalChannel := c.UserSubscribeToPersonal
	personalChannelNamespace := c.UserPersonalChannelNamespace
	var validPersonalChannelNamespace bool
	if !usePersonalChannel || personalChannelNamespace == "" {
		validPersonalChannelNamespace = true
	}

	var nss = make([]string, 0, len(c.Namespaces))
	for _, n := range c.Namespaces {
		name := n.Name
		match := patternRegexp.MatchString(name)
		if !match {
			return errors.New(errPrefix + "wrong namespace name – " + name)
		}
		if stringInSlice(name, nss) {
			return errors.New(errPrefix + "namespace name must be unique")
		}
		if n.HistoryRecover && (n.HistorySize == 0 || n.HistoryLifetime == 0) {
			return fmt.Errorf("namespace %s: both history size and history lifetime required for history recovery", name)
		}
		if name == personalChannelNamespace {
			validPersonalChannelNamespace = true
		}
		nss = append(nss, name)
	}

	if !validPersonalChannelNamespace {
		return fmt.Errorf("namespace for user personal channel not found: %s", personalChannelNamespace)
	}

	return nil
}

var _ ChannelOptionsGetter = (*ChannelRuleContainer)(nil)

type ChannelRuleContainer struct {
	mu     sync.RWMutex
	config ChannelRuleConfig
}

func NewNamespaceRuleChecker(config ChannelRuleConfig) *ChannelRuleContainer {
	return &ChannelRuleContainer{
		config: config,
	}
}

// Reload node config.
func (n *ChannelRuleContainer) Reload(c ChannelRuleConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.config = c
	return nil
}

// ChannelOpts returns channel options for channel using current channel config.
func (n *ChannelRuleContainer) ChannelOptions(ch string) (ChannelOptions, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	opts, err := n.config.channelOpts(n.namespaceName(ch))
	if err != nil {
		return ChannelOptions{}, err
	}
	return opts.ChannelOptions, nil
}

// namespaceName returns namespace name from channel if exists.
func (n *ChannelRuleContainer) namespaceName(ch string) string {
	cTrim := strings.TrimPrefix(ch, n.config.TokenChannelPrefix)
	if n.config.ChannelNamespaceBoundary != "" && strings.Contains(cTrim, n.config.ChannelNamespaceBoundary) {
		parts := strings.SplitN(cTrim, n.config.ChannelNamespaceBoundary, 2)
		return parts[0]
	}
	return ""
}

// ChannelOpts returns channel options for channel using current channel config.
func (n *ChannelRuleContainer) namespacedChannelOptions(ch string) (NamespaceChannelOptions, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.config.channelOpts(n.namespaceName(ch))
}

// channelOpts searches for channel options for specified namespace key.
func (c *ChannelRuleConfig) channelOpts(namespaceName string) (NamespaceChannelOptions, error) {
	if namespaceName == "" {
		return c.NamespaceChannelOptions, nil
	}
	for _, n := range c.Namespaces {
		if n.Name == namespaceName {
			return n.NamespaceChannelOptions, nil
		}
	}
	return NamespaceChannelOptions{}, ErrorNamespaceNotFound
}

// PersonalChannel returns personal channel for user based on node configuration.
func (n *ChannelRuleContainer) personalChannel(user string) string {
	config := n.Config()
	if config.UserPersonalChannelNamespace == "" {
		return config.ChannelUserBoundary + user
	}
	return config.UserPersonalChannelNamespace + config.ChannelNamespaceBoundary + config.ChannelUserBoundary + user
}

// Config returns a copy of node Config.
func (n *ChannelRuleContainer) Config() ChannelRuleConfig {
	n.mu.RLock()
	c := n.config
	n.mu.RUnlock()
	return c
}

// privateChannel checks if channel private. In case of private channel
// subscription request must contain a proper signature.
func (n *ChannelRuleContainer) isTokenChannel(ch string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.config.TokenChannelPrefix == "" {
		return false
	}
	return strings.HasPrefix(ch, n.config.TokenChannelPrefix)
}

// userAllowed checks if user can subscribe on channel - as channel
// can contain special part in the end to indicate which users allowed
// to subscribe on it.
func (n *ChannelRuleContainer) userAllowed(ch string, user string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	userBoundary := n.config.ChannelUserBoundary
	userSeparator := n.config.ChannelUserSeparator
	if userBoundary == "" {
		return true
	}
	if !strings.Contains(ch, userBoundary) {
		return true
	}
	parts := strings.Split(ch, userBoundary)
	if userSeparator == "" {
		return parts[len(parts)-1] == user
	}
	allowedUsers := strings.Split(parts[len(parts)-1], userSeparator)
	for _, allowedUser := range allowedUsers {
		if user == allowedUser {
			return true
		}
	}
	return false
}

type ClientHandler struct {
	node          *Node
	ruleContainer *ChannelRuleContainer
	tokenVerifier *TokenVerifier
}

func NewClientHandler(node *Node, ruleContainer *ChannelRuleContainer, tokenVerifier *TokenVerifier) *ClientHandler {
	return &ClientHandler{node: node, ruleContainer: ruleContainer, tokenVerifier: tokenVerifier}
}

func (n *ClientHandler) OnConnecting(_ context.Context, _ TransportInfo, e ConnectEvent) ConnectReply {
	var (
		credentials *Credentials
		channels    []string
	)

	if e.Token != "" {
		token, err := n.tokenVerifier.verifyConnectToken(e.Token)
		if err != nil {
			if err == ErrTokenExpired {
				return ConnectReply{
					Error: ErrorTokenExpired,
				}
			}
			n.node.logger.log(newLogEntry(LogLevelInfo, "invalid connection token", map[string]interface{}{"error": err.Error(), "client": e.ClientID}))
			return ConnectReply{
				Disconnect: DisconnectInvalidToken,
			}
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

	if credentials == nil && n.ruleContainer.config.ClientAnonymous {
		credentials = &Credentials{
			UserID: "",
		}
	}

	return ConnectReply{
		Credentials: credentials,
		Channels:    channels,
	}
}

func (n *ClientHandler) OnRefresh(c *Client, e RefreshEvent) RefreshReply {
	token, err := n.tokenVerifier.verifyConnectToken(e.Token)
	if err != nil {
		if err == ErrTokenExpired {
			return RefreshReply{
				Expired: true,
			}
		}
		n.node.logger.log(newLogEntry(LogLevelInfo, "invalid refresh token", map[string]interface{}{"error": err.Error(), "client": c.ID()}))
		return RefreshReply{
			Disconnect: DisconnectInvalidToken,
		}
	}
	return RefreshReply{
		ExpireAt: token.ExpireAt,
		Info:     token.Info,
	}
}

func (n *ClientHandler) OnSubRefresh(c *Client, e SubRefreshEvent) SubRefreshReply {
	token, err := n.tokenVerifier.verifySubscribeToken(e.Token)
	if err != nil {
		if err == ErrTokenExpired {
			return SubRefreshReply{
				Expired: true,
			}
		}
		c.node.logger.log(newLogEntry(LogLevelInfo, "invalid subscription refresh token", map[string]interface{}{"error": err.Error(), "client": c.ID(), "user": c.UserID()}))
		return SubRefreshReply{
			Disconnect: DisconnectInvalidToken,
		}
	}
	if c.ID() != token.Client {
		return SubRefreshReply{
			Disconnect: DisconnectInvalidToken,
		}
	}
	if e.Channel != token.Channel {
		return SubRefreshReply{
			Disconnect: DisconnectInvalidToken,
		}
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
		return SubscribeReply{
			Error: toClientErr(err),
		}
	}

	if chOpts.ServerSide {
		n.node.logger.log(newLogEntry(LogLevelInfo, "attempt to subscribe on server side channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{
			Error: ErrorPermissionDenied,
		}
	}

	if !n.ruleContainer.userAllowed(e.Channel, c.UserID()) {
		n.node.logger.log(newLogEntry(LogLevelInfo, "user is not allowed to subscribe on channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{
			Error: ErrorPermissionDenied,
		}
	}

	if !chOpts.Anonymous && c.user == "" && !n.ruleContainer.config.ClientInsecure {
		n.node.logger.log(newLogEntry(LogLevelInfo, "anonymous user is not allowed to subscribe on channel", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return SubscribeReply{
			Error: ErrorPermissionDenied,
		}
	}

	var (
		channelInfo protocol.Raw
		expireAt    int64
	)

	if n.ruleContainer.isTokenChannel(e.Channel) {
		if e.Token == "" {
			n.node.logger.log(newLogEntry(LogLevelInfo, "subscription token required", map[string]interface{}{"client": c.uid, "user": c.UserID()}))
			return SubscribeReply{
				Error: ErrorPermissionDenied,
			}
		}
		subToken, err := n.tokenVerifier.verifySubscribeToken(e.Token)
		if err != nil {
			if err == ErrTokenExpired {
				return SubscribeReply{
					Error: ErrorPermissionDenied,
				}
			}
			n.node.logger.log(newLogEntry(LogLevelInfo, "invalid subscription token", map[string]interface{}{"error": err.Error(), "client": c.ID(), "user": c.UserID()}))
			return SubscribeReply{
				Error: ErrorPermissionDenied,
			}
		}
		if c.ID() != subToken.Client {
			return SubscribeReply{
				Error: ErrorPermissionDenied,
			}
		}
		if e.Channel != subToken.Channel {
			return SubscribeReply{
				Error: ErrorPermissionDenied,
			}
		}
		expireAt = subToken.ExpireAt
		if subToken.ExpireTokenOnly {
			expireAt = 0
		}
		channelInfo = subToken.Info
	}

	return SubscribeReply{
		ExpireAt:    expireAt,
		ChannelInfo: channelInfo,
	}
}

func (n *ClientHandler) OnPublish(c *Client, e PublishEvent) PublishReply {
	chOpts, err := n.ruleContainer.namespacedChannelOptions(e.Channel)
	if err != nil {
		n.node.logger.log(newLogEntry(LogLevelInfo, "publish channel options error", map[string]interface{}{"channel": e.Channel, "user": c.UserID(), "client": c.ID()}))
		return PublishReply{
			Error: toClientErr(err),
		}
	}

	if chOpts.SubscribeToPublish {
		if _, ok := c.Channels()[e.Channel]; !ok {
			return PublishReply{
				Error: ErrorPermissionDenied,
			}
		}
	}

	insecure := n.ruleContainer.config.ClientInsecure

	if !chOpts.Publish && !insecure {
		return PublishReply{
			Error: ErrorPermissionDenied,
		}
	}

	return PublishReply{}
}
