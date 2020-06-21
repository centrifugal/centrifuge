package centrifuge

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// ChannelNamespace allows to create channels with different channel options.
type ChannelNamespace struct {
	// Name is a unique namespace name.
	Name string `json:"name"`

	// Options for namespace determine channel options for channels
	// belonging to this namespace.
	NamespacedChannelOptions `mapstructure:",squash"`
}

type NamespacedRuleConfig struct {
	// ChannelOptions embedded.
	NamespacedChannelOptions
	// Namespaces – list of namespaces for custom channel options.
	Namespaces []ChannelNamespace
	// ChannelPrivatePrefix is a prefix in channel name which indicates that
	// channel is private.
	ChannelPrivatePrefix string
	// ChannelNamespaceBoundary is a string separator which must be put after
	// namespace part in channel name.
	ChannelNamespaceBoundary string
	// ChannelUserBoundary is a string separator which must be set before allowed
	// users part in channel name.
	ChannelUserBoundary string
	// ChannelUserSeparator separates allowed users in user part of channel name.
	ChannelUserSeparator string
	// UserPersonalChannelPrefix defines prefix to be added to user personal channel.
	UserPersonalChannelNamespace string
}

// ChannelOptions represent channel specific configuration for namespace
// or global channel options if set on top level of configuration.
type NamespacedChannelOptions struct {
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

func (n NamespacedChannelOptions) channelOptions() ChannelOptions {
	return n.ChannelOptions
}

// DefaultRuleConfig ...
var DefaultRuleConfig = NamespacedRuleConfig{
	ChannelPrivatePrefix:     "$", // so private channel will look like "$gossips"
	ChannelNamespaceBoundary: ":", // so namespace "public" can be used as "public:news"
	ChannelUserBoundary:      "#", // so user limited channel is "user#2694" where "2696" is user ID
	ChannelUserSeparator:     ",", // so several users limited channel is "dialog#2694,3019"
}

// Validate validates config and returns error if problems found
func (c *NamespacedRuleConfig) Validate() error {
	errPrefix := "config error: "
	pattern := "^[-a-zA-Z0-9_.]{2,}$"
	patternRegexp, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	if c.HistoryRecover && (c.HistorySize == 0 || c.HistoryLifetime == 0) {
		return errors.New("both history size and history lifetime required for history recovery")
	}

	usePersonalChannel := c.UserPersonalChannelNamespace != ""
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

var _ ChannelOptionsGetter = (*NamespacedChannelRuleChecker)(nil)

type NamespacedChannelRuleChecker struct {
	mu     sync.RWMutex
	node   *Node
	config NamespacedRuleConfig
}

func NewNamespacedChannelRuleChecker(node *Node, config NamespacedRuleConfig) *NamespacedChannelRuleChecker {
	return &NamespacedChannelRuleChecker{
		node:   node,
		config: config,
	}
}

// namespaceName returns namespace name from channel if exists.
func (n *NamespacedChannelRuleChecker) namespaceName(ch string) string {
	cTrim := strings.TrimPrefix(ch, n.config.ChannelPrivatePrefix)
	if n.config.ChannelNamespaceBoundary != "" && strings.Contains(cTrim, n.config.ChannelNamespaceBoundary) {
		parts := strings.SplitN(cTrim, n.config.ChannelNamespaceBoundary, 2)
		return parts[0]
	}
	return ""
}

// ChannelOpts returns channel options for channel using current channel config.
func (n *NamespacedChannelRuleChecker) ChannelOptions(ch string) (ChannelOptions, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	opts, err := n.config.channelOpts(n.namespaceName(ch))
	if err != nil {
		return ChannelOptions{}, err
	}
	return opts.ChannelOptions, nil
}

// ChannelOpts returns channel options for channel using current channel config.
func (n *NamespacedChannelRuleChecker) namespacedChannelOptions(ch string) (NamespacedChannelOptions, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.config.channelOpts(n.namespaceName(ch))
}

// channelOpts searches for channel options for specified namespace key.
func (c *NamespacedRuleConfig) channelOpts(namespaceName string) (NamespacedChannelOptions, error) {
	if namespaceName == "" {
		return c.NamespacedChannelOptions, nil
	}
	for _, n := range c.Namespaces {
		if n.Name == namespaceName {
			return n.NamespacedChannelOptions, nil
		}
	}
	return NamespacedChannelOptions{}, ErrorNamespaceNotFound
}

// PersonalChannel returns personal channel for user based on node configuration.
func (n *NamespacedChannelRuleChecker) PersonalChannel(user string) string {
	config := n.Config()
	if config.UserPersonalChannelNamespace == "" {
		return config.ChannelUserBoundary + user
	}
	return config.UserPersonalChannelNamespace + config.ChannelNamespaceBoundary + config.ChannelUserBoundary + user
}

// Config returns a copy of node Config.
func (n *NamespacedChannelRuleChecker) Config() NamespacedRuleConfig {
	n.mu.RLock()
	c := n.config
	n.mu.RUnlock()
	return c
}

// privateChannel checks if channel private. In case of private channel
// subscription request must contain a proper signature.
func (n *NamespacedChannelRuleChecker) privateChannel(ch string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.config.ChannelPrivatePrefix == "" {
		return false
	}
	return strings.HasPrefix(ch, n.config.ChannelPrivatePrefix)
}

// userAllowed checks if user can subscribe on channel - as channel
// can contain special part in the end to indicate which users allowed
// to subscribe on it.
func (n *NamespacedChannelRuleChecker) userAllowed(ch string, user string) bool {
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

func (n *NamespacedChannelRuleChecker) ValidateSubscribe(c *Client, channel string) error {
	config := n.node.Config()

	chOpts, err := n.namespacedChannelOptions(channel)
	if err != nil {
		return err
	}

	if chOpts.ServerSide {
		n.node.logger.log(newLogEntry(LogLevelInfo, "attempt to subscribe on server side channel", map[string]interface{}{"channel": channel, "user": c.UserID(), "client": c.ID()}))
		return ErrorPermissionDenied
	}

	if !n.userAllowed(channel, c.UserID()) {
		n.node.logger.log(newLogEntry(LogLevelInfo, "user is not allowed to subscribe on channel", map[string]interface{}{"channel": channel, "user": c.UserID(), "client": c.ID()}))
		return ErrorPermissionDenied
	}

	if !chOpts.Anonymous && c.user == "" && !config.ClientInsecure {
		n.node.logger.log(newLogEntry(LogLevelInfo, "anonymous user is not allowed to subscribe on channel", map[string]interface{}{"channel": channel, "user": c.UserID(), "client": c.ID()}))
		return ErrorPermissionDenied
	}

	return nil
}

func (n *NamespacedChannelRuleChecker) ValidatePublish(c *Client, ch string) error {
	chOpts, err := n.namespacedChannelOptions(ch)
	if err != nil {
		// TODO: handle properly.
		c.node.logger.log(newLogEntry(LogLevelInfo, "attempt to publish to non-existing namespace", map[string]interface{}{"channel": ch, "user": c.UserID(), "client": c.ID()}))
		return ErrorNamespaceNotFound
	}

	if chOpts.SubscribeToPublish {
		_, ok := c.Channels()[ch]
		if !ok {
			return ErrorPermissionDenied
		}
	}

	insecure := c.node.Config().ClientInsecure

	if !chOpts.Publish && !insecure {
		return ErrorPermissionDenied
	}

	return nil
}
