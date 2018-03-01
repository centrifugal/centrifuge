package centrifuge

import (
	"context"

	"github.com/centrifugal/centrifuge/internal/proto"
	"github.com/centrifugal/centrifuge/internal/proto/apiproto"
)

// apiExecutor can run API methods.
type apiExecutor struct {
	node *Node
}

// newHandler creates new Handler.
func newAPIExecutor(n *Node) *apiExecutor {
	return &apiExecutor{
		node: n,
	}
}

// Publish publishes data into channel.
func (h *apiExecutor) Publish(ctx context.Context, cmd *apiproto.PublishRequest) *apiproto.PublishResponse {
	ch := cmd.Channel
	data := cmd.Data

	resp := &apiproto.PublishResponse{}

	if string(ch) == "" || len(data) == 0 {
		h.node.logger.log(newLogEntry(LogLevelError, "channel and data required for publish", nil))
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	chOpts, ok := h.node.ChannelOpts(ch)
	if !ok {
		resp.Error = apiproto.ErrNamespaceNotFound
		return resp
	}

	publication := &proto.Publication{
		Data: cmd.Data,
	}
	if cmd.UID != "" {
		publication.UID = cmd.UID
	}

	err := <-h.node.publish(cmd.Channel, publication, &chOpts)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error publishing message in engine", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}
	return resp
}

// Broadcast publishes data into multiple channels.
func (h *apiExecutor) Broadcast(ctx context.Context, cmd *apiproto.BroadcastRequest) *apiproto.BroadcastResponse {

	resp := &apiproto.BroadcastResponse{}

	channels := cmd.Channels
	data := cmd.Data

	if len(channels) == 0 {
		h.node.logger.log(newLogEntry(LogLevelError, "channels required for broadcast", nil))
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	if len(data) == 0 {
		h.node.logger.log(newLogEntry(LogLevelError, "data required for broadcast", nil))
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	errs := make([]<-chan error, len(channels))

	for i, ch := range channels {

		if string(ch) == "" {
			h.node.logger.log(newLogEntry(LogLevelError, "channel can not be blank in broadcast", nil))
			resp.Error = apiproto.ErrBadRequest
			return resp
		}

		chOpts, ok := h.node.ChannelOpts(ch)
		if !ok {
			h.node.logger.log(newLogEntry(LogLevelError, "can't find namespace for channel", map[string]interface{}{"channel": ch}))
			resp.Error = apiproto.ErrNamespaceNotFound
		}

		publication := &proto.Publication{
			Data: cmd.Data,
		}
		if cmd.UID != "" {
			publication.UID = cmd.UID
		}
		errs[i] = h.node.publish(ch, publication, &chOpts)
	}

	var firstErr error
	for i := range errs {
		err := <-errs[i]
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			h.node.logger.log(newLogEntry(LogLevelError, "error publishing into channel", map[string]interface{}{"channel": channels[i], "error": err.Error()}))
		}
	}
	if firstErr != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error broadcasting data", map[string]interface{}{"error": firstErr.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}
	return resp
}

// Unsubscribe unsubscribes user from channel and sends unsubscribe
// control message to other nodes so they could also unsubscribe user.
func (h *apiExecutor) Unsubscribe(ctx context.Context, cmd *apiproto.UnsubscribeRequest) *apiproto.UnsubscribeResponse {

	resp := &apiproto.UnsubscribeResponse{}

	user := cmd.User
	channel := cmd.Channel

	err := h.node.Unsubscribe(user, channel)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error unsubscribing user from channel", map[string]interface{}{"channel": channel, "user": user, "error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}
	return resp
}

// Disconnect disconnects user by its ID and sends disconnect
// control message to other nodes so they could also disconnect user.
func (h *apiExecutor) Disconnect(ctx context.Context, cmd *apiproto.DisconnectRequest) *apiproto.DisconnectResponse {

	resp := &apiproto.DisconnectResponse{}

	user := cmd.User

	err := h.node.Disconnect(user, false)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error disconnecting user", map[string]interface{}{"user": cmd.User, "error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}
	return resp
}

// Presence returns response with presence information for channel.
func (h *apiExecutor) Presence(ctx context.Context, cmd *apiproto.PresenceRequest) *apiproto.PresenceResponse {

	resp := &apiproto.PresenceResponse{}

	ch := cmd.Channel

	if string(ch) == "" {
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	chOpts, ok := h.node.ChannelOpts(ch)
	if !ok {
		resp.Error = apiproto.ErrNamespaceNotFound
		return resp
	}

	if !chOpts.Presence {
		resp.Error = apiproto.ErrNotAvailable
		return resp
	}

	presence, err := h.node.Presence(ch)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling presence", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	apiPresence := make(map[string]*apiproto.ClientInfo, len(presence))
	for k, v := range presence {
		apiPresence[k] = (*apiproto.ClientInfo)(v)
	}

	resp.Result = &apiproto.PresenceResult{
		Presence: apiPresence,
	}
	return resp
}

// PresenceStats returns response with presence stats information for channel.
func (h *apiExecutor) PresenceStats(ctx context.Context, cmd *apiproto.PresenceStatsRequest) *apiproto.PresenceStatsResponse {

	resp := &apiproto.PresenceStatsResponse{}

	ch := cmd.Channel

	if string(ch) == "" {
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	chOpts, ok := h.node.ChannelOpts(ch)
	if !ok {
		resp.Error = apiproto.ErrNamespaceNotFound
		return resp
	}

	if !chOpts.Presence || !chOpts.PresenceStats {
		resp.Error = apiproto.ErrNotAvailable
		return resp
	}

	presence, err := h.node.Presence(cmd.Channel)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling presence", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	numClients := len(presence)
	numUsers := 0
	uniqueUsers := map[string]struct{}{}

	for _, info := range presence {
		userID := info.User
		if _, ok := uniqueUsers[userID]; !ok {
			uniqueUsers[userID] = struct{}{}
			numUsers++
		}
	}

	resp.Result = &apiproto.PresenceStatsResult{
		NumClients: uint32(numClients),
		NumUsers:   uint32(numUsers),
	}

	return resp
}

// History returns response with history information for channel.
func (h *apiExecutor) History(ctx context.Context, cmd *apiproto.HistoryRequest) *apiproto.HistoryResponse {

	resp := &apiproto.HistoryResponse{}

	ch := cmd.Channel

	if string(ch) == "" {
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	chOpts, ok := h.node.ChannelOpts(ch)
	if !ok {
		resp.Error = apiproto.ErrNamespaceNotFound
		return resp
	}

	if chOpts.HistorySize <= 0 || chOpts.HistoryLifetime <= 0 {
		resp.Error = apiproto.ErrNotAvailable
		return resp
	}

	history, err := h.node.History(ch)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling history", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	apiPublications := make([]*apiproto.Publication, len(history))

	for i, publication := range history {
		apiPublications[i] = &apiproto.Publication{
			UID:  publication.UID,
			Data: publication.Data,
			Info: (*apiproto.ClientInfo)(publication.Info),
		}
	}

	resp.Result = &apiproto.HistoryResult{
		Publications: apiPublications,
	}
	return resp
}

// HistoryRemove removes all history information for channel.
func (h *apiExecutor) HistoryRemove(ctx context.Context, cmd *apiproto.HistoryRemoveRequest) *apiproto.HistoryRemoveResponse {

	resp := &apiproto.HistoryRemoveResponse{}

	ch := cmd.Channel

	if string(ch) == "" {
		resp.Error = apiproto.ErrBadRequest
		return resp
	}

	chOpts, ok := h.node.ChannelOpts(ch)
	if !ok {
		resp.Error = apiproto.ErrNamespaceNotFound
		return resp
	}

	if chOpts.HistorySize <= 0 || chOpts.HistoryLifetime <= 0 {
		resp.Error = apiproto.ErrNotAvailable
		return resp
	}

	err := h.node.RemoveHistory(ch)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling history remove", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	resp.Result = &apiproto.HistoryRemoveResult{}
	return resp
}


// Channels returns active channels.
func (h *apiExecutor) Channels(ctx context.Context, cmd *apiproto.ChannelsRequest) *apiproto.ChannelsResponse {

	resp := &apiproto.ChannelsResponse{}

	channels, err := h.node.Channels()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling channels", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	resp.Result = &apiproto.ChannelsResult{
		Channels: channels,
	}
	return resp
}

// Info returns information about running nodes.
func (h *apiExecutor) Info(ctx context.Context, cmd *apiproto.InfoRequest) *apiproto.InfoResponse {

	resp := &apiproto.InfoResponse{}

	info, err := h.node.Info()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error calling stats", map[string]interface{}{"error": err.Error()}))
		resp.Error = apiproto.ErrInternalServerError
		return resp
	}

	resp.Result = info
	return resp
}
