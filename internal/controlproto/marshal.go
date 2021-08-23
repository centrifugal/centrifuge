package controlproto

import "github.com/centrifugal/centrifuge/internal/controlpb"

// Encoder ...
type Encoder interface {
	EncodeCommand(*controlpb.Command) ([]byte, error)
	EncodeNode(*controlpb.Node) ([]byte, error)
	EncodeSubscribe(*controlpb.Subscribe) ([]byte, error)
	EncodeUnsubscribe(*controlpb.Unsubscribe) ([]byte, error)
	EncodeDisconnect(*controlpb.Disconnect) ([]byte, error)
	EncodeSurveyRequest(request *controlpb.SurveyRequest) ([]byte, error)
	EncodeSurveyResponse(request *controlpb.SurveyResponse) ([]byte, error)
	EncodeNotification(request *controlpb.Notification) ([]byte, error)
	EncodeRefresh(refresh *controlpb.Refresh) ([]byte, error)
}

var _ Encoder = (*ProtobufEncoder)(nil)

// ProtobufEncoder ...
type ProtobufEncoder struct{}

// NewProtobufEncoder ...
func NewProtobufEncoder() *ProtobufEncoder {
	return &ProtobufEncoder{}
}

// EncodeCommand ...
func (e *ProtobufEncoder) EncodeCommand(cmd *controlpb.Command) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeNode ...
func (e *ProtobufEncoder) EncodeNode(cmd *controlpb.Node) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeSubscribe ...
func (e *ProtobufEncoder) EncodeSubscribe(cmd *controlpb.Subscribe) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeUnsubscribe ...
func (e *ProtobufEncoder) EncodeUnsubscribe(cmd *controlpb.Unsubscribe) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeDisconnect ...
func (e *ProtobufEncoder) EncodeDisconnect(cmd *controlpb.Disconnect) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeSurveyRequest ...
func (e *ProtobufEncoder) EncodeSurveyRequest(cmd *controlpb.SurveyRequest) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeSurveyRequest ...
func (e *ProtobufEncoder) EncodeSurveyResponse(cmd *controlpb.SurveyResponse) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeNotification ...
func (e *ProtobufEncoder) EncodeNotification(cmd *controlpb.Notification) ([]byte, error) {
	return cmd.MarshalVT()
}

// EncodeRefresh ...
func (e *ProtobufEncoder) EncodeRefresh(cmd *controlpb.Refresh) ([]byte, error) {
	return cmd.MarshalVT()
}
