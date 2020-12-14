package controlproto

import "github.com/centrifugal/centrifuge/internal/controlpb"

// Encoder ...
type Encoder interface {
	EncodeCommand(*controlpb.Command) ([]byte, error)
	EncodeNode(*controlpb.Node) ([]byte, error)
	EncodeUnsubscribe(*controlpb.Unsubscribe) ([]byte, error)
	EncodeDisconnect(*controlpb.Disconnect) ([]byte, error)
	EncodeSurveyRequest(request *controlpb.SurveyRequest) ([]byte, error)
	EncodeSurveyResponse(request *controlpb.SurveyResponse) ([]byte, error)
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
	return cmd.Marshal()
}

// EncodeNode ...
func (e *ProtobufEncoder) EncodeNode(cmd *controlpb.Node) ([]byte, error) {
	return cmd.Marshal()
}

// EncodeUnsubscribe ...
func (e *ProtobufEncoder) EncodeUnsubscribe(cmd *controlpb.Unsubscribe) ([]byte, error) {
	return cmd.Marshal()
}

// EncodeDisconnect ...
func (e *ProtobufEncoder) EncodeDisconnect(cmd *controlpb.Disconnect) ([]byte, error) {
	return cmd.Marshal()
}

// EncodeSurveyRequest ...
func (e *ProtobufEncoder) EncodeSurveyRequest(cmd *controlpb.SurveyRequest) ([]byte, error) {
	return cmd.Marshal()
}

// EncodeSurveyRequest ...
func (e *ProtobufEncoder) EncodeSurveyResponse(cmd *controlpb.SurveyResponse) ([]byte, error) {
	return cmd.Marshal()
}
