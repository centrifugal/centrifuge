package controlproto

import "github.com/centrifugal/centrifuge/internal/controlpb"

// Decoder ...
type Decoder interface {
	DecodeCommand([]byte) (*controlpb.Command, error)
	DecodeNode([]byte) (*controlpb.Node, error)
	DecodeSubscribe([]byte) (*controlpb.Subscribe, error)
	DecodeUnsubscribe([]byte) (*controlpb.Unsubscribe, error)
	DecodeDisconnect([]byte) (*controlpb.Disconnect, error)
	DecodeSurveyRequest([]byte) (*controlpb.SurveyRequest, error)
	DecodeSurveyResponse([]byte) (*controlpb.SurveyResponse, error)
	DecodeNotification([]byte) (*controlpb.Notification, error)
}

var _ Decoder = (*ProtobufDecoder)(nil)

// ProtobufDecoder ...
type ProtobufDecoder struct{}

// NewProtobufDecoder ...
func NewProtobufDecoder() *ProtobufDecoder {
	return &ProtobufDecoder{}
}

// DecodeCommand ...
func (e *ProtobufDecoder) DecodeCommand(data []byte) (*controlpb.Command, error) {
	var cmd controlpb.Command
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeNode ...
func (e *ProtobufDecoder) DecodeNode(data []byte) (*controlpb.Node, error) {
	var cmd controlpb.Node
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeUnsubscribe ...
func (e *ProtobufDecoder) DecodeSubscribe(data []byte) (*controlpb.Subscribe, error) {
	var cmd controlpb.Subscribe
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeUnsubscribe ...
func (e *ProtobufDecoder) DecodeUnsubscribe(data []byte) (*controlpb.Unsubscribe, error) {
	var cmd controlpb.Unsubscribe
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeDisconnect ...
func (e *ProtobufDecoder) DecodeDisconnect(data []byte) (*controlpb.Disconnect, error) {
	var cmd controlpb.Disconnect
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeSurveyRequest ...
func (e *ProtobufDecoder) DecodeSurveyRequest(data []byte) (*controlpb.SurveyRequest, error) {
	var cmd controlpb.SurveyRequest
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeSurveyResponse ...
func (e *ProtobufDecoder) DecodeSurveyResponse(data []byte) (*controlpb.SurveyResponse, error) {
	var cmd controlpb.SurveyResponse
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// DecodeNotification ...
func (e *ProtobufDecoder) DecodeNotification(data []byte) (*controlpb.Notification, error) {
	var cmd controlpb.Notification
	err := cmd.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}
