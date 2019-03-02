package protocent

import (
	"encoding/json"

	"github.com/centrifugal/centrifuge/internal/proto"
)

// PushEncoder ...
type PushEncoder interface {
	Encode(*Push) ([]byte, error)
	EncodePublication(*proto.Publication) ([]byte, error)
	EncodeJoin(*proto.Join) ([]byte, error)
	EncodeLeave(*proto.Leave) ([]byte, error)
}

// JSONPushEncoder ...
type JSONPushEncoder struct {
}

// NewJSONPushEncoder ...
func NewJSONPushEncoder() *JSONPushEncoder {
	return &JSONPushEncoder{}
}

// Encode ...
func (e *JSONPushEncoder) Encode(message *Push) ([]byte, error) {
	return json.Marshal(message)
}

// EncodePublication ...
func (e *JSONPushEncoder) EncodePublication(message *proto.Publication) ([]byte, error) {
	return json.Marshal(message)
}

// EncodeJoin ...
func (e *JSONPushEncoder) EncodeJoin(message *proto.Join) ([]byte, error) {
	return json.Marshal(message)
}

// EncodeLeave ...
func (e *JSONPushEncoder) EncodeLeave(message *proto.Leave) ([]byte, error) {
	return json.Marshal(message)
}

// ProtobufPushEncoder ...
type ProtobufPushEncoder struct {
}

// NewProtobufPushEncoder ...
func NewProtobufPushEncoder() *ProtobufPushEncoder {
	return &ProtobufPushEncoder{}
}

// Encode ...
func (e *ProtobufPushEncoder) Encode(message *Push) ([]byte, error) {
	return message.Marshal()
}

// EncodePublication ...
func (e *ProtobufPushEncoder) EncodePublication(message *proto.Publication) ([]byte, error) {
	return message.Marshal()
}

// EncodeJoin ...
func (e *ProtobufPushEncoder) EncodeJoin(message *proto.Join) ([]byte, error) {
	return message.Marshal()
}

// EncodeLeave ...
func (e *ProtobufPushEncoder) EncodeLeave(message *proto.Leave) ([]byte, error) {
	return message.Marshal()
}
