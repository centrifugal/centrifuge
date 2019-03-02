package protocent

import (
	"encoding/json"

	"github.com/centrifugal/centrifuge/internal/proto"
)

// PushDecoder ...
type PushDecoder interface {
	Decode([]byte) (*Push, error)
	DecodePublication([]byte) (*proto.Publication, error)
	DecodeJoin([]byte) (*proto.Join, error)
	DecodeLeave([]byte) (*proto.Leave, error)
}

// JSONPushDecoder ...
type JSONPushDecoder struct {
}

// NewJSONPushDecoder ...
func NewJSONPushDecoder() *JSONPushDecoder {
	return &JSONPushDecoder{}
}

// Decode ...
func (e *JSONPushDecoder) Decode(data []byte) (*Push, error) {
	var m Push
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodePublication ...
func (e *JSONPushDecoder) DecodePublication(data []byte) (*proto.Publication, error) {
	var m proto.Publication
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeJoin ...
func (e *JSONPushDecoder) DecodeJoin(data []byte) (*proto.Join, error) {
	var m proto.Join
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeLeave  ...
func (e *JSONPushDecoder) DecodeLeave(data []byte) (*proto.Leave, error) {
	var m proto.Leave
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// ProtobufPushDecoder ...
type ProtobufPushDecoder struct {
}

// NewProtobufPushDecoder ...
func NewProtobufPushDecoder() *ProtobufPushDecoder {
	return &ProtobufPushDecoder{}
}

// Decode ...
func (e *ProtobufPushDecoder) Decode(data []byte) (*Push, error) {
	var m Push
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodePublication ...
func (e *ProtobufPushDecoder) DecodePublication(data []byte) (*proto.Publication, error) {
	var m proto.Publication
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeJoin ...
func (e *ProtobufPushDecoder) DecodeJoin(data []byte) (*proto.Join, error) {
	var m proto.Join
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeLeave  ...
func (e *ProtobufPushDecoder) DecodeLeave(data []byte) (*proto.Leave, error) {
	var m proto.Leave
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}
