package controlproto

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/controlpb"

	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	encoder := NewProtobufEncoder()
	decoder := NewProtobufDecoder()

	cmd := &controlpb.Command{
		Uid:    "test",
		Method: controlpb.Command_DISCONNECT,
		Params: []byte("{}"),
	}
	d, err := encoder.EncodeCommand(cmd)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedCmd, err := decoder.DecodeCommand(d)
	require.NoError(t, err)
	require.Equal(t, cmd, decodedCmd)

	node := &controlpb.Node{
		Uid:         "test",
		Name:        "test name",
		Version:     "v1.0.0",
		NumChannels: 2,
		NumClients:  3,
		NumUsers:    1,
		Uptime:      12,
		Metrics: &controlpb.Metrics{
			Interval: 60,
			Items: map[string]float64{
				"item": 1,
			},
		},
	}
	d, err = encoder.EncodeNode(node)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedNode, err := decoder.DecodeNode(d)
	require.NoError(t, err)
	require.Equal(t, node, decodedNode)

	disconnect := &controlpb.Disconnect{
		User: "test",
	}
	d, err = encoder.EncodeDisconnect(disconnect)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedDisconnect, err := decoder.DecodeDisconnect(d)
	require.NoError(t, err)
	require.Equal(t, disconnect, decodedDisconnect)

	sub := &controlpb.Subscribe{
		User:    "test",
		Channel: "test channel",
	}
	d, err = encoder.EncodeSubscribe(sub)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedSubscribe, err := decoder.DecodeSubscribe(d)
	require.NoError(t, err)
	require.Equal(t, sub, decodedSubscribe)

	unsub := &controlpb.Unsubscribe{
		User:    "test",
		Channel: "test channel",
	}
	d, err = encoder.EncodeUnsubscribe(unsub)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedUnsubscribe, err := decoder.DecodeUnsubscribe(d)
	require.NoError(t, err)
	require.Equal(t, unsub, decodedUnsubscribe)

	surveyRequest := &controlpb.SurveyRequest{
		Id:   1,
		Op:   "test",
		Data: nil,
	}
	d, err = encoder.EncodeSurveyRequest(surveyRequest)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedSurveyRequest, err := decoder.DecodeSurveyRequest(d)
	require.NoError(t, err)
	require.Equal(t, surveyRequest, decodedSurveyRequest)

	surveyResponse := &controlpb.SurveyResponse{
		Id:   1,
		Code: 1,
		Data: nil,
	}
	d, err = encoder.EncodeSurveyResponse(surveyResponse)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedSurveyResponse, err := decoder.DecodeSurveyResponse(d)
	require.NoError(t, err)
	require.Equal(t, surveyResponse, decodedSurveyResponse)

	notification := &controlpb.Notification{
		Op:   "test",
		Data: nil,
	}
	d, err = encoder.EncodeNotification(notification)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedNotification, err := decoder.DecodeNotification(d)
	require.NoError(t, err)
	require.Equal(t, notification, decodedNotification)
}

func TestDecoderError(t *testing.T) {
	decoder := NewProtobufDecoder()

	_, err := decoder.DecodeCommand([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeNode([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeCommand([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeDisconnect([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeSubscribe([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeUnsubscribe([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeSurveyRequest([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeSurveyResponse([]byte("-"))
	require.Error(t, err)

	_, err = decoder.DecodeNotification([]byte("-"))
	require.Error(t, err)
}
