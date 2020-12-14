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
		UID:    "test",
		Method: controlpb.MethodTypeDisconnect,
		Params: controlpb.Raw("{}"),
	}
	d, err := encoder.EncodeCommand(cmd)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedCmd, err := decoder.DecodeCommand(d)
	require.NoError(t, err)
	require.Equal(t, cmd, decodedCmd)

	node := &controlpb.Node{
		UID:         "test",
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
		ID:   1,
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
		ID:   1,
		Code: 1,
		Data: nil,
	}
	d, err = encoder.EncodeSurveyResponse(surveyResponse)
	require.NoError(t, err)
	require.NotNil(t, d)

	decodedSurveyResponse, err := decoder.DecodeSurveyResponse(d)
	require.NoError(t, err)
	require.Equal(t, surveyResponse, decodedSurveyResponse)
}
