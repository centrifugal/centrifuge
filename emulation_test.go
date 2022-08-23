package centrifuge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestEmulationHandler_Options(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodOptions, url, bytes.NewBuffer(nil))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEmulationHandler_RequestTooLarge(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{
		MaxRequestBodySize: 2,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    "unknown",
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func TestEmulationHandler_NodeNotFound(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    "unknown",
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEmulationHandler_OK(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    n.ID(),
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}
