package centrifuge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/centrifugal/protocol"
)

type EmulationConfig struct{}

// EmulationHandler ...
// EXPERIMENTAL, this is still a subject to change, do not use in production.
type EmulationHandler struct {
	node     *Node
	config   EmulationConfig
	emuLayer *emulationLayer
}

// NewEmulationHandler creates new EmulationHandler.
// EXPERIMENTAL, this is still a subject to change, do not use in production.
func NewEmulationHandler(node *Node, config EmulationConfig) *EmulationHandler {
	return &EmulationHandler{
		node:     node,
		config:   config,
		emuLayer: newEmulationLayer(node),
	}
}

func (s *EmulationHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		// For pre-flight browser requests.
		rw.WriteHeader(http.StatusOK)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.node.logger.log(newLogEntry(LogLevelError, "can't read emulation request body", map[string]interface{}{"error": err.Error()}))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	var req protocol.EmulationRequest
	if r.Header.Get("Content-Type") == "application/octet-stream" {
		err = req.UnmarshalVT(data)
	} else {
		err = json.Unmarshal(data, &req)
	}
	if err != nil {
		s.node.logger.log(newLogEntry(LogLevelInfo, "can't unmarshal emulation request", map[string]interface{}{"req": &req, "error": err.Error()}))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	err = s.emuLayer.Emulate(&req)
	if err != nil {
		s.node.logger.log(newLogEntry(LogLevelError, "error processing emulation request", map[string]interface{}{"req": &req, "error": err.Error()}))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

type emulationLayer struct {
	node *Node
}

func newEmulationLayer(node *Node) *emulationLayer {
	return &emulationLayer{node: node}
}

func (l *emulationLayer) Emulate(req *protocol.EmulationRequest) error {
	return l.node.sendEmulation(req)
}

const emulationOp = "centrifuge_emulation"

func (n *Node) sendEmulation(req *protocol.EmulationRequest) error {
	data, err := req.MarshalVT()
	if err != nil {
		return err
	}
	_, err = n.Survey(context.Background(), emulationOp, data, req.Node)
	return err
}

type emulationSurveyHandler struct {
	node *Node
}

func newEmulationSurveyHandler(node *Node) *emulationSurveyHandler {
	return &emulationSurveyHandler{node: node}
}

func (h *emulationSurveyHandler) HandleEmulation(e SurveyEvent, cb SurveyCallback) {
	var req protocol.EmulationRequest
	err := req.UnmarshalVT(e.Data)
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error unmarshal emulation request", map[string]interface{}{"data": string(e.Data), "error": err.Error()}))
		cb(SurveyReply{Code: 1})
		return
	}
	client, ok := h.node.Hub().clientBySession(req.Session)
	if !ok {
		cb(SurveyReply{Code: 2})
		return
	}
	var data []byte
	if client.transport.Protocol() == ProtocolTypeJSON {
		var d string
		err = json.Unmarshal(req.Data, &d)
		if err != nil {
			h.node.logger.log(newLogEntry(LogLevelError, "error unmarshal emulation request data", map[string]interface{}{"data": string(req.Data), "error": err.Error()}))
			cb(SurveyReply{Code: 3})
			return
		}
		data = []byte(d)
	} else {
		data = req.Data
	}
	go func() {
		_ = client.Handle(data)
		cb(SurveyReply{})
	}()
}