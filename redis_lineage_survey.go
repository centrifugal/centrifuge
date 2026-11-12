package centrifuge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// The lineage survey of ResyncOnlyLostSlots: the node opening the gate of a
// server that started a new lineage asks every node for the newest write it
// observed in each slot of the server, and keeps a slot only if none of them
// is missing (see redisLineage.open).

// lineageSurveyOp is reserved: survey ops starting with "centrifuge_" belong to
// Centrifuge.
const lineageSurveyOp = "centrifuge_lineage"

// lineageSurveyTimeout bounds the survey; a node that doesn't answer in time
// makes the gate reset every slot.
var lineageSurveyTimeout = 3 * time.Second

// lineageWitnessWindow: the survey keeps slots only if every node has been
// running, and counting writes, for at least this long, and none left within
// it. A node that restarted or left may have observed the lost writes and
// forgotten them, and its clients reconnect elsewhere with positions past what
// the server has. The window covers the time from the writes being lost (the
// old primary failing) to the survey (the gate opening on the new one).
var lineageWitnessWindow = 2 * time.Minute

type lineageSurveyRequest struct {
	Shard string `json:"s"`
	Slots []int  `json:"sl"`
}

type lineageSurveyReply struct {
	Tokens []string `json:"t"`
	Counts []uint64 `json:"c"`
	// Up: seconds the node has been running; Counting: seconds it has been
	// counting writes on the shard (0: not counting).
	Up       int64 `json:"u"`
	Counting int64 `json:"n"`
}

func lineageShardKey(s *RedisShard) string {
	return s.string()
}

func (n *Node) registerLineage(l *redisLineage) {
	n.lineages.Store(lineageShardKey(l.shard), l)
}

func (n *Node) handleLineageSurvey(data []byte) SurveyReply {
	var req lineageSurveyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return SurveyReply{Code: 1}
	}
	v, ok := n.lineages.Load(req.Shard)
	reply := lineageSurveyReply{
		Tokens: make([]string, len(req.Slots)),
		Counts: make([]uint64, len(req.Slots)),
		Up:     time.Now().Unix() - n.startedAt,
	}
	if ok {
		l := v.(*redisLineage)
		reply.Counting = int64(l.countingFor() / time.Second)
		l.obsMu.Lock()
		for i, s := range req.Slots {
			if s < 0 || s > 16383 {
				continue
			}
			reply.Tokens[i] = l.obs[s].token
			reply.Counts[i] = l.obs[s].count
		}
		l.obsMu.Unlock()
	}
	// A node without this shard observed nothing in it; it reports Counting 0,
	// which makes the gate reset every slot: harmless, but conservative.
	out, err := json.Marshal(reply)
	if err != nil {
		return SurveyReply{Code: 1}
	}
	return SurveyReply{Data: out}
}

// surveyObserved asks every node for the newest observed write of each slot.
// complete is false when a node didn't answer or answered with an error.
func (l *redisLineage) surveyObserved(ctx context.Context, slots []int) (tokens [][]string, counts [][]uint64, complete bool, err error) {
	if l.node == nil {
		return nil, nil, false, fmt.Errorf("no node")
	}
	data, err := json.Marshal(lineageSurveyRequest{Shard: lineageShardKey(l.shard), Slots: slots})
	if err != nil {
		return nil, nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, lineageSurveyTimeout)
	defer cancel()
	expected := l.node.nodes.size()
	if expected < 1 {
		// The node isn't registered yet (not running): no complete answer.
		return nil, nil, false, nil
	}
	if d := l.node.nodes.lastDeparture(); !d.IsZero() && time.Since(d) < lineageWitnessWindow {
		// A node left recently: it may have observed the lost writes.
		return nil, nil, false, nil
	}
	results, err := l.node.Survey(ctx, lineageSurveyOp, data, "")
	if err != nil {
		return nil, nil, false, err
	}
	complete = len(results) >= expected && expected > 0
	for _, r := range results {
		if r.Code != 0 {
			complete = false
			continue
		}
		var reply lineageSurveyReply
		if err := json.Unmarshal(r.Data, &reply); err != nil || len(reply.Tokens) != len(slots) || len(reply.Counts) != len(slots) {
			complete = false
			continue
		}
		window := int64(lineageWitnessWindow / time.Second)
		if reply.Up < window || reply.Counting < window {
			// A witness that may have forgotten what it observed.
			complete = false
			continue
		}
		tokens = append(tokens, reply.Tokens)
		counts = append(counts, reply.Counts)
	}
	return tokens, counts, complete, nil
}

// lineageSlotState is what a server holds for a slot at a lineage break.
type lineageSlotState struct {
	token    string     // the token key's token
	ancestor lineageObs // its ancestor, if it has one
	count    uint64     // the slot's write counter
}

// slotKept tells whether every write observed in a slot is on the server.
func slotKept(st lineageSlotState, tokens [][]string, counts [][]uint64, i int) bool {
	if st.token == "" {
		return false
	}
	for n := range tokens {
		t, c := tokens[n][i], counts[n][i]
		switch {
		case t == "":
			// Nothing observed.
		case t == st.token:
			if c > st.count {
				return false
			}
		case st.ancestor.token != "" && t == st.ancestor.token:
			if c > st.ancestor.count {
				return false
			}
		default:
			// A write of a lineage the server doesn't descend from.
			return false
		}
	}
	return true
}
