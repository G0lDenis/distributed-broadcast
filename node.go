package node

import (
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// BroadcastNode is a node that supports reliable causal broadcast.
type BroadcastNode interface {
	hive.Node
	Broadcast(payload any) error
	DeliveredMessages() []hive.Message
}

type ReliableCausalBroadcastNode struct {
	*hive.BaseNode
	allNodeIDs []string
	mu         sync.Mutex
	vector     map[string]int
	nextSeq    int
	seen       map[string]struct{}
	pending    map[string]rbMessage
	delivered  []hive.Message
}

// NewReliableCausalBroadcastNode creates a new node that performs reliable causal broadcast.
func NewReliableCausalBroadcastNode(id string, allNodeIDs []string) BroadcastNode {
	base := hive.NewBaseNode(id)
	n := &ReliableCausalBroadcastNode{
		BaseNode:   base,
		allNodeIDs: append([]string(nil), allNodeIDs...),
		vector:     make(map[string]int, len(allNodeIDs)),
		seen:       make(map[string]struct{}),
		pending:    make(map[string]rbMessage),
	}
	n.SetNodeRef(n)
	return n
}

// Broadcast sends the payload to all nodes (including self). Each recipient will
// apply reliable broadcast (flood) and causal delivery.
func (n *ReliableCausalBroadcastNode) Broadcast(payload any) error {
	n.mu.Lock()
	n.nextSeq++
	n.vector[n.ID()]++
	msg := rbMessage{
		Origin:  n.ID(),
		Seq:     n.nextSeq,
		Payload: payload,
		Clock:   copyClock(n.vector),
	}
	n.mu.Unlock()
	return n.handleFirstSeen(msg)
}

func (n *ReliableCausalBroadcastNode) Receive(msg *hive.Message) error {
	switch p := msg.Payload.(type) {
	case rbMessage:
		return n.handleFirstSeen(p)
	default:
		return nil
	}
}

// DeliveredMessages returns the application-level messages that have been
// delivered to this node, in delivery order.
func (n *ReliableCausalBroadcastNode) DeliveredMessages() []hive.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]hive.Message, len(n.delivered))
	copy(out, n.delivered)
	return out
}

// Orderer orders events based on their vector clocks and identifies groups of
// mutually parallel events.
type Orderer struct{}

func happensBefore(va, vb map[string]int) bool {
	strict := false
	for k := range va {
		if va[k] > vb[k] {
			return false
		}
		if va[k] < vb[k] {
			strict = true
		}
	}
	for k, b := range vb {
		if va[k] > b {
			return false
		}
		if va[k] < b {
			strict = true
		}
	}
	return strict
}

func topoSortByPayload(ids []string, less func(i, j int) bool) []string {
	n := len(ids)
	adj := make([][]int, n)
	indeg := make([]int, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && less(i, j) {
				adj[i] = append(adj[i], j)
				indeg[j]++
			}
		}
	}
	avail := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if indeg[i] == 0 {
			avail = append(avail, i)
		}
	}
	sort.Slice(avail, func(i, j int) bool { return ids[avail[i]] < ids[avail[j]] })
	var out []string
	for len(avail) > 0 {
		cur := avail[0]
		avail = avail[1:]
		out = append(out, ids[cur])
		for _, to := range adj[cur] {
			indeg[to]--
			if indeg[to] == 0 {
				avail = append(avail, to)
			}
		}
		sort.Slice(avail, func(i, j int) bool { return ids[avail[i]] < ids[avail[j]] })
	}
	return out
}

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	if len(msgs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(msgs))
	idToIdx := make(map[string]int, len(msgs))
	vcs := make([]map[string]int, len(msgs))
	for i, m := range msgs {
		ids[i] = fmt.Sprint(m.Payload)
		idToIdx[ids[i]] = i
		vcs[i] = copyClock(vectorClockFromMetadata(m.Metadata))
	}
	less := func(i, j int) bool { return happensBefore(vcs[i], vcs[j]) }
	parallelPred := func(i, j int) bool { return !less(i, j) && !less(j, i) }

	ordered = topoSortByPayload(ids, less)

	assigned := make(map[string]bool, len(ids))
	for _, id := range ids {
		if assigned[id] {
			continue
		}
		group := []string{id}
		assigned[id] = true
		for j := 0; j < len(ids); j++ {
			if assigned[ids[j]] {
				continue
			}
			ok := true
			for _, gid := range group {
				if !parallelPred(idToIdx[gid], j) {
					ok = false
					break
				}
			}
			if ok {
				group = append(group, ids[j])
				assigned[ids[j]] = true
			}
		}
		if len(group) > 1 {
			sort.Strings(group)
			parallel = append(parallel, group)
		}
	}
	return ordered, parallel
}

type rbMessage struct {
	Origin  string
	Seq     int
	Payload interface{}
	Clock   map[string]int
}

func (n *ReliableCausalBroadcastNode) handleFirstSeen(m rbMessage) error {
	key := fmt.Sprintf("%s#%d", m.Origin, m.Seq)

	n.mu.Lock()
	if _, ok := n.seen[key]; ok {
		n.mu.Unlock()
		return nil
	}
	n.seen[key] = struct{}{}
	n.pending[key] = m
	for {
		progress := false
		for k, p := range n.pending {
			ok := true
			for nodeID, ts := range p.Clock {
				if nodeID == p.Origin {
					if p.Origin != n.ID() && ts != n.vector[nodeID]+1 {
						ok = false
						break
					}
				} else if ts > n.vector[nodeID] {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			delete(n.pending, k)
			for nodeID, ts := range p.Clock {
				if ts > n.vector[nodeID] {
					n.vector[nodeID] = ts
				}
			}
			n.delivered = append(n.delivered, hive.Message{
				From:     p.Origin,
				To:       n.ID(),
				Payload:  p.Payload,
				Metadata: map[string]any{"vector_clock": copyClock(p.Clock)},
			})
			progress = true
		}
		if !progress {
			break
		}
	}
	n.mu.Unlock()

	for _, nodeID := range n.allNodeIDs {
		if err := n.Send(nodeID, m); err != nil {
			return err
		}
	}
	go n.refloodLoop(m)
	return nil
}

func (n *ReliableCausalBroadcastNode) refloodLoop(m rbMessage) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if !n.IsRunning() {
			return
		}
		for _, nodeID := range n.allNodeIDs {
			_ = n.Send(nodeID, m)
		}
	}
}

func copyClock(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	maps.Copy(out, in)
	return out
}

func vectorClockFromMetadata(metadata map[string]any) map[string]int {
	if metadata == nil {
		return map[string]int{}
	}
	raw, ok := metadata["vector_clock"]
	if !ok {
		return map[string]int{}
	}
	clock, ok := raw.(map[string]int)
	if !ok {
		return map[string]int{}
	}
	return clock
}
