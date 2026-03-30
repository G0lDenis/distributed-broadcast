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

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	if len(msgs) == 0 {
		return nil, nil
	}

	type event struct {
		payload string
		clock   map[string]int
	}
	events := make([]event, 0, len(msgs))
	for _, m := range msgs {
		events = append(events, event{
			payload: fmt.Sprint(m.Payload),
			clock:   extractClock(m.Metadata),
		})
	}

	adj := make([][]int, len(events))
	indeg := make([]int, len(events))
	for i := 0; i < len(events); i++ {
		for j := i + 1; j < len(events); j++ {
			aBeforeB := clockLTE(events[i].clock, events[j].clock) && !clockLTE(events[j].clock, events[i].clock)
			bBeforeA := clockLTE(events[j].clock, events[i].clock) && !clockLTE(events[i].clock, events[j].clock)
			if aBeforeB {
				adj[i] = append(adj[i], j)
				indeg[j]++
			}
			if bBeforeA {
				adj[j] = append(adj[j], i)
				indeg[i]++
			}
		}
	}

	available := make([]int, 0, len(events))
	for i := range events {
		if indeg[i] == 0 {
			available = append(available, i)
		}
	}
	sort.Slice(available, func(i, j int) bool {
		return events[available[i]].payload < events[available[j]].payload
	})

	for len(available) > 0 {
		cur := available[0]
		available = available[1:]
		ordered = append(ordered, events[cur].payload)
		for _, to := range adj[cur] {
			indeg[to]--
			if indeg[to] == 0 {
				available = append(available, to)
			}
		}
		sort.Slice(available, func(i, j int) bool {
			return events[available[i]].payload < events[available[j]].payload
		})
	}

	for i := 0; i < len(events); i++ {
		for j := i + 1; j < len(events); j++ {
			aBeforeB := clockLTE(events[i].clock, events[j].clock) && !clockLTE(events[j].clock, events[i].clock)
			bBeforeA := clockLTE(events[j].clock, events[i].clock) && !clockLTE(events[i].clock, events[j].clock)
			if !aBeforeB && !bBeforeA {
				parallel = append(parallel, []string{events[i].payload, events[j].payload})
			}
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

func extractClock(metadata map[string]any) map[string]int {
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

func clockLTE(a, b map[string]int) bool {
	for k, av := range a {
		if av > b[k] {
			return false
		}
	}
	return true
}
