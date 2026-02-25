package node

import (
	"github.com/nikitakosatka/hive/pkg/hive"
)

// BroadcastNode is a node that supports reliable causal broadcast.
type BroadcastNode interface {
	hive.Node
	Broadcast(payload interface{}) error
	DeliveredMessages() []hive.Message
}

type ReliableCausalBroadcastNode struct {
	*hive.BaseNode
}

// NewReliableCausalBroadcastNode creates a new node that performs reliable causal broadcast.
func NewReliableCausalBroadcastNode(id string, allNodeIDs []string) BroadcastNode {
	panic("Not implemented")
}

// Broadcast sends the payload to all nodes (including self). Each recipient will
// apply reliable broadcast (flood) and causal delivery.
func (n *ReliableCausalBroadcastNode) Broadcast(payload interface{}) error {
	panic("Not implemented")
}

func (n *ReliableCausalBroadcastNode) Send(to string, payload interface{}) error {
	// To send message use n.SendMessage()
	panic("Not implemented")
}

func (n *ReliableCausalBroadcastNode) Receive(msg *hive.Message) error {
	panic("Not implemented")
}

// DeliveredMessages returns the application-level messages that have been
// delivered to this node, in delivery order.
func (n *ReliableCausalBroadcastNode) DeliveredMessages() []hive.Message {
	panic("Not implemented")
}

// Orderer orders events based on their vector clocks and identifies groups of
// mutually parallel events.
type Orderer struct{}

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	panic("Not implemented")
}
