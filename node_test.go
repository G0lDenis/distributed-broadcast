package node

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nikitakosatka/hive/pkg/failure"
	"github.com/nikitakosatka/hive/pkg/hive"
	"github.com/nikitakosatka/hive/pkg/network"
	timemodel "github.com/nikitakosatka/hive/pkg/time"
)

type broadcast struct {
	From    string
	Payload string
}

const phaseDeliveryTimeout = 5 * time.Second

func TestReliableCausalBroadcastNode_Order(t *testing.T) {
	t.Parallel()

	netCfg := network.NewReliableNetwork()
	timeCfg := timemodel.NewTime(
		timemodel.Synchronous,
		&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
		0.0,
	)
	failCfg := failure.NewCrashStop(0)
	config := hive.NewConfig(
		hive.WithNetwork(netCfg),
		hive.WithTime(timeCfg),
		hive.WithNodesFailures(failCfg),
	)

	tests := []struct {
		name                   string
		nodes                  []string
		phases                 [][]broadcast
		expectedOrderChains    [][]string
		expectedParallelGroups [][]string
	}{
		{
			name:  "single_broadcast_all_receive",
			nodes: []string{"A", "B", "C"},
			phases: [][]broadcast{
				{{From: "A", Payload: "m1"}},
			},
			expectedOrderChains:    nil,
			expectedParallelGroups: nil,
		},
		{
			name:  "three_broadcasts_same_set",
			nodes: []string{"A", "B", "C"},
			phases: [][]broadcast{
				{{From: "A", Payload: "m1"}},
				{{From: "B", Payload: "m2"}},
				{{From: "C", Payload: "m3"}},
			},
			expectedOrderChains:    [][]string{{"m1", "m2"}, {"m1", "m3"}, {"m2", "m3"}},
			expectedParallelGroups: nil,
		},
		{
			name:  "per_node_order_respected",
			nodes: []string{"A", "B"},
			phases: [][]broadcast{
				{{From: "A", Payload: "first"}},
				{{From: "A", Payload: "second"}},
			},
			expectedOrderChains:    [][]string{{"first", "second"}},
			expectedParallelGroups: nil,
		},
		{
			name:  "linear_chain_four_messages",
			nodes: []string{"A", "B", "C"},
			phases: [][]broadcast{
				{{From: "A", Payload: "a"}},
				{{From: "A", Payload: "b"}},
				{{From: "B", Payload: "c"}},
				{{From: "B", Payload: "d"}},
			},
			expectedOrderChains: [][]string{
				{"a", "b"},
				{"b", "c"},
				{"c", "d"},
			},
			expectedParallelGroups: nil,
		},
		{
			name:  "two_broadcasters_same_phase_order",
			nodes: []string{"A", "B", "C"},
			phases: [][]broadcast{
				{{From: "A", Payload: "fromA"}, {From: "B", Payload: "fromB"}},
			},
			expectedOrderChains:    nil,
			expectedParallelGroups: [][]string{{"fromA", "fromB"}},
		},
		{
			name:                   "single_node_no_messages",
			nodes:                  []string{"A"},
			phases:                 nil,
			expectedOrderChains:    nil,
			expectedParallelGroups: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sim := hive.NewSimulator(config)
			defer sim.Stop()

			nodeIDs := tt.nodes
			nodes := make(map[string]BroadcastNode)
			for _, id := range nodeIDs {
				n := NewReliableCausalBroadcastNode(id, nodeIDs)
				nodes[id] = n
				assert.NoError(t, sim.AddNode(n))
			}
			for _, id := range nodeIDs {
				assert.NoError(t, nodes[id].Start(context.Background()))
			}
			sim.Start()

			for _, phase := range tt.phases {
				for _, b := range phase {
					assert.NoError(t, nodes[b.From].Broadcast(b.Payload))
				}
				waitUntilPhaseDelivered(t, nodes, phase, phaseDeliveryTimeout)
			}

			time.Sleep(100 * time.Millisecond)
			for _, id := range nodeIDs {
				assert.NoError(t, nodes[id].Stop())
			}

			var allBroadcasts []broadcast
			for _, phase := range tt.phases {
				allBroadcasts = append(allBroadcasts, phase...)
			}

			// Each payload was broadcast once; every node must deliver it exactly once.
			expectedDelivered := make(map[string]int)
			for _, b := range allBroadcasts {
				expectedDelivered[b.Payload] += len(nodeIDs)
			}
			var allMsgs []hive.Message
			for _, id := range nodeIDs {
				delivered := nodes[id].DeliveredMessages()
				for _, m := range delivered {
					p := fmt.Sprint(m.Payload)
					expectedDelivered[p]--
					allMsgs = append(allMsgs, m)
				}
			}
			for payload, count := range expectedDelivered {
				assert.Equal(t, 0, count, "message %s should be delivered exactly once at each node", payload)
			}

			msgsByPayload := make(map[string]hive.Message)
			for _, m := range allMsgs {
				p := fmt.Sprint(m.Payload)
				if _, ok := msgsByPayload[p]; !ok {
					msgsByPayload[p] = m
				}
			}
			var uniqueMsgs []hive.Message
			for _, m := range msgsByPayload {
				uniqueMsgs = append(uniqueMsgs, m)
			}

			orderer := &Orderer{}
			ordered, parallel := orderer.Order(uniqueMsgs...)
			orderedIdxs := make(map[string]int, len(ordered))
			for i, p := range ordered {
				orderedIdxs[p] = i
			}

			for _, pair := range tt.expectedOrderChains {
				assert.Less(t, orderedIdxs[pair[0]], orderedIdxs[pair[1]])
			}

			assert.ElementsMatch(t, sortInnerStrings(tt.expectedParallelGroups), sortInnerStrings(parallel))
		})
	}
}

func TestCrashRecovery_ReliableDelivery(t *testing.T) {
	config := hive.NewConfig(
		hive.WithNetwork(network.NewReliableNetwork()),
		hive.WithTime(timemodel.NewTime(
			timemodel.Synchronous,
			&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
			0.0,
		)),
		hive.WithNodesFailures(failure.NewCrashRecovery(0.5, 500*time.Millisecond, 0.2)),
	)
	sim := hive.NewSimulator(config)
	defer sim.Stop()

	ids := []string{"A", "B", "C"}
	nodes := make(map[string]BroadcastNode)
	for _, id := range ids {
		n := NewReliableCausalBroadcastNode(id, ids)
		nodes[id] = n
		assert.NoError(t, sim.AddNode(n))
	}
	for _, id := range ids {
		assert.NoError(t, nodes[id].Start(context.Background()))
	}
	sim.Start()

	phases := [][]broadcast{
		{{From: "A", Payload: "m1"}},
		{{From: "B", Payload: "m2"}},
		{{From: "C", Payload: "m3"}},
	}
	for _, phase := range phases {
		for _, b := range phase {
			assert.NoError(t, nodes[b.From].Broadcast(b.Payload))
		}
		waitUntilPhaseDelivered(t, nodes, phase, 25*time.Second)
	}

	time.Sleep(200 * time.Millisecond)
	for _, id := range ids {
		assert.NoError(t, nodes[id].Stop())
	}

	expected := map[string]int{"m1": len(ids), "m2": len(ids), "m3": len(ids)}
	for _, id := range ids {
		for _, m := range nodes[id].DeliveredMessages() {
			p := fmt.Sprint(m.Payload)
			expected[p]--
		}
	}
	for p, count := range expected {
		assert.Equal(t, 0, count, "payload %s should be delivered at each node", p)
	}
}

func waitUntilPhaseDelivered(t *testing.T, nodes map[string]BroadcastNode, phase []broadcast, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, b := range phase {
			for _, n := range nodes {
				delivered := n.DeliveredMessages()
				var found bool
				for _, m := range delivered {
					if fmt.Sprint(m.Payload) == b.Payload {
						found = true
						break
					}
				}
				if !found {
					done = false
					break
				}
			}
			if !done {
				break
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for phase delivery: %+v", phase)
}

func sortInnerStrings(arr [][]string) [][]string {
	sorted := make([][]string, len(arr))
	for i, subArr := range arr {
		sorted[i] = make([]string, len(subArr))
		copy(sorted[i], subArr)
		sort.Strings(sorted[i])
	}
	return sorted
}
