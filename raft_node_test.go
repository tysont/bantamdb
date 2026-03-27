package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCluster creates n RaftNodes connected via a ChannelTransport.
// Returns the nodes and a cleanup function.
func newTestCluster(t *testing.T, n int) ([]*RaftNode, *ChannelTransport, func()) {
	t.Helper()
	transport := NewChannelTransport()
	var nodes []*RaftNode
	var peers []string
	for i := range n {
		addr := nodeAddr(i)
		peers = append(peers, addr)
	}
	for i := range n {
		addr := peers[i]
		var otherPeers []string
		for j, p := range peers {
			if j != i {
				otherPeers = append(otherPeers, p)
			}
		}
		node := NewRaftNode(addr, otherPeers, transport)
		transport.Register(addr, node)
		nodes = append(nodes, node)
	}
	cleanup := func() {
		for _, n := range nodes {
			n.Stop()
		}
	}
	return nodes, transport, cleanup
}

func nodeAddr(i int) string {
	return "node-" + string(rune('A'+i))
}

// waitForLeader polls until exactly one node is leader, with timeout.
func waitForLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*RaftNode
		for _, n := range nodes {
			if n.Role() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for single leader")
	return nil
}

func TestRaftNode_SingleNodeElection(t *testing.T) {
	assert := assert.New(t)
	nodes, _, cleanup := newTestCluster(t, 1)
	defer cleanup()

	nodes[0].Start()
	leader := waitForLeader(t, nodes, 2*time.Second)
	assert.Equal(nodes[0], leader)
	assert.Equal(Leader, nodes[0].Role())
}

func TestRaftNode_ThreeNodeElection(t *testing.T) {
	assert := assert.New(t)
	nodes, _, cleanup := newTestCluster(t, 3)
	defer cleanup()

	for _, n := range nodes {
		n.Start()
	}

	leader := waitForLeader(t, nodes, 2*time.Second)
	assert.NotNil(leader)

	// Exactly one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.Role() == Leader {
			leaderCount++
		}
	}
	assert.Equal(1, leaderCount)
}

func TestRaftNode_HeartbeatPreventsElection(t *testing.T) {
	assert := assert.New(t)
	nodes, _, cleanup := newTestCluster(t, 3)
	defer cleanup()

	for _, n := range nodes {
		n.Start()
	}
	leader := waitForLeader(t, nodes, 2*time.Second)
	require.NotNil(t, leader)

	// Wait a bit -- leader should remain stable due to heartbeats
	time.Sleep(500 * time.Millisecond)

	leaderCount := 0
	for _, n := range nodes {
		if n.Role() == Leader {
			leaderCount++
		}
	}
	assert.Equal(1, leaderCount)
}

func TestRaftNode_ProposeAndCommit(t *testing.T) {
	assert := assert.New(t)
	nodes, _, cleanup := newTestCluster(t, 3)
	defer cleanup()

	for _, n := range nodes {
		n.Start()
	}
	leader := waitForLeader(t, nodes, 2*time.Second)
	require.NotNil(t, leader)

	// Propose data through the leader
	batch := NewBatch(Timestamp{WallTime: 100}, []*Transaction{
		NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil),
	})
	data, err := batch.Marshal()
	require.NoError(t, err)

	index, err := leader.Propose(data)
	require.NoError(t, err)
	assert.Equal(uint64(1), index)

	// Wait for commit and application
	time.Sleep(200 * time.Millisecond)

	// All nodes should have the entry committed
	for _, n := range nodes {
		assert.GreaterOrEqual(n.state.CommitIndex(), uint64(1),
			"node %s should have committed the entry", n.id)
	}
}

func TestRaftNode_ApplyChannel(t *testing.T) {
	assert := assert.New(t)
	nodes, _, cleanup := newTestCluster(t, 3)
	defer cleanup()

	for _, n := range nodes {
		n.Start()
	}
	leader := waitForLeader(t, nodes, 2*time.Second)
	require.NotNil(t, leader)

	batch := NewBatch(Timestamp{WallTime: 100}, nil)
	data, _ := batch.Marshal()
	leader.Propose(data)

	// The leader's apply channel should receive the committed entry
	select {
	case entry := <-leader.ApplyCh():
		assert.Equal(uint64(1), entry.Index)
		decoded, err := UnmarshalBatch(entry.Data)
		assert.NoError(err)
		assert.Equal(Timestamp{WallTime: 100}, decoded.Timestamp)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for apply")
	}
}

func TestRaftNode_LeaderFailover(t *testing.T) {
	assert := assert.New(t)
	nodes, transport, cleanup := newTestCluster(t, 3)
	defer cleanup()

	for _, n := range nodes {
		n.Start()
	}
	leader := waitForLeader(t, nodes, 2*time.Second)
	require.NotNil(t, leader)

	// Disconnect the leader
	transport.Disconnect(leader.ID())
	leader.Stop()

	// Remaining nodes should elect a new leader
	var remaining []*RaftNode
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	assert.NotNil(newLeader)
	assert.NotEqual(leader.ID(), newLeader.ID())
}
