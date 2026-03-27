package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Verify RaftLog satisfies the Log interface at compile time.
var _ Log = (*RaftLog)(nil)

func newTestRaftLog(t *testing.T, nodeCount int) ([]*RaftLog, func()) {
	t.Helper()
	transport := NewChannelTransport()
	var logs []*RaftLog
	var peers []string
	for i := range nodeCount {
		peers = append(peers, nodeAddr(i))
	}
	for i := range nodeCount {
		addr := peers[i]
		var otherPeers []string
		for j, p := range peers {
			if j != i {
				otherPeers = append(otherPeers, p)
			}
		}
		cfg := DefaultConfig()
		cfg.EpochDuration = 20 * time.Millisecond
		rl := NewRaftLog(cfg, addr, otherPeers, transport)
		transport.Register(addr, rl.node)
		logs = append(logs, rl)
	}
	cleanup := func() {
		for _, l := range logs {
			l.Stop()
		}
	}
	return logs, cleanup
}

func waitForRaftLeader(t *testing.T, logs []*RaftLog, timeout time.Duration) *RaftLog {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, l := range logs {
			if l.IsLeader() {
				return l
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Raft leader")
	return nil
}

func TestRaftLog_SingleNodeAppend(t *testing.T) {
	assert := assert.New(t)
	logs, cleanup := newTestRaftLog(t, 1)
	defer cleanup()

	ch := logs[0].Subscribe()
	require.NoError(t, logs[0].Start())

	// Wait for leader election
	waitForRaftLeader(t, logs, 2*time.Second)

	// Append a transaction -- should block until committed
	txn := NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil)
	ts, err := logs[0].Append(txn)
	require.NoError(t, err)
	assert.False(ts.IsZero())

	// Batch should appear on subscriber channel
	select {
	case batch := <-ch:
		require.NotNil(t, batch)
		assert.Len(batch.Transactions, 1)
		assert.Equal("a", batch.Transactions[0].Writes[0].Id)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch")
	}
}

func TestRaftLog_ThreeNodeReplication(t *testing.T) {
	assert := assert.New(t)
	logs, cleanup := newTestRaftLog(t, 3)
	defer cleanup()

	// Subscribe to all nodes
	channels := make([]<-chan *Batch, len(logs))
	for i, l := range logs {
		channels[i] = l.Subscribe()
		require.NoError(t, l.Start())
	}

	leader := waitForRaftLeader(t, logs, 2*time.Second)
	require.NotNil(t, leader)

	// Append through the leader
	txn := NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil)
	ts, err := leader.Append(txn)
	require.NoError(t, err)
	assert.False(ts.IsZero())

	// All nodes should receive the batch on their subscriber channels
	for i, ch := range channels {
		select {
		case batch := <-ch:
			require.NotNil(t, batch)
			assert.Len(batch.Transactions, 1, "node %d should receive the batch", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("node %d timed out waiting for batch", i)
		}
	}
}

func TestRaftLog_AppendOnFollower(t *testing.T) {
	logs, cleanup := newTestRaftLog(t, 3)
	defer cleanup()

	for _, l := range logs {
		l.Subscribe()
		require.NoError(t, l.Start())
	}

	leader := waitForRaftLeader(t, logs, 2*time.Second)
	require.NotNil(t, leader)

	// Find a follower
	var follower *RaftLog
	for _, l := range logs {
		if l != leader {
			follower = l
			break
		}
	}
	require.NotNil(t, follower)

	// Append on follower should fail with ErrNotLeader
	txn := NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil)
	_, err := follower.Append(txn)
	assert.ErrorIs(t, err, ErrNotLeader)
}
