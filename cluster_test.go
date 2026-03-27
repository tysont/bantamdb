package bdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCluster wires up n nodes with RaftLog + MemoryStorage + Coordinator,
// all connected via ChannelTransport.
type testCluster struct {
	nodes       []*clusterNode
	transport   *ChannelTransport
}

type clusterNode struct {
	id          string
	log         *RaftLog
	storage     *MemoryStorage
	coordinator *Coordinator
}

func newTestClusterFull(t *testing.T, n int) (*testCluster, func()) {
	t.Helper()
	transport := NewChannelTransport()
	var peers []string
	for i := range n {
		peers = append(peers, nodeAddr(i))
	}

	cluster := &testCluster{transport: transport}
	for i := range n {
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
		transport.Register(addr, rl.Node())

		storage := NewMemoryStorage()
		storage.Start(rl.Subscribe())
		coord := NewCoordinator(rl, storage)

		cluster.nodes = append(cluster.nodes, &clusterNode{
			id:          addr,
			log:         rl,
			storage:     storage,
			coordinator: coord,
		})
	}

	// Start all nodes
	for _, n := range cluster.nodes {
		require.NoError(t, n.log.Start())
	}

	cleanup := func() {
		for _, n := range cluster.nodes {
			n.log.Stop()
			n.storage.Stop()
		}
	}
	return cluster, cleanup
}

func (c *testCluster) waitForLeader(t *testing.T, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.log.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for cluster leader")
	return nil
}

func TestCluster_LeaderElection(t *testing.T) {
	assert := assert.New(t)
	cluster, cleanup := newTestClusterFull(t, 3)
	defer cleanup()

	leader := cluster.waitForLeader(t, 3*time.Second)
	assert.NotNil(leader)

	// Exactly one leader
	leaderCount := 0
	for _, n := range cluster.nodes {
		if n.log.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(1, leaderCount)
}

func TestCluster_WriteOnLeaderReadOnFollower(t *testing.T) {
	assert := assert.New(t)
	cluster, cleanup := newTestClusterFull(t, 3)
	defer cleanup()

	leader := cluster.waitForLeader(t, 3*time.Second)
	require.NotNil(t, leader)

	// Write through the leader
	ts, err := leader.coordinator.Put("key1", map[string][]byte{"val": []byte("hello")})
	require.NoError(t, err)
	assert.False(ts.IsZero())

	// Read from a follower -- wait for storage to catch up
	var follower *clusterNode
	for _, n := range cluster.nodes {
		if n != leader {
			follower = n
			break
		}
	}
	require.NotNil(t, follower)

	err = follower.storage.WaitForTimestamp(ts)
	require.NoError(t, err)

	doc, err := follower.coordinator.GetAt("key1", ts)
	assert.NoError(err)
	assert.Equal("hello", string(doc.Fields["val"]))
}

func TestCluster_LeaderFailover(t *testing.T) {
	assert := assert.New(t)
	cluster, cleanup := newTestClusterFull(t, 3)
	defer cleanup()

	leader := cluster.waitForLeader(t, 3*time.Second)
	require.NotNil(t, leader)

	// Write some data first
	ts, err := leader.coordinator.Put("key1", map[string][]byte{"val": []byte("before-failover")})
	require.NoError(t, err)

	// Wait for all followers to have the data
	for _, n := range cluster.nodes {
		if n != leader {
			n.storage.WaitForTimestamp(ts)
		}
	}

	// Kill the leader
	cluster.transport.Disconnect(leader.id)
	leader.log.Stop()

	// Wait for a new leader among remaining nodes
	var remaining []*clusterNode
	for _, n := range cluster.nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	var newLeader *clusterNode
	for time.Now().Before(deadline) {
		for _, n := range remaining {
			if n.log.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotNil(t, newLeader, "new leader should be elected")
	assert.NotEqual(leader.id, newLeader.id)

	// Data should still be readable on the new leader
	doc, err := newLeader.coordinator.GetAt("key1", ts)
	assert.NoError(err)
	assert.Equal("before-failover", string(doc.Fields["val"]))
}

func TestCluster_ConcurrentWrites(t *testing.T) {
	assert := assert.New(t)
	cluster, cleanup := newTestClusterFull(t, 3)
	defer cleanup()

	leader := cluster.waitForLeader(t, 3*time.Second)
	require.NotNil(t, leader)

	n := 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var maxTS Timestamp
	wg.Add(n)
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			id := RandomString(8)
			ts, err := leader.coordinator.Put(id, map[string][]byte{"i": []byte{byte(idx)}})
			if err != nil {
				return
			}
			mu.Lock()
			if ts.After(maxTS) {
				maxTS = ts
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Wait for storage to apply all writes
	require.NoError(t, leader.storage.WaitForTimestamp(maxTS))

	docs, err := leader.coordinator.Scan()
	assert.NoError(err)
	assert.Len(docs, n)
}

func TestCluster_QuorumRequired(t *testing.T) {
	cluster, cleanup := newTestClusterFull(t, 3)
	defer cleanup()

	leader := cluster.waitForLeader(t, 3*time.Second)
	require.NotNil(t, leader)

	// Disconnect both followers -- leader loses quorum
	for _, n := range cluster.nodes {
		if n != leader {
			cluster.transport.Disconnect(n.id)
		}
	}

	// Writes should still be accepted by Append (it blocks) but won't commit.
	// We test this by checking that Propose doesn't get replicated to majority.
	// The leader will still accept to its local log but can't advance commitIndex.
	batch := NewBatch(Timestamp{WallTime: 999}, nil)
	data, _ := batch.Marshal()
	leader.log.Node().Propose(data)

	time.Sleep(200 * time.Millisecond)

	// Commit index should not advance since quorum is lost
	// (The proposed entry is at index 1 but commitIndex stays at 0
	// because only 1 of 3 nodes has it)
	assert.Equal(t, uint64(0), leader.log.Node().state.CommitIndex())
}
