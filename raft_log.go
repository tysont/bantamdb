// ABOUTME: RaftLog implements the bdb.Log interface backed by Raft consensus,
// ABOUTME: bridging BantamDB's Calvin protocol to distributed log replication.
package bdb

import (
	"sync"
	"time"
)

// RaftLog implements the Log interface using Raft consensus for distributed
// transaction batching. The leader collects pending transactions, batches
// them on each epoch tick, and proposes the batch through Raft. Committed
// batches are delivered to subscribers on all nodes via the apply loop.
type RaftLog struct {
	mu          sync.Mutex
	config      Config
	clock       *Clock
	node        *RaftNode
	pending     []raftPendingEntry
	subscribers []chan *Batch
	ticker      *time.Ticker
	done        chan struct{}
	stopped     bool
}

type raftPendingEntry struct {
	txn       *Transaction
	committed chan Timestamp
}

// NewRaftLog creates a new Raft-backed transaction log.
func NewRaftLog(config Config, id string, peers []string, transport Transport) *RaftLog {
	rl := &RaftLog{
		config: config,
		clock:  NewClock(),
	}
	rl.node = NewRaftNode(id, peers, transport)
	return rl
}

// IsLeader reports whether this node is the Raft leader.
func (l *RaftLog) IsLeader() bool {
	return l.node.Role() == Leader
}

// Append adds a transaction to the pending batch. On the leader, it blocks
// until the batch is committed through Raft. On a follower, returns ErrNotLeader.
func (l *RaftLog) Append(txn *Transaction) (Timestamp, error) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return TimestampZero(), ErrStopped
	}
	if !l.IsLeader() {
		l.mu.Unlock()
		return TimestampZero(), ErrNotLeader
	}
	entry := raftPendingEntry{
		txn:       txn,
		committed: make(chan Timestamp, 1),
	}
	l.pending = append(l.pending, entry)
	l.mu.Unlock()

	ts, ok := <-entry.committed
	if !ok {
		return TimestampZero(), ErrStopped
	}
	return ts, nil
}

// Subscribe returns a channel that receives committed batches.
func (l *RaftLog) Subscribe() <-chan *Batch {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch := make(chan *Batch, 128)
	l.subscribers = append(l.subscribers, ch)
	return ch
}

// Start begins the Raft node and epoch ticking.
func (l *RaftLog) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return ErrStopped
	}
	l.done = make(chan struct{})

	// Start the Raft node (election timer, etc.)
	l.node.Start()

	// Start the apply loop -- reads committed entries from Raft
	go l.applyLoop()

	// Start the epoch ticker -- leader batches and proposes
	l.ticker = time.NewTicker(l.config.EpochDuration)
	go l.tickLoop()

	return nil
}

// Stop halts the Raft log, node, and all goroutines.
func (l *RaftLog) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return nil
	}
	l.stopped = true
	if l.ticker != nil {
		l.ticker.Stop()
	}
	if l.done != nil {
		close(l.done)
	}
	l.node.Stop()
	for _, entry := range l.pending {
		close(entry.committed)
	}
	l.pending = nil
	for _, ch := range l.subscribers {
		close(ch)
	}
	return nil
}

func (l *RaftLog) tickLoop() {
	for {
		select {
		case <-l.ticker.C:
			l.tick()
		case <-l.done:
			return
		}
	}
}

// tick collects pending transactions, creates a batch, and proposes it
// through Raft. Only runs on the leader -- followers have no pending entries.
func (l *RaftLog) tick() {
	l.mu.Lock()
	if l.stopped || !l.IsLeader() || len(l.pending) == 0 {
		l.mu.Unlock()
		return
	}
	// Snapshot the pending entries but don't remove them.
	// The applyLoop will resolve and remove them when the commit arrives.
	txns := make([]*Transaction, len(l.pending))
	for i, e := range l.pending {
		txns[i] = e.txn
	}
	l.mu.Unlock()

	ts := l.clock.Now()
	batch := NewBatch(ts, txns)
	data, err := batch.Marshal()
	if err != nil {
		return
	}

	_, err = l.node.Propose(data)
	if err != nil {
		// Proposal failed -- unblock all current waiters
		l.mu.Lock()
		for _, e := range l.pending {
			close(e.committed)
		}
		l.pending = nil
		l.mu.Unlock()
	}
}

// applyLoop reads committed log entries from the Raft node and delivers
// them as batches to subscribers. Runs on all nodes (leader and followers).
func (l *RaftLog) applyLoop() {
	for {
		select {
		case entry, ok := <-l.node.ApplyCh():
			if !ok {
				return
			}
			batch, err := UnmarshalBatch(entry.Data)
			if err != nil {
				continue
			}

			l.mu.Lock()
			// Deliver to subscribers
			for _, ch := range l.subscribers {
				select {
				case ch <- batch:
				default:
				}
			}
			// Resolve pending entries that match this batch
			l.resolvePendingLocked(batch.Timestamp)
			l.mu.Unlock()

		case <-l.done:
			return
		}
	}
}

// resolvePendingLocked notifies blocked Append callers that their
// transactions have been committed. Called when a batch is applied.
func (l *RaftLog) resolvePendingLocked(ts Timestamp) {
	for _, entry := range l.pending {
		entry.committed <- ts
	}
	l.pending = nil
}
