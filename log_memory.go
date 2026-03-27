// ABOUTME: MemoryLog is an in-memory implementation of the transaction log.
// ABOUTME: It batches transactions into epochs and delivers them via a channel.
package bdb

import (
	"sync"
	"time"
)

var _ Log = (*MemoryLog)(nil)

// pendingEntry is a transaction waiting to be committed in the next batch.
type pendingEntry struct {
	txn       *Transaction
	committed chan Timestamp
}

// MemoryLog implements the Log interface with in-memory storage. It
// accumulates transactions and flushes them as batches on each epoch tick.
// Append blocks until the transaction's batch is flushed, returning the
// commit timestamp.
type MemoryLog struct {
	mu          sync.Mutex
	config      Config
	clock       *Clock
	pending     []pendingEntry
	subscribers []chan *Batch
	ticker      *time.Ticker
	done        chan struct{}
	stopped     bool
}

// NewMemoryLog creates a new in-memory transaction log.
func NewMemoryLog(config Config) *MemoryLog {
	return &MemoryLog{
		config: config,
		clock:  NewClock(),
	}
}

// Append adds a transaction to the current epoch's pending batch and
// blocks until the batch is committed. Returns the commit timestamp.
func (l *MemoryLog) Append(txn *Transaction) (Timestamp, error) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return TimestampZero(), ErrStopped
	}
	entry := pendingEntry{
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

// Subscribe returns a channel that will receive completed batches.
// Must be called before Start.
func (l *MemoryLog) Subscribe() <-chan *Batch {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch := make(chan *Batch, 128)
	l.subscribers = append(l.subscribers, ch)
	return ch
}

// Start begins automatic epoch ticking at the configured interval.
func (l *MemoryLog) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return ErrStopped
	}
	l.ticker = time.NewTicker(l.config.EpochDuration)
	l.done = make(chan struct{})
	go func() {
		for {
			select {
			case <-l.ticker.C:
				l.Tick()
			case <-l.done:
				return
			}
		}
	}()
	return nil
}

// Stop halts epoch ticking, closes all subscriber channels, and unblocks
// any pending Append calls.
func (l *MemoryLog) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return nil
	}
	l.stopped = true
	if l.ticker != nil {
		l.ticker.Stop()
		close(l.done)
	}
	for _, entry := range l.pending {
		close(entry.committed)
	}
	l.pending = nil
	for _, ch := range l.subscribers {
		close(ch)
	}
	return nil
}

// Tick manually triggers an epoch flush. Pending transactions are batched
// together, delivered to subscribers, and their Append callers are
// unblocked with the batch timestamp.
func (l *MemoryLog) Tick() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	ts := l.clock.Now()
	txns := make([]*Transaction, len(l.pending))
	entries := l.pending
	for i, e := range entries {
		txns[i] = e.txn
	}
	l.pending = nil

	batch := NewBatch(ts, txns)
	for _, ch := range l.subscribers {
		ch <- batch
	}
	for _, entry := range entries {
		entry.committed <- ts
		close(entry.committed)
	}
}
