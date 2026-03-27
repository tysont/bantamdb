// ABOUTME: MemoryLog is an in-memory implementation of the transaction log.
// ABOUTME: It batches transactions into epochs and delivers them via a channel.
package bdb

import (
	"sync"
	"time"
)

var _ Log = (*MemoryLog)(nil)

// MemoryLog implements the Log interface with in-memory storage. It
// accumulates transactions and flushes them as batches on each epoch tick.
type MemoryLog struct {
	mu          sync.Mutex
	config      Config
	clock       *Clock
	pending     []*Transaction
	subscribers []chan *Batch
	ticker      *time.Ticker
	done        chan struct{}
	stopped     bool
}

// NewMemoryLog creates a new in-memory transaction log.
func NewMemoryLog(config Config) *MemoryLog {
	return &MemoryLog{
		config:  config,
		clock:   NewClock(),
		pending: make([]*Transaction, 0),
	}
}

// Append adds a transaction to the current epoch's pending batch.
func (l *MemoryLog) Append(txn *Transaction) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return ErrStopped
	}
	l.pending = append(l.pending, txn)
	return nil
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

// Stop halts epoch ticking and closes all subscriber channels.
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
	for _, ch := range l.subscribers {
		close(ch)
	}
	return nil
}

// Tick manually triggers an epoch flush. This is useful for testing
// without relying on timers.
func (l *MemoryLog) Tick() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	ts := l.clock.Now()
	batch := NewBatch(ts, l.pending)
	l.pending = make([]*Transaction, 0)
	for _, ch := range l.subscribers {
		ch <- batch
	}
}
