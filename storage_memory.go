// ABOUTME: MemoryStorage is an in-memory MVCC implementation of the storage layer.
// ABOUTME: It tails the transaction log, validates OCC, and stores multiple document versions.
package bdb

import (
	"sync"
)

var _ Storage = (*MemoryStorage)(nil)

// version represents a single point-in-time version of a document.
// A nil Doc indicates a tombstone (deletion).
type version struct {
	Timestamp Timestamp
	Doc       *Document
}

// MemoryStorage implements the Storage interface with in-memory MVCC.
// Each document ID maps to a list of versions ordered by timestamp.
// Reads at a specific timestamp return the latest version at or before
// that point. OCC validation checks that read-set keys haven't been
// written after the batch timestamp.
type MemoryStorage struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	versions map[string][]version
	applied  Timestamp
	done     chan struct{}
	stopped  bool
}

// NewMemoryStorage creates a new in-memory MVCC storage instance.
func NewMemoryStorage() *MemoryStorage {
	s := &MemoryStorage{
		versions: make(map[string][]version),
	}
	s.cond = sync.NewCond(s.mu.RLocker())
	return s
}

// Get retrieves the latest version of a document at or before the given
// timestamp. Returns ErrNotFound if no version exists or if the latest
// version at that point is a tombstone.
func (s *MemoryStorage) Get(id string, at Timestamp) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getAt(id, at)
}

// getAt is the lock-free internal implementation of Get. Caller must hold
// at least a read lock.
func (s *MemoryStorage) getAt(id string, at Timestamp) (*Document, error) {
	vs, ok := s.versions[id]
	if !ok {
		return nil, ErrNotFound
	}
	// Find the latest version with timestamp <= at by scanning backward.
	for i := len(vs) - 1; i >= 0; i-- {
		if !vs[i].Timestamp.After(at) {
			if vs[i].Doc == nil {
				return nil, ErrNotFound // tombstone
			}
			return vs[i].Doc, nil
		}
	}
	return nil, ErrNotFound
}

// Scan returns all documents that exist at the given timestamp.
func (s *MemoryStorage) Scan(at Timestamp) ([]*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var docs []*Document
	for id := range s.versions {
		doc, err := s.getAt(id, at)
		if err == nil {
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

// AppliedTimestamp returns the timestamp of the last applied batch.
func (s *MemoryStorage) AppliedTimestamp() Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// WaitForTimestamp blocks until storage has applied a batch with a
// timestamp >= ts, or until the storage is stopped.
func (s *MemoryStorage) WaitForTimestamp(ts Timestamp) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for !s.applied.After(ts) && s.applied.Compare(ts) != 0 && !s.stopped {
		s.cond.Wait()
	}
	if s.stopped && s.applied.Compare(ts) < 0 {
		return ErrStopped
	}
	return nil
}

// Start begins tailing the batch channel from the transaction log.
func (s *MemoryStorage) Start(batches <-chan *Batch) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	s.done = make(chan struct{})
	s.mu.Unlock()

	go func() {
		for {
			select {
			case batch, ok := <-batches:
				if !ok {
					return
				}
				s.applyBatch(batch)
			case <-s.done:
				return
			}
		}
	}()
	return nil
}

// Stop halts batch processing and wakes any waiters.
func (s *MemoryStorage) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	s.stopped = true
	if s.done != nil {
		close(s.done)
	}
	s.cond.Broadcast()
	return nil
}

// applyBatch processes all transactions in a batch with OCC validation
// and MVCC version storage.
func (s *MemoryStorage) applyBatch(batch *Batch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, txn := range batch.Transactions {
		if !s.validate(txn, batch.Timestamp) {
			continue
		}
		for _, doc := range txn.Writes {
			s.versions[doc.Id] = append(s.versions[doc.Id], version{
				Timestamp: batch.Timestamp,
				Doc:       doc,
			})
		}
		for _, id := range txn.Deletes {
			s.versions[id] = append(s.versions[id], version{
				Timestamp: batch.Timestamp,
				Doc:       nil, // tombstone
			})
		}
	}
	s.applied = batch.Timestamp
	s.cond.Broadcast()
}

// validate checks OCC: no key in the read set was written after the
// batch timestamp.
func (s *MemoryStorage) validate(txn *Transaction, ts Timestamp) bool {
	for _, id := range txn.ReadSet {
		if vs, ok := s.versions[id]; ok && len(vs) > 0 {
			latest := vs[len(vs)-1].Timestamp
			if latest.After(ts) {
				return false
			}
		}
	}
	return true
}
