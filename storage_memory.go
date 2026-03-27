// ABOUTME: MemoryStorage is an in-memory implementation of the storage layer.
// ABOUTME: It tails the transaction log, validates OCC, and applies writes.
package bdb

import (
	"sync"
)

var _ Storage = (*MemoryStorage)(nil)

// MemoryStorage implements the Storage interface with in-memory maps.
// It tails the transaction log's batch channel, validates each transaction
// using optimistic concurrency control, and applies valid writes.
type MemoryStorage struct {
	mu         sync.RWMutex
	documents  map[string]*Document
	timestamps map[string]Timestamp // last write timestamp per document ID
	done       chan struct{}
	stopped    bool
}

// NewMemoryStorage creates a new in-memory storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		documents:  make(map[string]*Document),
		timestamps: make(map[string]Timestamp),
	}
}

// Get retrieves a document by ID. Returns ErrNotFound if the document
// does not exist.
func (s *MemoryStorage) Get(id string) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.documents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}

// Scan returns all documents in the store.
func (s *MemoryStorage) Scan() ([]*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	docs := make([]*Document, 0, len(s.documents))
	for _, d := range s.documents {
		docs = append(docs, d)
	}
	return docs, nil
}

// Start begins tailing the batch channel from the transaction log.
// For each batch, it validates and applies transactions in order.
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

// Stop halts batch processing.
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
	return nil
}

// applyBatch processes all transactions in a batch, validating OCC
// constraints and applying writes for valid transactions.
func (s *MemoryStorage) applyBatch(batch *Batch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, txn := range batch.Transactions {
		if !s.validate(txn, batch.Timestamp) {
			continue
		}
		for _, doc := range txn.Writes {
			s.documents[doc.Id] = doc
			s.timestamps[doc.Id] = batch.Timestamp
		}
		for _, id := range txn.Deletes {
			delete(s.documents, id)
			s.timestamps[id] = batch.Timestamp
		}
	}
}

// validate checks that no key in the transaction's read set was written
// at a timestamp later than the batch timestamp. This is the optimistic
// concurrency control check matching Fauna's storage validation.
func (s *MemoryStorage) validate(txn *Transaction, ts Timestamp) bool {
	for _, id := range txn.ReadSet {
		if writeTS, ok := s.timestamps[id]; ok {
			if writeTS.After(ts) {
				return false
			}
		}
	}
	return true
}
