package bdb

import (
	"sync"
)

type MemoryStorage struct {
	lock      *sync.Mutex
	epoch     uint64
	documents map[string]*Document
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		lock:      &sync.Mutex{},
		epoch:     0,
		documents: make(map[string]*Document),
	}
}

func (s *MemoryStorage) Apply(batch *Batch) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, transaction := range batch.Transactions {
		for _, document := range transaction.Writes {
			s.documents[document.Id] = document
		}
	}
	return nil
}

func (s *MemoryStorage) put(document *Document) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.documents[document.Id] = document
	return nil
}

func (s *MemoryStorage) Get(id string) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	d, ok := s.documents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}
