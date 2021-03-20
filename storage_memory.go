package main

import (
	"errors"
	"sort"
	"sync"
)

type MemoryStorage struct {
	lock *sync.Mutex
	epoch uint64
	documents map[uint64]*Document
	locations []uint64
	epochs map[uint64]uint64
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		lock: &sync.Mutex{},
		epoch: 0,
		documents: make(map[uint64]*Document, 0),
		locations: make([]uint64, 0),
		epochs: make(map[uint64]uint64, 0),
	}
}

func (s *MemoryStorage) Apply(batch *Batch) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, transaction := range batch.Transactions {
		for _, check := range transaction.Checks {
			l := GetMurmurHash(check)
			if e, ok := s.epochs[l]; ok {
				if e > batch.Epoch {
					return errors.New("optimistic concurrency control check failed")
				}
			}
		}
		for _, document := range transaction.Writes {
			err := s.put(document)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *MemoryStorage) put(document *Document) error {
	l := GetMurmurHash(document.Id)
	s.documents[l] = document
	s.locations = append(s.locations, l)
	i := sort.Search(len(s.locations), func(i int) bool { return s.locations[i] > l })
	if i < len(s.locations) {
		copy(s.locations[i+1:], s.locations[i:])
		s.locations[i] = l
	}
	s.epochs[l] = s.epoch
	return nil
}

func (s *MemoryStorage) Get(id string) (*Document, error) {
	l := GetMurmurHash(id)
	return s.documents[l], nil
}

func (s *MemoryStorage) Range(min uint64, max uint64) (chan *Document, error) {
	c := make(chan *Document, 128)
	n := sort.Search(len(s.locations), func(i int) bool { return s.locations[i] >= min })
	go func() {
		for i := n; i < len(s.locations) && s.locations[i] <= max; i++ {
			l := s.locations[i]
			d := s.documents[l]
			c <- d
		}
		close(c)
	}()
	return c, nil
}
