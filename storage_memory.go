package main

import (
	"sort"
)

type MemoryStorage struct {
	locations []uint32
	documents map[uint32]*Document
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		locations: make([]uint32, 0),
		documents: make(map[uint32]*Document, 0),
	}
}

func (s *MemoryStorage) Put(document *Document) error {
	l := GetMurmurHash(document.Key)
	s.documents[l] = document
	s.locations = append(s.locations, l)
	i := sort.Search(len(s.locations), func(i int) bool { return s.locations[i] > l })
	if i < len(s.locations) {
		copy(s.locations[i+1:], s.locations[i:])
		s.locations[i] = l
	}
	return nil
}

func (s *MemoryStorage) Get(key string) (*Document, error) {
	l := GetMurmurHash(key)
	return s.documents[l], nil
}

func (s *MemoryStorage) GetRange(min uint32, max uint32) (chan *Document, error) {
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
