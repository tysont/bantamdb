package main

type Storage interface {
	Apply(batch *Batch) error
	Get(id string) (*Document, error)
	Range(min uint32, max uint32) (chan *Document, error)
}
