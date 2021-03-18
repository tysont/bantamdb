package main

type Storage interface {
	Put(document *Document) error
	Get(key string) (*Document, error)
}
