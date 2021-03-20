package main

type Log interface {
	Write(transaction Transaction) error
	Range(min uint64, max uint64) (chan *Batch, error)
}
