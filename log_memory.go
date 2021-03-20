package main

import (
	"sync"
	"time"
)

type MemoryLog struct
{
	lock *sync.Mutex
	ticker *time.Ticker
	batch *Batch
	batches map[uint64]*Batch
}

func NewMemoryLog() *MemoryLog {
	l := &MemoryLog{
		lock: &sync.Mutex{},
		ticker: time.NewTicker(epochMilliseconds * time.Millisecond),
		batch: NewBatch(0, make([]*Transaction, 0)),
		batches: make(map[uint64]*Batch, 0),
	}
	go func() {
		for _ = range l.ticker.C {
			l.tick()
		}
	}()
	return l
}

func (l *MemoryLog) Write(transaction *Transaction) error {
	l.batch.Transactions = append(l.batch.Transactions, transaction)
	return nil
}

func (l *MemoryLog) Range(min uint64) (chan *Batch, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	c := make(chan *Batch, 128)
	go func() {
		for i := min; i < l.batch.Epoch; i++ {
			if b, ok := l.batches[i]; ok {
				c <- b
			}
		}
		close(c)
	}()
	return c, nil
}

func (l *MemoryLog) tick() {
	l.lock.Lock()
	defer l.lock.Unlock()
	b := NewBatch(l.batch.Epoch + 1, make([]*Transaction, 0))
	l.batches[b.Epoch] = l.batch
	l.batch = b
}