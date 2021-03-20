package main

type Batch struct {
	Epoch uint64
	Transactions []*Transaction
}

func NewBatch(epoch uint64, transactions []*Transaction) *Batch {
	return &Batch{
		Epoch: epoch,
		Transactions: transactions,
	}
}
