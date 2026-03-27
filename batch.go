// ABOUTME: Batch groups transactions into a single epoch for ordered execution,
// ABOUTME: forming the core unit of the Calvin transaction log.
package bdb

// Batch represents a group of transactions assigned to a single epoch.
// The transaction log produces batches at a fixed interval, and storage
// nodes apply them in epoch order to maintain deterministic state.
type Batch struct {
	Epoch        uint64
	Transactions []*Transaction
}

// NewBatch creates a batch for the given epoch with the provided transactions.
func NewBatch(epoch uint64, transactions []*Transaction) *Batch {
	return &Batch{
		Epoch:        epoch,
		Transactions: transactions,
	}
}
