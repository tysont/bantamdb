// ABOUTME: Log defines the transaction log interface for the Calvin protocol.
// ABOUTME: The log batches transactions into epochs and delivers them to subscribers.
package bdb

// Log is the transaction log layer of the Calvin architecture. It accepts
// transactions, groups them into epoch-based batches on a fixed interval,
// and delivers completed batches to subscribers for execution by the
// storage layer.
type Log interface {
	// Append adds a transaction to the current epoch's pending batch.
	Append(txn *Transaction) error

	// Subscribe returns a channel that receives completed batches as
	// each epoch is flushed. Must be called before Start.
	Subscribe() <-chan *Batch

	// Start begins epoch ticking. Batches are flushed and delivered
	// to subscribers at each tick.
	Start() error

	// Stop halts epoch ticking and closes subscriber channels.
	Stop() error
}
