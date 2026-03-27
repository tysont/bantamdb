// ABOUTME: Storage defines the data storage interface for the Calvin protocol.
// ABOUTME: Storage tails the transaction log, validates reads, and applies writes.
package bdb

// Storage is the data storage layer of the Calvin architecture. It serves
// reads directly and processes writes by tailing the transaction log's
// batch channel. Each batch is validated using optimistic concurrency
// control before writes are applied.
type Storage interface {
	// Get retrieves a document by ID. Returns ErrNotFound if the
	// document does not exist.
	Get(id string) (*Document, error)

	// Scan returns all documents in the store.
	Scan() ([]*Document, error)

	// Start begins tailing the provided batch channel from the
	// transaction log. For each batch received, storage validates
	// transactions and applies their writes.
	Start(batches <-chan *Batch) error

	// Stop halts batch processing.
	Stop() error
}

// TransactionResult reports the outcome of a single transaction's execution.
type TransactionResult struct {
	Err error
}
