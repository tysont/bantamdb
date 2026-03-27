// ABOUTME: Storage defines the data storage interface for the Calvin protocol.
// ABOUTME: Storage tails the transaction log, validates reads via OCC, and applies writes with MVCC.
package bdb

// Storage is the data storage layer of the Calvin architecture. It serves
// reads at specific points in time using MVCC (multi-version concurrency
// control) and processes writes by tailing the transaction log's batch
// channel. Each batch is validated using optimistic concurrency control
// before writes are applied.
type Storage interface {
	// Get retrieves a document by ID at the given timestamp. Returns the
	// latest version whose timestamp is <= at. Returns ErrNotFound if
	// no version exists at or before that timestamp.
	Get(id string, at Timestamp) (*Document, error)

	// Scan returns all documents at the given timestamp.
	Scan(at Timestamp) ([]*Document, error)

	// AppliedTimestamp returns the timestamp of the last applied batch.
	// This tells the coordinator how far storage has caught up.
	AppliedTimestamp() Timestamp

	// WaitForTimestamp blocks until storage has applied a batch with a
	// timestamp >= ts, or until the storage is stopped.
	WaitForTimestamp(ts Timestamp) error

	// Start begins tailing the provided batch channel from the
	// transaction log. For each batch received, storage validates
	// transactions and applies their writes.
	Start(batches <-chan *Batch) error

	// Stop halts batch processing.
	Stop() error
}
