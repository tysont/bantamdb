// ABOUTME: Coordinator is the stateless query coordination layer of the Calvin
// ABOUTME: architecture, routing writes through the log and reads direct to storage.
package bdb

// Coordinator is the query coordination layer of the Calvin architecture.
// It accepts client requests, builds well-formed transactions for writes
// and appends them to the transaction log. Write operations block until
// the transaction is committed and return the commit timestamp. Reads are
// served directly from storage.
type Coordinator struct {
	log     Log
	storage Storage
}

// NewCoordinator creates a coordinator backed by the given log and storage.
func NewCoordinator(log Log, storage Storage) *Coordinator {
	return &Coordinator{
		log:     log,
		storage: storage,
	}
}

// Put creates a transaction that writes a single document and appends it
// to the transaction log. Blocks until committed, returns the commit timestamp.
func (c *Coordinator) Put(id string, fields map[string][]byte) (Timestamp, error) {
	doc := NewDocument(id, fields)
	txn := NewTransaction(nil, []*Document{doc}, nil)
	return c.log.Append(txn)
}

// Delete creates a transaction that removes a single document and appends
// it to the transaction log. Blocks until committed, returns the commit timestamp.
func (c *Coordinator) Delete(id string) (Timestamp, error) {
	txn := NewTransaction(nil, nil, []string{id})
	return c.log.Append(txn)
}

// Transact appends an arbitrary transaction to the log. Blocks until
// committed, returns the commit timestamp.
func (c *Coordinator) Transact(txn *Transaction) (Timestamp, error) {
	return c.log.Append(txn)
}

// Get retrieves a document at the latest applied timestamp.
func (c *Coordinator) Get(id string) (*Document, error) {
	at := c.storage.AppliedTimestamp()
	return c.storage.Get(id, at)
}

// GetAt retrieves a document at a specific timestamp. It blocks until
// storage has caught up to the requested timestamp.
func (c *Coordinator) GetAt(id string, at Timestamp) (*Document, error) {
	if err := c.storage.WaitForTimestamp(at); err != nil {
		return nil, err
	}
	return c.storage.Get(id, at)
}

// Scan returns all documents at the latest applied timestamp.
func (c *Coordinator) Scan() ([]*Document, error) {
	at := c.storage.AppliedTimestamp()
	return c.storage.Scan(at)
}

// ScanAt returns all documents at a specific timestamp. It blocks until
// storage has caught up to the requested timestamp.
func (c *Coordinator) ScanAt(at Timestamp) ([]*Document, error) {
	if err := c.storage.WaitForTimestamp(at); err != nil {
		return nil, err
	}
	return c.storage.Scan(at)
}
