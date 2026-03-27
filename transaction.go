// ABOUTME: Transaction represents a unit of work in the Calvin protocol.
// ABOUTME: Transactions declare their full read and write sets upfront.
package bdb

// Transaction represents an atomic unit of work. Following the Calvin
// protocol, transactions must declare their complete read and write sets
// before execution so the system can determine ordering and detect conflicts.
type Transaction struct {
	ReadSet []string
	WriteSet []string
	Writes  []*Document
	Deletes []string
}

// NewTransaction creates a transaction with the given read set and writes.
// The write set is derived automatically from the writes and deletes.
func NewTransaction(readSet []string, writes []*Document, deletes []string) *Transaction {
	writeSet := make([]string, 0, len(writes)+len(deletes))
	for _, w := range writes {
		writeSet = append(writeSet, w.Id)
	}
	writeSet = append(writeSet, deletes...)
	return &Transaction{
		ReadSet:  readSet,
		WriteSet: writeSet,
		Writes:   writes,
		Deletes:  deletes,
	}
}
