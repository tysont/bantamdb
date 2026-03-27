// ABOUTME: Batch groups transactions into a single epoch for ordered execution,
// ABOUTME: forming the core unit of the Calvin transaction log.
package bdb

import "encoding/json"

// Batch represents a group of transactions assigned to a single timestamp.
// The transaction log produces batches at a fixed interval, and storage
// nodes apply them in timestamp order to maintain deterministic state.
type Batch struct {
	Timestamp    Timestamp
	Transactions []*Transaction
}

// NewBatch creates a batch for the given timestamp with the provided transactions.
func NewBatch(ts Timestamp, transactions []*Transaction) *Batch {
	return &Batch{
		Timestamp:    ts,
		Transactions: transactions,
	}
}

// Marshal serializes a batch to bytes for Raft log entries.
func (b *Batch) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// UnmarshalBatch deserializes a batch from bytes.
func UnmarshalBatch(data []byte) (*Batch, error) {
	var b Batch
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, err
	}
	return &b, nil
}
