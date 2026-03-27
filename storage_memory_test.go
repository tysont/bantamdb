package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sendBatch is a test helper that sends a batch on a channel and waits
// briefly for storage to process it.
func sendBatch(ch chan<- *Batch, batch *Batch) {
	ch <- batch
	time.Sleep(5 * time.Millisecond)
}

func TestMemoryStorage_GetMissing(t *testing.T) {
	s := NewMemoryStorage()
	_, err := s.Get("nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestMemoryStorage_ApplySingleWrite(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 1)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	txn := NewTransaction(nil, []*Document{d}, nil)
	sendBatch(ch, NewBatch(1, []*Transaction{txn}))

	doc, err := s.Get("a")
	assert.NoError(err)
	assert.Equal("v", string(doc.Fields["k"]))
}

func TestMemoryStorage_ApplyOverwrite(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	d1 := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(1, []*Transaction{NewTransaction(nil, []*Document{d1}, nil)}))

	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	sendBatch(ch, NewBatch(2, []*Transaction{NewTransaction(nil, []*Document{d2}, nil)}))

	doc, err := s.Get("a")
	assert.NoError(err)
	assert.Equal("v2", string(doc.Fields["k"]))
}

func TestMemoryStorage_ApplyDelete(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	sendBatch(ch, NewBatch(1, []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	doc, err := s.Get("a")
	assert.NoError(err)
	assert.NotNil(doc)

	sendBatch(ch, NewBatch(2, []*Transaction{NewTransaction(nil, nil, []string{"a"})}))

	_, err = s.Get("a")
	assert.ErrorIs(err, ErrNotFound)
}

func TestMemoryStorage_ApplyOCCConflict(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write "a" at epoch 2
	d := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(2, []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	// Try to read "a" in a transaction at epoch 1 (stale read)
	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	txn := NewTransaction([]string{"a"}, []*Document{d2}, nil)
	sendBatch(ch, NewBatch(1, []*Transaction{txn}))

	// The conflicting transaction's write should be rejected, original value preserved
	doc, err := s.Get("a")
	assert.NoError(err)
	assert.Equal("v1", string(doc.Fields["k"]))
}

func TestMemoryStorage_ApplyOCCSuccess(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write "a" at epoch 1
	d := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(1, []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	// Read "a" and write it at epoch 2 (valid, epoch >= last write epoch)
	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	txn := NewTransaction([]string{"a"}, []*Document{d2}, nil)
	sendBatch(ch, NewBatch(2, []*Transaction{txn}))

	doc, err := s.Get("a")
	assert.NoError(err)
	assert.Equal("v2", string(doc.Fields["k"]))
}

func TestMemoryStorage_Scan(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 1)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	d1 := NewDocument("a", map[string][]byte{"k": []byte("1")})
	d2 := NewDocument("b", map[string][]byte{"k": []byte("2")})
	d3 := NewDocument("c", map[string][]byte{"k": []byte("3")})
	txn := NewTransaction(nil, []*Document{d1, d2, d3}, nil)
	sendBatch(ch, NewBatch(1, []*Transaction{txn}))

	docs, err := s.Scan()
	assert.NoError(err)
	assert.Len(docs, 3)

	ids := make(map[string]bool)
	for _, d := range docs {
		ids[d.Id] = true
	}
	assert.True(ids["a"])
	assert.True(ids["b"])
	assert.True(ids["c"])
}

func TestMemoryStorage_StartStop(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 1)
	assert.NoError(s.Start(ch))
	assert.NoError(s.Stop())
}
