package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ts creates a simple Timestamp for testing with distinct wall times.
func ts(n int64) Timestamp {
	return Timestamp{WallTime: n, Logical: 0}
}

// sendBatch is a test helper that sends a batch on a channel and waits
// briefly for storage to process it.
func sendBatch(ch chan<- *Batch, batch *Batch) {
	ch <- batch
	time.Sleep(5 * time.Millisecond)
}

func TestMemoryStorage_GetMissing(t *testing.T) {
	s := NewMemoryStorage()
	_, err := s.Get("nonexistent", TimestampMax())
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
	sendBatch(ch, NewBatch(ts(1), []*Transaction{txn}))

	doc, err := s.Get("a", TimestampMax())
	assert.NoError(err)
	assert.Equal("v", string(doc.Fields["k"]))
}

func TestMemoryStorage_PointInTimeRead(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write v1 at timestamp 1
	d1 := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(ts(1), []*Transaction{NewTransaction(nil, []*Document{d1}, nil)}))

	// Write v2 at timestamp 2
	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	sendBatch(ch, NewBatch(ts(2), []*Transaction{NewTransaction(nil, []*Document{d2}, nil)}))

	// Read at timestamp 1 returns v1
	doc, err := s.Get("a", ts(1))
	assert.NoError(err)
	assert.Equal("v1", string(doc.Fields["k"]))

	// Read at timestamp 2 returns v2
	doc, err = s.Get("a", ts(2))
	assert.NoError(err)
	assert.Equal("v2", string(doc.Fields["k"]))

	// Read at timestamp 3 (after all writes) returns v2
	doc, err = s.Get("a", ts(3))
	assert.NoError(err)
	assert.Equal("v2", string(doc.Fields["k"]))

	// Read at timestamp 0 (before all writes) returns not found
	_, err = s.Get("a", ts(0))
	assert.ErrorIs(err, ErrNotFound)
}

func TestMemoryStorage_ApplyDelete(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	sendBatch(ch, NewBatch(ts(1), []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	// Read at latest - exists
	doc, err := s.Get("a", TimestampMax())
	assert.NoError(err)
	assert.NotNil(doc)

	// Delete at timestamp 2
	sendBatch(ch, NewBatch(ts(2), []*Transaction{NewTransaction(nil, nil, []string{"a"})}))

	// Read at latest - gone
	_, err = s.Get("a", TimestampMax())
	assert.ErrorIs(err, ErrNotFound)

	// Read at timestamp 1 - still visible (MVCC)
	doc, err = s.Get("a", ts(1))
	assert.NoError(err)
	assert.Equal("v", string(doc.Fields["k"]))
}

func TestMemoryStorage_ApplyOCCConflict(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write "a" at timestamp 2
	d := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(ts(2), []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	// Try to read "a" in a transaction at timestamp 1 (stale read)
	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	txn := NewTransaction([]string{"a"}, []*Document{d2}, nil)
	sendBatch(ch, NewBatch(ts(1), []*Transaction{txn}))

	// The conflicting transaction's write should be rejected
	doc, err := s.Get("a", TimestampMax())
	assert.NoError(err)
	assert.Equal("v1", string(doc.Fields["k"]))
}

func TestMemoryStorage_ApplyOCCSuccess(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write "a" at timestamp 1
	d := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	sendBatch(ch, NewBatch(ts(1), []*Transaction{NewTransaction(nil, []*Document{d}, nil)}))

	// Read "a" and write it at timestamp 2 (valid)
	d2 := NewDocument("a", map[string][]byte{"k": []byte("v2")})
	txn := NewTransaction([]string{"a"}, []*Document{d2}, nil)
	sendBatch(ch, NewBatch(ts(2), []*Transaction{txn}))

	doc, err := s.Get("a", TimestampMax())
	assert.NoError(err)
	assert.Equal("v2", string(doc.Fields["k"]))
}

func TestMemoryStorage_ScanAtTimestamp(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// Write a, b at timestamp 1
	d1 := NewDocument("a", map[string][]byte{"k": []byte("1")})
	d2 := NewDocument("b", map[string][]byte{"k": []byte("2")})
	sendBatch(ch, NewBatch(ts(1), []*Transaction{NewTransaction(nil, []*Document{d1, d2}, nil)}))

	// Write c, delete a at timestamp 2
	d3 := NewDocument("c", map[string][]byte{"k": []byte("3")})
	sendBatch(ch, NewBatch(ts(2), []*Transaction{
		NewTransaction(nil, []*Document{d3}, []string{"a"}),
	}))

	// Scan at timestamp 1: should see a, b
	docs, err := s.Scan(ts(1))
	assert.NoError(err)
	assert.Len(docs, 2)

	// Scan at timestamp 2: should see b, c (a was deleted)
	docs, err = s.Scan(ts(2))
	assert.NoError(err)
	assert.Len(docs, 2)
	ids := map[string]bool{}
	for _, d := range docs {
		ids[d.Id] = true
	}
	assert.True(ids["b"])
	assert.True(ids["c"])
}

func TestMemoryStorage_AppliedTimestamp(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 2)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	assert.True(s.AppliedTimestamp().IsZero(), "should start at zero")

	sendBatch(ch, NewBatch(ts(1), nil))
	assert.Equal(ts(1), s.AppliedTimestamp())

	sendBatch(ch, NewBatch(ts(5), nil))
	assert.Equal(ts(5), s.AppliedTimestamp())
}

func TestMemoryStorage_WaitForTimestamp(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 1)
	require.NoError(t, s.Start(ch))
	defer s.Stop()

	// WaitForTimestamp on already-reached timestamp returns immediately
	sendBatch(ch, NewBatch(ts(5), nil))
	assert.NoError(s.WaitForTimestamp(ts(3)))

	// WaitForTimestamp blocks until a future batch arrives
	done := make(chan error, 1)
	go func() {
		done <- s.WaitForTimestamp(ts(10))
	}()

	// Should not have returned yet
	select {
	case <-done:
		t.Fatal("WaitForTimestamp should still be blocking")
	case <-time.After(10 * time.Millisecond):
	}

	// Send a batch that advances past the waited timestamp
	sendBatch(ch, NewBatch(ts(10), nil))

	select {
	case err := <-done:
		assert.NoError(err)
	case <-time.After(time.Second):
		t.Fatal("WaitForTimestamp should have returned")
	}
}

func TestMemoryStorage_StartStop(t *testing.T) {
	assert := assert.New(t)
	s := NewMemoryStorage()
	ch := make(chan *Batch, 1)
	assert.NoError(s.Start(ch))
	assert.NoError(s.Stop())
}
