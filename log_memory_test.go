package bdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// appendAsync starts Append in a goroutine and returns a channel for the result.
func appendAsync(l *MemoryLog, txn *Transaction) <-chan Timestamp {
	ch := make(chan Timestamp, 1)
	go func() {
		ts, _ := l.Append(txn)
		ch <- ts
	}()
	return ch
}

func TestMemoryLog_AppendAndTick(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	txn := NewTransaction(nil, []*Document{d}, nil)
	result := appendAsync(l, txn)

	// Give goroutine time to register
	time.Sleep(time.Millisecond)
	l.Tick()

	// Append should unblock with the batch timestamp
	ts := <-result
	assert.False(ts.IsZero(), "commit timestamp should not be zero")

	// Batch should appear on subscriber channel
	select {
	case batch := <-ch:
		require.NotNil(t, batch)
		assert.Equal(ts, batch.Timestamp)
		assert.Len(batch.Transactions, 1)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch")
	}
}

func TestMemoryLog_TickEmpty(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	l.Tick()

	select {
	case batch := <-ch:
		require.NotNil(t, batch)
		assert.False(batch.Timestamp.IsZero())
		assert.Empty(batch.Transactions)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch")
	}
}

func TestMemoryLog_MultipleBatches(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	// Batch 1: one transaction
	d1 := NewDocument("a", map[string][]byte{"k": []byte("v1")})
	r1 := appendAsync(l, NewTransaction(nil, []*Document{d1}, nil))
	time.Sleep(time.Millisecond)
	l.Tick()
	ts1 := <-r1

	// Batch 2: two transactions
	d2 := NewDocument("b", map[string][]byte{"k": []byte("v2")})
	d3 := NewDocument("c", map[string][]byte{"k": []byte("v3")})
	r2 := appendAsync(l, NewTransaction(nil, []*Document{d2}, nil))
	r3 := appendAsync(l, NewTransaction(nil, []*Document{d3}, nil))
	time.Sleep(time.Millisecond)
	l.Tick()
	ts2 := <-r2
	ts3 := <-r3

	batch1 := <-ch
	require.NotNil(t, batch1)
	assert.Len(batch1.Transactions, 1)

	batch2 := <-ch
	require.NotNil(t, batch2)
	assert.Len(batch2.Transactions, 2)
	assert.True(batch2.Timestamp.After(batch1.Timestamp))

	// All transactions in the same batch get the same timestamp
	assert.Equal(ts1, batch1.Timestamp)
	assert.Equal(ts2, ts3)
	assert.Equal(ts2, batch2.Timestamp)
}

func TestMemoryLog_ConcurrentAppend(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	n := 100
	results := make([]chan Timestamp, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		results[i] = make(chan Timestamp, 1)
		go func(idx int) {
			defer wg.Done()
			d := NewDocument("x", map[string][]byte{"k": []byte("v")})
			ts, _ := l.Append(NewTransaction(nil, []*Document{d}, nil))
			results[idx] <- ts
		}(i)
	}

	// Wait for all goroutines to have called Append
	time.Sleep(10 * time.Millisecond)
	l.Tick()
	wg.Wait()

	batch := <-ch
	require.NotNil(t, batch)
	assert.Len(batch.Transactions, n)

	// All results should have the same timestamp
	for i := range n {
		ts := <-results[i]
		assert.Equal(batch.Timestamp, ts)
	}
}

func TestMemoryLog_StartStop(t *testing.T) {
	assert := assert.New(t)
	cfg := DefaultConfig()
	cfg.EpochDuration = 5 * time.Millisecond
	l := NewMemoryLog(cfg)
	ch := l.Subscribe()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	go l.Append(NewTransaction(nil, []*Document{d}, nil))

	assert.NoError(l.Start())

	select {
	case batch := <-ch:
		require.NotNil(t, batch)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for automatic batch")
	}

	assert.NoError(l.Stop())

	_, open := <-ch
	assert.False(open, "subscriber channel should be closed after Stop")
}

func TestMemoryLog_AppendAfterStop(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	_ = l.Subscribe()
	assert.NoError(l.Start())
	assert.NoError(l.Stop())

	_, err := l.Append(NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil))
	assert.ErrorIs(err, ErrStopped)
}
