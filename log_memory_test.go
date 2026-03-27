package bdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLog_AppendAndTick(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	txn := NewTransaction(nil, []*Document{d}, nil)
	assert.NoError(l.Append(txn))

	l.Tick()

	select {
	case batch := <-ch:
		require.NotNil(t, batch)
		assert.False(batch.Timestamp.IsZero(), "batch timestamp should not be zero")
		assert.Len(batch.Transactions, 1)
		assert.Equal("a", batch.Transactions[0].Writes[0].Id)
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
	assert.NoError(l.Append(NewTransaction(nil, []*Document{d1}, nil)))
	l.Tick()

	// Batch 2: two transactions
	d2 := NewDocument("b", map[string][]byte{"k": []byte("v2")})
	d3 := NewDocument("c", map[string][]byte{"k": []byte("v3")})
	assert.NoError(l.Append(NewTransaction(nil, []*Document{d2}, nil)))
	assert.NoError(l.Append(NewTransaction(nil, []*Document{d3}, nil)))
	l.Tick()

	batch1 := <-ch
	require.NotNil(t, batch1)
	assert.Len(batch1.Transactions, 1)

	batch2 := <-ch
	require.NotNil(t, batch2)
	assert.Len(batch2.Transactions, 2)
	assert.True(batch2.Timestamp.After(batch1.Timestamp), "batch timestamps must be monotonically increasing")
}

func TestMemoryLog_ConcurrentAppend(t *testing.T) {
	assert := assert.New(t)
	l := NewMemoryLog(DefaultConfig())
	ch := l.Subscribe()

	n := 100
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			d := NewDocument("x", map[string][]byte{"k": []byte("v")})
			l.Append(NewTransaction(nil, []*Document{d}, nil))
		}()
	}
	wg.Wait()

	l.Tick()

	batch := <-ch
	require.NotNil(t, batch)
	assert.Len(batch.Transactions, n)
}

func TestMemoryLog_StartStop(t *testing.T) {
	assert := assert.New(t)
	cfg := DefaultConfig()
	cfg.EpochDuration = 5 * time.Millisecond
	l := NewMemoryLog(cfg)
	ch := l.Subscribe()

	d := NewDocument("a", map[string][]byte{"k": []byte("v")})
	assert.NoError(l.Append(NewTransaction(nil, []*Document{d}, nil)))

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

	err := l.Append(NewTransaction(nil, []*Document{NewDocument("a", nil)}, nil))
	assert.ErrorIs(err, ErrStopped)
}
