package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStack creates a fully wired coordinator with auto-ticking log.
func newTestStack(t *testing.T) (*Coordinator, func()) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.EpochDuration = 5 * time.Millisecond
	log := NewMemoryLog(cfg)
	storage := NewMemoryStorage()
	storage.Start(log.Subscribe())
	coord := NewCoordinator(log, storage)
	log.Start()
	cleanup := func() {
		log.Stop()
		storage.Stop()
	}
	return coord, cleanup
}

func TestCoordinator_Put(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newTestStack(t)
	defer cleanup()

	ts, err := coord.Put("a", map[string][]byte{"k": []byte("v")})
	require.NoError(t, err)
	assert.False(ts.IsZero())

	doc, err := coord.GetAt("a", ts)
	assert.NoError(err)
	assert.Equal("v", string(doc.Fields["k"]))
}

func TestCoordinator_Delete(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newTestStack(t)
	defer cleanup()

	putTs, err := coord.Put("a", map[string][]byte{"k": []byte("v")})
	require.NoError(t, err)

	doc, err := coord.GetAt("a", putTs)
	assert.NoError(err)
	assert.NotNil(doc)

	delTs, err := coord.Delete("a")
	require.NoError(t, err)

	_, err = coord.GetAt("a", delTs)
	assert.ErrorIs(err, ErrNotFound)
}

func TestCoordinator_Transact(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newTestStack(t)
	defer cleanup()

	d1 := NewDocument("a", map[string][]byte{"k": []byte("1")})
	d2 := NewDocument("b", map[string][]byte{"k": []byte("2")})
	txn := NewTransaction(nil, []*Document{d1, d2}, nil)
	ts, err := coord.Transact(txn)
	require.NoError(t, err)

	doc1, err := coord.GetAt("a", ts)
	assert.NoError(err)
	assert.Equal("1", string(doc1.Fields["k"]))

	doc2, err := coord.GetAt("b", ts)
	assert.NoError(err)
	assert.Equal("2", string(doc2.Fields["k"]))
}

func TestCoordinator_Get(t *testing.T) {
	coord, cleanup := newTestStack(t)
	defer cleanup()

	_, err := coord.Get("nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestCoordinator_Scan(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newTestStack(t)
	defer cleanup()

	_, err := coord.Put("a", map[string][]byte{"k": []byte("1")})
	require.NoError(t, err)
	ts2, err := coord.Put("b", map[string][]byte{"k": []byte("2")})
	require.NoError(t, err)

	// Wait for storage to catch up to the latest write
	coord.storage.WaitForTimestamp(ts2)

	docs, err := coord.Scan()
	assert.NoError(err)
	assert.Len(docs, 2)
}

func TestCoordinator_ReadYourOwnWrites(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newTestStack(t)
	defer cleanup()

	// Write returns a timestamp
	ts, err := coord.Put("key", map[string][]byte{"val": []byte("hello")})
	require.NoError(t, err)

	// Immediately read at that timestamp -- this blocks until storage catches up
	doc, err := coord.GetAt("key", ts)
	assert.NoError(err)
	assert.Equal("hello", string(doc.Fields["val"]))
}
