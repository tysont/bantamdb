package bdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStack creates a fully wired coordinator+log+storage for testing.
// It returns the coordinator and a cleanup function. Use log.Tick() to
// manually advance epochs.
func newTestStack() (*Coordinator, *MemoryLog, func()) {
	cfg := DefaultConfig()
	log := NewMemoryLog(cfg)
	storage := NewMemoryStorage()
	ch := log.Subscribe()
	storage.Start(ch)
	coord := NewCoordinator(log, storage)
	cleanup := func() {
		log.Stop()
		storage.Stop()
	}
	return coord, log, cleanup
}

func TestCoordinator_Put(t *testing.T) {
	assert := assert.New(t)
	coord, log, cleanup := newTestStack()
	defer cleanup()

	assert.NoError(coord.Put("a", map[string][]byte{"k": []byte("v")}))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	doc, err := coord.Get("a")
	assert.NoError(err)
	assert.Equal("v", string(doc.Fields["k"]))
}

func TestCoordinator_Delete(t *testing.T) {
	assert := assert.New(t)
	coord, log, cleanup := newTestStack()
	defer cleanup()

	assert.NoError(coord.Put("a", map[string][]byte{"k": []byte("v")}))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	assert.NoError(coord.Delete("a"))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	_, err := coord.Get("a")
	assert.ErrorIs(err, ErrNotFound)
}

func TestCoordinator_Transact(t *testing.T) {
	assert := assert.New(t)
	coord, log, cleanup := newTestStack()
	defer cleanup()

	d1 := NewDocument("a", map[string][]byte{"k": []byte("1")})
	d2 := NewDocument("b", map[string][]byte{"k": []byte("2")})
	txn := NewTransaction(nil, []*Document{d1, d2}, nil)
	assert.NoError(coord.Transact(txn))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	doc1, err := coord.Get("a")
	assert.NoError(err)
	assert.Equal("1", string(doc1.Fields["k"]))

	doc2, err := coord.Get("b")
	assert.NoError(err)
	assert.Equal("2", string(doc2.Fields["k"]))
}

func TestCoordinator_Get(t *testing.T) {
	coord, _, cleanup := newTestStack()
	defer cleanup()

	_, err := coord.Get("nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestCoordinator_Scan(t *testing.T) {
	assert := assert.New(t)
	coord, log, cleanup := newTestStack()
	defer cleanup()

	require.NoError(t, coord.Put("a", map[string][]byte{"k": []byte("1")}))
	require.NoError(t, coord.Put("b", map[string][]byte{"k": []byte("2")}))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	docs, err := coord.Scan()
	assert.NoError(err)
	assert.Len(docs, 2)
}
