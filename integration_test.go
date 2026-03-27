package bdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIntegrationStack creates a fully wired stack with auto-ticking log.
func newIntegrationStack(t *testing.T, epochDuration time.Duration) (*Coordinator, func()) {
	t.Helper()
	cfg := Config{EpochDuration: epochDuration}
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

func TestIntegration_EndToEnd(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newIntegrationStack(t, 5*time.Millisecond)
	defer cleanup()

	// Put several documents -- writes block until committed
	ts1, err := coord.Put("user1", map[string][]byte{"name": []byte("alice")})
	require.NoError(t, err)
	_, err = coord.Put("user2", map[string][]byte{"name": []byte("bob")})
	require.NoError(t, err)
	ts3, err := coord.Put("user3", map[string][]byte{"name": []byte("charlie")})
	require.NoError(t, err)

	// Read-your-own-writes: no sleep needed
	doc, err := coord.GetAt("user1", ts1)
	assert.NoError(err)
	assert.Equal("alice", string(doc.Fields["name"]))

	doc, err = coord.GetAt("user3", ts3)
	assert.NoError(err)
	assert.Equal("charlie", string(doc.Fields["name"]))
}

func TestIntegration_MultipleEpochs(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newIntegrationStack(t, 5*time.Millisecond)
	defer cleanup()

	// Write
	ts1, err := coord.Put("a", map[string][]byte{"v": []byte("1")})
	require.NoError(t, err)

	doc, err := coord.GetAt("a", ts1)
	assert.NoError(err)
	assert.Equal("1", string(doc.Fields["v"]))

	// Overwrite
	ts2, err := coord.Put("a", map[string][]byte{"v": []byte("2")})
	require.NoError(t, err)

	doc, err = coord.GetAt("a", ts2)
	assert.NoError(err)
	assert.Equal("2", string(doc.Fields["v"]))

	// MVCC: old version still readable at original timestamp
	doc, err = coord.GetAt("a", ts1)
	assert.NoError(err)
	assert.Equal("1", string(doc.Fields["v"]))

	// Delete
	ts3, err := coord.Delete("a")
	require.NoError(t, err)

	_, err = coord.GetAt("a", ts3)
	assert.ErrorIs(err, ErrNotFound)

	// MVCC: still readable at pre-delete timestamp
	doc, err = coord.GetAt("a", ts2)
	assert.NoError(err)
	assert.Equal("2", string(doc.Fields["v"]))
}

func TestIntegration_ConcurrentClients(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newIntegrationStack(t, 5*time.Millisecond)
	defer cleanup()

	n := 50
	timestamps := make([]Timestamp, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			id := RandomString(8)
			ts, _ := coord.Put(id, map[string][]byte{"i": []byte{byte(idx)}})
			mu.Lock()
			timestamps[idx] = ts
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Wait for the latest write to be applied
	var maxTS Timestamp
	for _, ts := range timestamps {
		if ts.After(maxTS) {
			maxTS = ts
		}
	}
	coord.storage.WaitForTimestamp(maxTS)

	docs, err := coord.Scan()
	assert.NoError(err)
	assert.Len(docs, n)
}

func TestIntegration_ReadYourOwnWrites(t *testing.T) {
	assert := assert.New(t)
	coord, cleanup := newIntegrationStack(t, 5*time.Millisecond)
	defer cleanup()

	// Write and immediately read at the commit timestamp -- no sleep
	ts, err := coord.Put("key", map[string][]byte{"val": []byte("hello")})
	require.NoError(t, err)

	doc, err := coord.GetAt("key", ts)
	assert.NoError(err)
	assert.Equal("hello", string(doc.Fields["val"]))
}
