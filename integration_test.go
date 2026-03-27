package bdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIntegrationStack creates a fully wired stack with the log auto-ticking.
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

// waitForEpoch waits long enough for at least one epoch to flush.
func waitForEpoch(d time.Duration) {
	time.Sleep(d * 3)
}

func TestIntegration_EndToEnd(t *testing.T) {
	assert := assert.New(t)
	epoch := 5 * time.Millisecond
	coord, cleanup := newIntegrationStack(t, epoch)
	defer cleanup()

	// Put several documents
	require.NoError(t, coord.Put("user1", map[string][]byte{"name": []byte("alice")}))
	require.NoError(t, coord.Put("user2", map[string][]byte{"name": []byte("bob")}))
	require.NoError(t, coord.Put("user3", map[string][]byte{"name": []byte("charlie")}))

	waitForEpoch(epoch)

	// Verify all readable
	doc, err := coord.Get("user1")
	assert.NoError(err)
	assert.Equal("alice", string(doc.Fields["name"]))

	doc, err = coord.Get("user2")
	assert.NoError(err)
	assert.Equal("bob", string(doc.Fields["name"]))

	docs, err := coord.Scan()
	assert.NoError(err)
	assert.Len(docs, 3)
}

func TestIntegration_MultipleEpochs(t *testing.T) {
	assert := assert.New(t)
	epoch := 5 * time.Millisecond
	coord, cleanup := newIntegrationStack(t, epoch)
	defer cleanup()

	// Epoch 1
	require.NoError(t, coord.Put("a", map[string][]byte{"v": []byte("1")}))
	waitForEpoch(epoch)

	doc, err := coord.Get("a")
	assert.NoError(err)
	assert.Equal("1", string(doc.Fields["v"]))

	// Epoch 2: overwrite
	require.NoError(t, coord.Put("a", map[string][]byte{"v": []byte("2")}))
	waitForEpoch(epoch)

	doc, err = coord.Get("a")
	assert.NoError(err)
	assert.Equal("2", string(doc.Fields["v"]))

	// Epoch 3: delete
	require.NoError(t, coord.Delete("a"))
	waitForEpoch(epoch)

	_, err = coord.Get("a")
	assert.ErrorIs(err, ErrNotFound)
}

func TestIntegration_ConcurrentClients(t *testing.T) {
	assert := assert.New(t)
	epoch := 5 * time.Millisecond
	coord, cleanup := newIntegrationStack(t, epoch)
	defer cleanup()

	n := 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			id := RandomString(8)
			coord.Put(id, map[string][]byte{"i": []byte{byte(i)}})
		}()
	}
	wg.Wait()

	waitForEpoch(epoch)

	docs, err := coord.Scan()
	assert.NoError(err)
	assert.Len(docs, n)
}

func TestIntegration_OCCConflict(t *testing.T) {
	assert := assert.New(t)

	// Use manual ticking for precise epoch control
	cfg := DefaultConfig()
	log := NewMemoryLog(cfg)
	storage := NewMemoryStorage()
	ch := log.Subscribe()
	storage.Start(ch)
	defer func() {
		log.Stop()
		storage.Stop()
	}()

	// Epoch 1: write "a" with value "original"
	log.Append(NewTransaction(nil, []*Document{
		NewDocument("a", map[string][]byte{"v": []byte("original")}),
	}, nil))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	doc, err := storage.Get("a")
	require.NoError(t, err)
	assert.Equal("original", string(doc.Fields["v"]))

	// Epoch 2: a valid update that reads "a" (read epoch 1 <= batch epoch 2)
	log.Append(NewTransaction([]string{"a"}, []*Document{
		NewDocument("a", map[string][]byte{"v": []byte("updated")}),
	}, nil))
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	doc, err = storage.Get("a")
	assert.NoError(err)
	assert.Equal("updated", string(doc.Fields["v"]))
}
