package bdb

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestServer() (*httptest.Server, *MemoryLog, func()) {
	cfg := DefaultConfig()
	log := NewMemoryLog(cfg)
	storage := NewMemoryStorage()
	ch := log.Subscribe()
	storage.Start(ch)
	coord := NewCoordinator(log, storage)
	handler := NewHandler(coord)
	server := httptest.NewServer(handler)
	cleanup := func() {
		server.Close()
		log.Stop()
		storage.Stop()
	}
	return server, log, cleanup
}

func postDocument(t *testing.T, url string, id string, fields map[string]string) *http.Response {
	t.Helper()
	byteFields := make(map[string][]byte)
	for k, v := range fields {
		byteFields[k] = []byte(v)
	}
	body := struct {
		Id     string            `json:"id"`
		Fields map[string][]byte `json:"fields"`
	}{Id: id, Fields: byteFields}
	b, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url+"/documents", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	return resp
}

func TestHTTP_PutAndGet(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := newTestServer()
	defer cleanup()

	resp := postDocument(t, server.URL, "user1", map[string]string{"name": "alice"})
	assert.Equal(http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	log.Tick()
	time.Sleep(5 * time.Millisecond)

	resp, err := http.Get(server.URL + "/documents/user1")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(http.StatusOK, resp.StatusCode)

	var doc Document
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	assert.Equal("user1", doc.Id)
	assert.Equal("alice", string(doc.Fields["name"]))
}

func TestHTTP_GetNotFound(t *testing.T) {
	server, _, cleanup := newTestServer()
	defer cleanup()

	resp, err := http.Get(server.URL + "/documents/nonexistent")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHTTP_Delete(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := newTestServer()
	defer cleanup()

	postDocument(t, server.URL, "user1", map[string]string{"name": "alice"})
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/documents/user1", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(http.StatusAccepted, resp.StatusCode)

	log.Tick()
	time.Sleep(5 * time.Millisecond)

	resp, err = http.Get(server.URL + "/documents/user1")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(http.StatusNotFound, resp.StatusCode)
}

func TestHTTP_Scan(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := newTestServer()
	defer cleanup()

	postDocument(t, server.URL, "a", map[string]string{"k": "1"})
	postDocument(t, server.URL, "b", map[string]string{"k": "2"})
	postDocument(t, server.URL, "c", map[string]string{"k": "3"})
	log.Tick()
	time.Sleep(5 * time.Millisecond)

	resp, err := http.Get(server.URL + "/documents")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(http.StatusOK, resp.StatusCode)

	var docs []Document
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&docs))
	assert.Len(docs, 3)
}

func TestHTTP_Transaction(t *testing.T) {
	assert := assert.New(t)
	server, log, cleanup := newTestServer()
	defer cleanup()

	txn := struct {
		ReadSet []string    `json:"readSet"`
		Writes  []*Document `json:"writes"`
		Deletes []string    `json:"deletes"`
	}{
		Writes: []*Document{
			NewDocument("a", map[string][]byte{"k": []byte("1")}),
			NewDocument("b", map[string][]byte{"k": []byte("2")}),
		},
	}
	b, _ := json.Marshal(txn)
	resp, err := http.Post(server.URL+"/transactions", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(http.StatusAccepted, resp.StatusCode)

	log.Tick()
	time.Sleep(5 * time.Millisecond)

	resp, err = http.Get(server.URL + "/documents/a")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func TestHTTP_BadRequest(t *testing.T) {
	server, _, cleanup := newTestServer()
	defer cleanup()

	resp, err := http.Post(server.URL+"/documents", "application/json", bytes.NewReader([]byte("not json")))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
