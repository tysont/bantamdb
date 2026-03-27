// ABOUTME: Transport abstraction for Raft RPCs, with HTTP/JSON and in-memory
// ABOUTME: channel implementations for production and testing respectively.
package bdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Transport is the network layer for Raft RPCs. Abstracting this allows
// the Raft node to be tested with in-memory transports.
type Transport interface {
	SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// RPCHandler processes incoming Raft RPCs. RaftNode implements this.
type RPCHandler interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}

// ChannelTransport routes Raft RPCs directly to in-memory handlers for
// deterministic testing without networking.
type ChannelTransport struct {
	mu       sync.RWMutex
	handlers map[string]RPCHandler
}

// NewChannelTransport creates a new in-memory transport.
func NewChannelTransport() *ChannelTransport {
	return &ChannelTransport{
		handlers: make(map[string]RPCHandler),
	}
}

// Register adds a handler for the given node address.
func (t *ChannelTransport) Register(addr string, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[addr] = handler
}

// SendRequestVote delivers a RequestVote RPC to the target handler.
func (t *ChannelTransport) SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error) {
	t.mu.RLock()
	h, ok := t.handlers[target]
	t.mu.RUnlock()
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("node %s not reachable", target)
	}
	return h.HandleRequestVote(args), nil
}

// SendAppendEntries delivers an AppendEntries RPC to the target handler.
func (t *ChannelTransport) SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	t.mu.RLock()
	h, ok := t.handlers[target]
	t.mu.RUnlock()
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("node %s not reachable", target)
	}
	return h.HandleAppendEntries(args), nil
}

// Disconnect simulates a network partition by removing a handler.
func (t *ChannelTransport) Disconnect(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, addr)
}

// Reconnect restores a previously disconnected handler.
func (t *ChannelTransport) Reconnect(addr string, handler RPCHandler) {
	t.Register(addr, handler)
}

// HTTPTransport implements Transport using HTTP/JSON POST requests.
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport creates an HTTP transport with sensible defaults.
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// SendRequestVote sends a RequestVote RPC via HTTP POST.
func (t *HTTPTransport) SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := t.post(target, "/raft/request-vote", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC via HTTP POST.
func (t *HTTPTransport) SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := t.post(target, "/raft/append-entries", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (t *HTTPTransport) post(target, path string, body any, result any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := t.client.Post(target+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to %s%s: %w", target, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post to %s%s: status %d", target, path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

// RegisterRaftHandlers adds Raft RPC HTTP endpoints to the given mux.
func RegisterRaftHandlers(mux *http.ServeMux, handler RPCHandler) {
	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		reply := handler.HandleRequestVote(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		reply := handler.HandleAppendEntries(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
}
