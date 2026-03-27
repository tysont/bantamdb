// ABOUTME: Raft persistent state including term, vote, and log entries.
// ABOUTME: All operations are mutex-protected for concurrent access.
package bdb

import "sync"

// NodeRole represents the current role of a Raft node.
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

// LogEntry is a single entry in the Raft log. For BantamDB, Data
// contains a serialized Batch (via Batch.Marshal).
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
}

// RaftState holds the persistent state for a Raft node. All fields
// are protected by a mutex for concurrent access.
type RaftState struct {
	mu          sync.RWMutex
	currentTerm uint64
	votedFor    string
	log         []LogEntry
	commitIndex uint64
}

// NewRaftState creates a new empty Raft state.
func NewRaftState() *RaftState {
	return &RaftState{}
}

// CurrentTerm returns the current term.
func (s *RaftState) CurrentTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

// SetTerm sets the current term and clears the voted-for field.
func (s *RaftState) SetTerm(term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = ""
}

// VotedFor returns the node ID this node voted for in the current term.
func (s *RaftState) VotedFor() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

// SetVotedFor records which node this node voted for in the current term.
func (s *RaftState) SetVotedFor(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = id
}

// LastIndex returns the index of the last log entry, or 0 if empty.
func (s *RaftState) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Index
}

// LastTerm returns the term of the last log entry, or 0 if empty.
func (s *RaftState) LastTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Term
}

// Get returns the log entry at the given 1-based index.
func (s *RaftState) Get(index uint64) (LogEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return LogEntry{}, false
	}
	return s.log[index-1], true
}

// TermAt returns the term of the entry at the given index.
func (s *RaftState) TermAt(index uint64) (uint64, bool) {
	e, ok := s.Get(index)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

// Append adds entries to the end of the log.
func (s *RaftState) Append(entries []LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
}

// TruncateFrom removes all entries at and after the given index.
func (s *RaftState) TruncateFrom(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= uint64(len(s.log)) {
		s.log = s.log[:index-1]
	}
}

// EntriesFrom returns all entries starting at the given 1-based index.
func (s *RaftState) EntriesFrom(index uint64) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return nil
	}
	result := make([]LogEntry, len(s.log)-(int(index)-1))
	copy(result, s.log[index-1:])
	return result
}

// CommitIndex returns the index of the highest committed entry.
func (s *RaftState) CommitIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commitIndex
}

// SetCommitIndex sets the commit index.
func (s *RaftState) SetCommitIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = index
}
