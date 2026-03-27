// ABOUTME: RaftNode implements the Raft consensus protocol with leader election,
// ABOUTME: log replication, and quorum-based commitment.
package bdb

import (
	"math/rand/v2"
	"sync"
	"time"
)

// RaftNode is a single participant in a Raft cluster. It manages leader
// election, log replication, and notifies committed entries via ApplyCh.
type RaftNode struct {
	mu        sync.Mutex
	id        string
	peers     []string
	role      NodeRole
	state     *RaftState
	transport Transport

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timing
	electionTimer *time.Timer
	heartbeatDone chan struct{}

	// Committed entries are sent here for application
	applyCh     chan LogEntry
	lastApplied uint64

	stopCh  chan struct{}
	stopped bool
}

// NewRaftNode creates a new Raft node. It does not start until Start is called.
func NewRaftNode(id string, peers []string, transport Transport) *RaftNode {
	return &RaftNode{
		id:        id,
		peers:     peers,
		role:      Follower,
		state:     NewRaftState(),
		transport: transport,
		applyCh:   make(chan LogEntry, 256),
	}
}

// ID returns the node's identifier.
func (n *RaftNode) ID() string {
	return n.id
}

// Role returns the node's current Raft role.
func (n *RaftNode) Role() NodeRole {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// ApplyCh returns the channel that receives committed log entries.
func (n *RaftNode) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

// LeaderID returns the ID of the current leader, or empty string if unknown.
func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return ""
}

// Start begins the Raft node's election timer and participation.
func (n *RaftNode) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.stopCh = make(chan struct{})
	n.resetElectionTimerLocked()
}

// Stop halts the Raft node.
func (n *RaftNode) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.stopped = true
	if n.stopCh != nil {
		close(n.stopCh)
	}
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatDone != nil {
		close(n.heartbeatDone)
	}
}

// Propose submits data to be replicated. Only works on the leader.
// Returns the log index of the proposed entry.
func (n *RaftNode) Propose(data []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := n.state.CurrentTerm()
	index := n.state.LastIndex() + 1
	entry := LogEntry{Index: index, Term: term, Data: data}
	n.state.Append([]LogEntry{entry})
	n.matchIndex[n.id] = index
	// Check if this entry can be committed immediately (e.g., single-node cluster)
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	// Replicate to followers
	n.replicateToAll()
	return index, nil
}

// HandleRequestVote processes a RequestVote RPC.
func (n *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.state.CurrentTerm()

	// Reply false if term < currentTerm
	if args.Term < currentTerm {
		return RequestVoteReply{Term: currentTerm, VoteGranted: false}
	}

	// If RPC term is higher, step down
	if args.Term > currentTerm {
		n.becomeFollowerLocked(args.Term)
		currentTerm = args.Term
	}

	votedFor := n.state.VotedFor()
	canVote := votedFor == "" || votedFor == args.CandidateID

	// Check candidate's log is at least as up-to-date
	lastTerm := n.state.LastTerm()
	lastIndex := n.state.LastIndex()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if canVote && logOK {
		n.state.SetVotedFor(args.CandidateID)
		n.resetElectionTimerLocked()
		return RequestVoteReply{Term: currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: currentTerm, VoteGranted: false}
}

// HandleAppendEntries processes an AppendEntries RPC.
func (n *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.state.CurrentTerm()

	// Reply false if term < currentTerm
	if args.Term < currentTerm {
		return AppendEntriesReply{Term: currentTerm, Success: false}
	}

	// Valid leader -- reset election timer
	if args.Term >= currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.resetElectionTimerLocked()

	// Check log consistency at PrevLogIndex
	if args.PrevLogIndex > 0 {
		prevTerm, ok := n.state.TermAt(args.PrevLogIndex)
		if !ok || prevTerm != args.PrevLogTerm {
			return AppendEntriesReply{Term: n.state.CurrentTerm(), Success: false}
		}
	}

	// Append new entries, truncating conflicts
	for _, entry := range args.Entries {
		existingTerm, exists := n.state.TermAt(entry.Index)
		if exists && existingTerm != entry.Term {
			n.state.TruncateFrom(entry.Index)
			exists = false
		}
		if !exists {
			n.state.Append([]LogEntry{entry})
		}
	}

	// Advance commit index
	if args.LeaderCommit > n.state.CommitIndex() {
		newCommit := args.LeaderCommit
		lastIndex := n.state.LastIndex()
		if newCommit > lastIndex {
			newCommit = lastIndex
		}
		n.state.SetCommitIndex(newCommit)
		n.applyCommittedLocked()
	}

	return AppendEntriesReply{Term: n.state.CurrentTerm(), Success: true}
}

// --- Internal methods ---

func (n *RaftNode) resetElectionTimerLocked() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := 300*time.Millisecond + time.Duration(rand.IntN(300))*time.Millisecond
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.startElection()
	})
}

func (n *RaftNode) becomeFollowerLocked(term uint64) {
	n.role = Follower
	n.state.SetTerm(term)
	if n.heartbeatDone != nil {
		close(n.heartbeatDone)
		n.heartbeatDone = nil
	}
}

func (n *RaftNode) startElection() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	newTerm := n.state.CurrentTerm() + 1
	n.state.SetTerm(newTerm)
	n.state.SetVotedFor(n.id)
	n.resetElectionTimerLocked()

	lastIndex := n.state.LastIndex()
	lastTerm := n.state.LastTerm()
	peers := n.peers
	n.mu.Unlock()

	// Vote for self
	votes := 1
	total := len(peers) + 1

	// If single node, win immediately
	if total == 1 {
		n.mu.Lock()
		n.becomeLeaderLocked()
		n.mu.Unlock()
		return
	}

	// Request votes from peers in parallel
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, peer := range peers {
		go func(target string) {
			defer wg.Done()
			reply, err := n.transport.SendRequestVote(target, RequestVoteArgs{
				Term:         newTerm,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.state.CurrentTerm() {
				n.becomeFollowerLocked(reply.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				mu.Lock()
				votes++
				won := votes > total/2
				mu.Unlock()
				if won {
					n.mu.Lock()
					if n.role == Candidate && n.state.CurrentTerm() == newTerm {
						n.becomeLeaderLocked()
					}
					n.mu.Unlock()
				}
			}
		}(peer)
	}
}

func (n *RaftNode) becomeLeaderLocked() {
	n.role = Leader
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Initialize leader state
	nextIdx := n.state.LastIndex() + 1
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = n.state.LastIndex()

	// Start heartbeat loop
	n.heartbeatDone = make(chan struct{})
	go n.heartbeatLoop()
}

func (n *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	n.replicateToAll()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if !isLeader {
				return
			}
			n.replicateToAll()
		case <-n.heartbeatDone:
			return
		}
	}
}

func (n *RaftNode) replicateToAll() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
}

func (n *RaftNode) replicateToPeer(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		if t, ok := n.state.TermAt(prevLogIndex); ok {
			prevLogTerm = t
		}
	}
	entries := n.state.EntriesFrom(nextIdx)
	term := n.state.CurrentTerm()
	commitIndex := n.state.CommitIndex()
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peer, AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.state.CurrentTerm() {
		n.becomeFollowerLocked(reply.Term)
		return
	}

	if n.role != Leader {
		return
	}

	if reply.Success {
		if len(entries) > 0 {
			lastEntry := entries[len(entries)-1]
			n.nextIndex[peer] = lastEntry.Index + 1
			n.matchIndex[peer] = lastEntry.Index
			n.advanceCommitIndexLocked()
		}
	} else {
		// Decrement nextIndex and retry on next heartbeat
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *RaftNode) advanceCommitIndexLocked() {
	// Find the highest N such that a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm
	currentTerm := n.state.CurrentTerm()
	for idx := n.state.LastIndex(); idx > n.state.CommitIndex(); idx-- {
		term, ok := n.state.TermAt(idx)
		if !ok || term != currentTerm {
			continue
		}
		replicatedCount := 1 // count self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicatedCount++
			}
		}
		total := len(n.peers) + 1
		if replicatedCount > total/2 {
			n.state.SetCommitIndex(idx)
			n.applyCommittedLocked()
			return
		}
	}
}

func (n *RaftNode) applyCommittedLocked() {
	commitIndex := n.state.CommitIndex()
	for n.lastApplied < commitIndex {
		n.lastApplied++
		entry, ok := n.state.Get(n.lastApplied)
		if !ok {
			continue
		}
		select {
		case n.applyCh <- entry:
		default:
			// Channel full; drop entry (shouldn't happen with buffered channel)
		}
	}
}
