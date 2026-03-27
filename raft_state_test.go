package bdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRaftState_InitialState(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	assert.Equal(uint64(0), s.CurrentTerm())
	assert.Equal("", s.VotedFor())
	assert.Equal(uint64(0), s.LastIndex())
	assert.Equal(uint64(0), s.LastTerm())
	assert.Equal(uint64(0), s.CommitIndex())
}

func TestRaftState_TermAndVote(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.SetTerm(3)
	assert.Equal(uint64(3), s.CurrentTerm())

	s.SetVotedFor("node-a")
	assert.Equal("node-a", s.VotedFor())

	// New term clears vote
	s.SetTerm(4)
	assert.Equal("", s.VotedFor())
}

func TestRaftState_AppendAndGet(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.Append([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	})

	assert.Equal(uint64(2), s.LastIndex())
	assert.Equal(uint64(1), s.LastTerm())

	e, ok := s.Get(1)
	assert.True(ok)
	assert.Equal([]byte("a"), e.Data)

	e, ok = s.Get(2)
	assert.True(ok)
	assert.Equal([]byte("b"), e.Data)

	_, ok = s.Get(3)
	assert.False(ok)

	_, ok = s.Get(0)
	assert.False(ok)
}

func TestRaftState_TruncateFrom(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.Append([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	})

	s.TruncateFrom(2)
	assert.Equal(uint64(1), s.LastIndex())

	_, ok := s.Get(2)
	assert.False(ok)
}

func TestRaftState_EntriesFrom(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.Append([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	})

	entries := s.EntriesFrom(2)
	assert.Len(entries, 2)
	assert.Equal(uint64(2), entries[0].Index)
	assert.Equal(uint64(3), entries[1].Index)

	entries = s.EntriesFrom(4)
	assert.Empty(entries)
}

func TestRaftState_TermAt(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.Append([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 3, Data: []byte("b")},
	})

	term, ok := s.TermAt(1)
	assert.True(ok)
	assert.Equal(uint64(1), term)

	term, ok = s.TermAt(2)
	assert.True(ok)
	assert.Equal(uint64(3), term)

	_, ok = s.TermAt(0)
	assert.False(ok)

	_, ok = s.TermAt(3)
	assert.False(ok)
}

func TestRaftState_CommitIndex(t *testing.T) {
	assert := assert.New(t)
	s := NewRaftState()

	s.SetCommitIndex(5)
	assert.Equal(uint64(5), s.CommitIndex())
}
