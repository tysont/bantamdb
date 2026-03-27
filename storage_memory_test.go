package bdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutGetStorage(t *testing.T) {
	assert := assert.New(t)
	k := "foo"
	d1 := NewDocument("a", map[string][]byte{k: []byte("bar")})
	d2 := NewDocument("b", map[string][]byte{"baz": []byte("ack")})
	d3 := NewDocument("c", map[string][]byte{"bay": []byte("buh")})
	s := NewMemoryStorage()
	assert.NoError(s.put(d1))
	assert.NoError(s.put(d2))
	assert.NoError(s.put(d3))
	d, err := s.Get(d1.Id)
	assert.NoError(err)
	assert.NotNil(d)
	assert.Equal(d1.Fields[k], d.Fields[k])
}

func TestApplyGetStorage(t *testing.T) {
	assert := assert.New(t)
	id := "a"
	k := "foo"
	d1 := NewDocument(id, map[string][]byte{k: []byte("bar")})
	s := NewMemoryStorage()
	assert.NoError(s.put(d1))
	v := "zow"
	f := map[string][]byte{k: []byte(v)}
	d4 := NewDocument(id, f)
	txn := NewTransaction([]string{id}, []*Document{d4}, nil)
	b := NewBatch(0, []*Transaction{txn})
	err := s.Apply(b)
	assert.NoError(err)
	d, err := s.Get(id)
	assert.NoError(err)
	assert.Equal(v, string(d.Fields[k]))
}
