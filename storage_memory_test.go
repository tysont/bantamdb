package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPutGetRangeStorage(t *testing.T) {
	assert := assert.New(t)
	k := "foo"
	d1 := NewDocument("a", map[string][]byte{k: []byte("bar")})
	d2 := NewDocument("b", map[string][]byte{"baz": []byte("ack")})
	d3 := NewDocument("c", map[string][]byte{"bay": []byte("buh")})
	s := NewMemoryStorage()
	err := s.put(d1)
	assert.NoError(err)
	err = s.put(d2)
	assert.NoError(err)
	err = s.put(d3)
	assert.NoError(err)
	d, err := s.Get(d1.Id)
	assert.NoError(err)
	assert.NotNil(d)
	assert.Equal(d1.Fields[k], d.Fields[k])
	time.Sleep(10 * time.Millisecond)
	c, err := s.Range(0, uint64Max)
	assert.NoError(err)
	i := 0
	for d := range c {
		assert.NotNil(d)
		i++
	}
	assert.Equal(3, i)
}

func TestApplyGetStorage(t *testing.T) {
	assert := assert.New(t)
	id := "a"
	k := "foo"
	d1 := NewDocument(id, map[string][]byte{k: []byte("bar")})
	d2 := NewDocument("b", map[string][]byte{"baz": []byte("ack")})
	d3 := NewDocument("c", map[string][]byte{"bay": []byte("buh")})
	s := NewMemoryStorage()
	err := s.put(d1)
	assert.NoError(err)
	err = s.put(d2)
	assert.NoError(err)
	err = s.put(d3)
	assert.NoError(err)
	v := "zow"
	f := map[string][]byte {k: []byte(v)}
	d4 := NewDocument(id, f)
	cs := []string {id, "b", "c"}
	ds := []*Document {d4}
	txn := NewTransaction(cs, ds)
	b := NewBatch(0, []*Transaction{txn})
	err = s.Apply(b)
	assert.NoError(err)
	d, err := s.Get(id)
	assert.Equal(v, string(d.Fields[k]))
}