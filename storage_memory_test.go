package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutGetApplier(t *testing.T) {
	assert := assert.New(t)
	k := "foo"
	d1 := NewDocument("s", map[string][]byte{k: []byte("bar")})
	d2 := NewDocument("b", map[string][]byte{"baz": []byte("ack")})
	d3 := NewDocument("c", map[string][]byte{"bay": []byte("buh")})
	s := NewMemoryStorage()
	err := s.Put(d1)
	assert.NoError(err)
	err = s.Put(d2)
	assert.NoError(err)
	err = s.Put(d3)
	assert.NoError(err)
	d, err := s.Get(d1.Key)
	assert.NoError(err)
	assert.NotNil(d)
	assert.Equal(d1.Fields[k], d.Fields[k])
	c, err := s.GetRange(0, 4294967295)
	assert.NoError(err)
	i := 0
	for d := range c {
		assert.NotNil(d)
		i++
	}
	assert.Equal(3, i)
}