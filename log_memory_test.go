package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWriteBatchTransactionLog(t *testing.T) {
	assert := assert.New(t)
	id := "a"
	k := "foo"
	v := "zow"
	f := map[string][]byte {k: []byte(v)}
	d1 := NewDocument(id, f)
	cs := []string {id, "b", "c"}
	ds := []*Document {d1}
	txn := NewTransaction(cs, ds)
	l := NewMemoryLog()
	err := l.Write(txn)
	assert.NoError(err)
	time.Sleep(epochMilliseconds * 2 * time.Millisecond)
	c, err := l.Range(0)
	assert.NoError(err)
	i := 0
	for b := range c {
		assert.NotNil(b)
		i += len(b.Transactions)
	}
	assert.Equal(1, i)
}