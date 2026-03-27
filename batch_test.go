package bdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch_MarshalRoundTrip(t *testing.T) {
	assert := assert.New(t)

	original := NewBatch(Timestamp{WallTime: 12345, Logical: 7}, []*Transaction{
		NewTransaction([]string{"a"}, []*Document{
			NewDocument("b", map[string][]byte{"k": []byte("v")}),
		}, []string{"c"}),
	})

	data, err := original.Marshal()
	require.NoError(t, err)

	decoded, err := UnmarshalBatch(data)
	require.NoError(t, err)

	assert.Equal(original.Timestamp, decoded.Timestamp)
	assert.Len(decoded.Transactions, 1)
	assert.Equal([]string{"a"}, decoded.Transactions[0].ReadSet)
	assert.Equal("b", decoded.Transactions[0].Writes[0].Id)
	assert.Equal([]byte("v"), decoded.Transactions[0].Writes[0].Fields["k"])
	assert.Equal([]string{"c"}, decoded.Transactions[0].Deletes)
}
