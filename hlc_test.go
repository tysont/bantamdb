package bdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHLC_Monotonicity(t *testing.T) {
	clock := NewClock()
	prev := clock.Now()
	for range 100 {
		next := clock.Now()
		assert.True(t, next.After(prev), "successive Now() must be monotonically increasing")
		prev = next
	}
}

func TestHLC_UpdateWithFutureRemote(t *testing.T) {
	clock := NewClock()
	local := clock.Now()

	// Simulate a remote timestamp 1 second in the future
	future := Timestamp{WallTime: time.Now().UnixNano() + int64(time.Second), Logical: 0}
	clock.Update(future)

	after := clock.Now()
	assert.True(t, after.After(future), "local clock must advance past the remote timestamp")
	assert.True(t, after.After(local), "local clock must advance past its previous value")
}

func TestHLC_UpdateSameWallTime(t *testing.T) {
	clock := NewClock()
	t1 := clock.Now()

	// Create a remote with the same wall time but higher logical
	remote := Timestamp{WallTime: t1.WallTime, Logical: t1.Logical + 10}
	clock.Update(remote)

	t2 := clock.Now()
	assert.True(t, t2.After(remote), "must advance past remote with same wall time")
}

func TestTimestamp_Compare(t *testing.T) {
	a := Timestamp{WallTime: 100, Logical: 0}
	b := Timestamp{WallTime: 200, Logical: 0}
	c := Timestamp{WallTime: 100, Logical: 1}

	assert.Equal(t, -1, a.Compare(b), "earlier wall time is less")
	assert.Equal(t, 1, b.Compare(a), "later wall time is greater")
	assert.Equal(t, 0, a.Compare(a), "same timestamp is equal")
	assert.Equal(t, -1, a.Compare(c), "same wall time, lower logical is less")
	assert.Equal(t, 1, c.Compare(a), "same wall time, higher logical is greater")
}

func TestTimestamp_After(t *testing.T) {
	a := Timestamp{WallTime: 100, Logical: 0}
	b := Timestamp{WallTime: 200, Logical: 0}

	assert.True(t, b.After(a))
	assert.False(t, a.After(b))
	assert.False(t, a.After(a), "equal timestamps: After should be false")
}

func TestTimestamp_ZeroAndMax(t *testing.T) {
	z := TimestampZero()
	m := TimestampMax()

	assert.True(t, m.After(z), "max must be after zero")
	assert.False(t, z.After(m), "zero must not be after max")
	assert.Equal(t, 0, z.Compare(TimestampZero()), "zero equals zero")
}

func TestTimestamp_JSONRoundTrip(t *testing.T) {
	original := Timestamp{WallTime: 1234567890, Logical: 42}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded Timestamp
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, original, decoded)
}

func TestTimestamp_StringRoundTrip(t *testing.T) {
	original := Timestamp{WallTime: 1234567890, Logical: 42}
	s := original.String()
	assert.NotEmpty(t, s)

	parsed, err := ParseTimestamp(s)
	require.NoError(t, err)
	assert.Equal(t, original, parsed)
}
