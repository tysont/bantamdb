// ABOUTME: Hybrid Logical Clock implementation for distributed MVCC timestamps.
// ABOUTME: Combines physical wall-clock time with a logical counter for causal ordering.
package bdb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Timestamp is a hybrid logical clock value that combines physical wall-clock
// time with a logical counter. It provides causal ordering across distributed
// nodes: if event A happens before event B, then A's timestamp is strictly
// less than B's, even when wall clocks are imperfectly synchronized.
type Timestamp struct {
	WallTime int64  `json:"wallTime"` // physical time in nanoseconds since epoch
	Logical  uint32 `json:"logical"`  // logical counter for same-wall-time ordering
}

// TimestampZero returns the zero timestamp, representing the earliest possible time.
func TimestampZero() Timestamp {
	return Timestamp{}
}

// TimestampMax returns the maximum possible timestamp.
func TimestampMax() Timestamp {
	return Timestamp{WallTime: math.MaxInt64, Logical: math.MaxUint32}
}

// Compare returns -1 if t < other, 0 if t == other, 1 if t > other.
func (t Timestamp) Compare(other Timestamp) int {
	if t.WallTime < other.WallTime {
		return -1
	}
	if t.WallTime > other.WallTime {
		return 1
	}
	if t.Logical < other.Logical {
		return -1
	}
	if t.Logical > other.Logical {
		return 1
	}
	return 0
}

// After reports whether t is strictly after other.
func (t Timestamp) After(other Timestamp) bool {
	return t.Compare(other) > 0
}

// IsZero reports whether this is the zero timestamp.
func (t Timestamp) IsZero() bool {
	return t.WallTime == 0 && t.Logical == 0
}

// String returns a string representation in the form "wallTime:logical".
func (t Timestamp) String() string {
	return fmt.Sprintf("%d:%d", t.WallTime, t.Logical)
}

// ParseTimestamp parses a timestamp from its string representation.
func ParseTimestamp(s string) (Timestamp, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return Timestamp{}, fmt.Errorf("invalid timestamp format: %q", s)
	}
	wall, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Timestamp{}, fmt.Errorf("invalid wall time: %w", err)
	}
	logical, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return Timestamp{}, fmt.Errorf("invalid logical counter: %w", err)
	}
	return Timestamp{WallTime: wall, Logical: uint32(logical)}, nil
}

// Clock is a hybrid logical clock that produces monotonically increasing
// timestamps. It is safe for concurrent use.
type Clock struct {
	mu      sync.Mutex
	current Timestamp
	now     func() time.Time // injectable for testing
}

// NewClock creates a new hybrid logical clock.
func NewClock() *Clock {
	return &Clock{now: time.Now}
}

// Now returns a new timestamp that is guaranteed to be greater than all
// previously returned timestamps and at least as large as the current
// wall-clock time.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := c.now().UnixNano()
	if wall > c.current.WallTime {
		c.current = Timestamp{WallTime: wall, Logical: 0}
	} else {
		c.current.Logical++
	}
	return c.current
}

// Update merges the clock with a remote timestamp, ensuring the local
// clock advances past both its current value and the remote value.
func (c *Clock) Update(remote Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := c.now().UnixNano()
	if wall > c.current.WallTime && wall > remote.WallTime {
		c.current = Timestamp{WallTime: wall, Logical: 0}
	} else if remote.WallTime > c.current.WallTime {
		c.current = Timestamp{WallTime: remote.WallTime, Logical: remote.Logical + 1}
	} else if c.current.WallTime > remote.WallTime {
		c.current.Logical++
	} else {
		// Same wall time: take max logical and increment
		if remote.Logical > c.current.Logical {
			c.current.Logical = remote.Logical + 1
		} else {
			c.current.Logical++
		}
	}
}
