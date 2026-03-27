// ABOUTME: Configuration for a BantamDB instance including epoch timing,
// ABOUTME: network settings, and Raft cluster parameters.
package bdb

import "time"

// Config holds the settings for a BantamDB instance.
type Config struct {
	// Calvin settings
	EpochDuration time.Duration
	Port          int

	// Raft settings
	NodeID            string
	RaftAddr          string        // bind address for Raft RPCs
	Peers             []string      // addresses of other nodes in the cluster
	ElectionTimeout   time.Duration // base timeout (randomized 1x-2x)
	HeartbeatInterval time.Duration
	Bootstrap         bool   // true for the first node in a new cluster
	JoinAddr          string // address of existing node to join
}

// DefaultConfig returns a Config with sensible defaults for single-node use.
func DefaultConfig() Config {
	return Config{
		EpochDuration:     10 * time.Millisecond,
		Port:              8080,
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
	}
}
