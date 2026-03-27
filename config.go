// ABOUTME: Configuration for a BantamDB instance including epoch timing
// ABOUTME: and network settings.
package bdb

import "time"

// Config holds the settings for a BantamDB instance.
type Config struct {
	EpochDuration time.Duration
	Port          int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		EpochDuration: 10 * time.Millisecond,
		Port:          8080,
	}
}
