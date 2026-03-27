// ABOUTME: Sentinel errors for BantamDB operations, used across layers
// ABOUTME: for consistent error handling with errors.Is().
package bdb

import "errors"

var (
	// ErrNotFound is returned when a requested document does not exist.
	ErrNotFound = errors.New("document not found")

	// ErrConflict is returned when a transaction fails OCC validation
	// because a key in its read set was modified after the transaction's
	// snapshot epoch.
	ErrConflict = errors.New("transaction conflict")

	// ErrStopped is returned when an operation is attempted on a stopped component.
	ErrStopped = errors.New("component is stopped")
)
