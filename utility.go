// ABOUTME: General-purpose utility functions used across the codebase,
// ABOUTME: including random string generation for IDs.
package bdb

import "math/rand/v2"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandomString generates a random alphabetic string of length n.
func RandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}
