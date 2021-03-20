package main

import (
	"github.com/spaolacci/murmur3"
	"math/rand"
)

const uint32Max = 4294967295
const uint64Max = 18446744073709551615

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GetRandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func GetMurmurHash(s string) uint64 {
	h := murmur3.New64()
	h.Write([]byte(s))
	k := h.Sum64()
	return k
}