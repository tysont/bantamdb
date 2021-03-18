package main

import (
	"github.com/spaolacci/murmur3"
	"math/rand"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GetRandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func GetMurmurHash(s string) uint32 {
	h := murmur3.New32()
	h.Write([]byte(s))
	k := h.Sum32()
	return k
}