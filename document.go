package main

type Document struct {
	Key    string
	Fields map[string][]byte
}

func NewDocument(key string, fields map[string][]byte) *Document {
	return &Document{
		Key: key,
		Fields: fields,
	}
}