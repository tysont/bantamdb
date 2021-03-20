package main

type Document struct {
	Id     string
	Fields map[string][]byte
}

func NewDocument(id string, fields map[string][]byte) *Document {
	return &Document{
		Id:     id,
		Fields: fields,
	}
}