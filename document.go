// ABOUTME: Document is the fundamental data unit in BantamDB, a semi-structured
// ABOUTME: record identified by a string key with arbitrary byte-valued fields.
package bdb

// Document represents a single record in the database.
type Document struct {
	Id     string
	Fields map[string][]byte
}

// NewDocument creates a document with the given ID and fields.
func NewDocument(id string, fields map[string][]byte) *Document {
	return &Document{
		Id:     id,
		Fields: fields,
	}
}
