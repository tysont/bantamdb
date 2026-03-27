# BantamDB

A minimal database built on the [Calvin protocol](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) for deterministic transactions, inspired by [Fauna's](https://fauna.com) Distributed Transaction Engine.

BantamDB is a reference implementation designed to be simple, readable, and faithful to the Calvin architecture. It implements three self-sufficient layers that mirror Fauna's DTE:

```
        HTTP/JSON API
             |
    [Query Coordinator]        stateless compute: routes writes and reads
             |
     [Transaction Log]         epoch-based batching with subscriber channel
             |
       [Data Storage]          tails the log, validates OCC, applies writes
```

## How it works

**Writes** flow through the full pipeline: the coordinator builds a transaction and appends it to the log. The log batches transactions into epochs on a configurable interval and delivers them to storage via a channel. Storage validates each transaction using optimistic concurrency control and applies the writes.

**Reads** bypass the log entirely and are served directly from storage, matching Fauna's architecture where read requests go straight to data nodes.

**Optimistic concurrency control** happens in the storage layer. Each transaction declares a read set. Before applying writes, storage checks that no key in the read set was modified at a later epoch. Conflicting transactions are rejected deterministically.

## Quick start

```sh
go build -o bdb ./cmd/bdb
./bdb
```

This starts an HTTP server on port 8080 with a 10ms epoch interval. Use `--port` and `--epoch` to configure.

## HTTP API

Field values are byte arrays, represented as base64 in JSON.

**Create or update a document:**
```sh
curl -X POST localhost:8080/documents \
  -d '{"id": "user1", "fields": {"name": "YWxpY2U="}}'
```

**Get a document:**
```sh
curl localhost:8080/documents/user1
```

**Delete a document:**
```sh
curl -X DELETE localhost:8080/documents/user1
```

**List all documents:**
```sh
curl localhost:8080/documents
```

**Submit a multi-document transaction:**
```sh
curl -X POST localhost:8080/transactions \
  -d '{"readSet": ["user1"], "writes": [{"Id": "user1", "Fields": {"name": "Ym9i"}}]}'
```

Write operations return `202 Accepted` because they are asynchronous -- the transaction is appended to the log and will be applied on the next epoch tick.

## Testing

```sh
go test ./...           # run all tests
go test -race ./...     # run with race detector
go test -v ./...        # verbose output
```

## Architecture

| Layer | Files | Role |
|-------|-------|------|
| Query Coordinator | `coordinator.go` | Stateless compute. Routes writes to the log, reads to storage. |
| Transaction Log | `log.go`, `log_memory.go` | Self-ticking epoch batching. Delivers batches to subscribers via channel. |
| Data Storage | `storage.go`, `storage_memory.go` | Tails the log. Validates OCC. Applies writes and serves reads. |
| HTTP API | `http.go` | Translates REST requests into coordinator calls. |
| CLI | `cmd/bdb/main.go` | Wires layers together, starts HTTP server. |

## References

- [Calvin: Fast Distributed Transactions for Partitioned Database Systems](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) (Thomson et al., SIGMOD 2012)
- [Fauna Architectural Overview](https://fauna.com/architecture)
