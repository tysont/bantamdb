# BantamDB

A minimal distributed database built on the [Calvin protocol](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) for deterministic transactions, inspired by [Fauna's](https://fauna.com) Distributed Transaction Engine.

BantamDB is a reference implementation designed to be simple, readable, and faithful to the Calvin architecture. Everything is built from scratch, including the Raft consensus implementation. It implements three self-sufficient layers that mirror Fauna's DTE:

```
  Node A               Node B               Node C
  ┌──────────┐         ┌──────────┐         ┌──────────┐
  │Coordinator│         │Coordinator│         │Coordinator│
  │ Raft Log │←───────→│ Raft Log │←───────→│ Raft Log │
  │ Storage  │         │ Storage  │         │ Storage  │
  └──────────┘         └──────────┘         └──────────┘
```

## How it works

**Writes** are synchronous and strongly consistent. The coordinator appends the transaction to the Raft log, which replicates it to a quorum of nodes before committing. Each committed batch receives an HLC (Hybrid Logical Clock) timestamp that provides causal ordering across nodes. The coordinator blocks until the write is committed and returns the timestamp to the client.

**Reads** bypass the log and are served directly from storage using MVCC (Multi-Version Concurrency Control). Each document keeps a history of versions keyed by HLC timestamp, enabling point-in-time reads. The coordinator waits for local storage to catch up to the requested timestamp before reading, ensuring read-your-own-writes consistency.

**Consensus** uses a custom Raft implementation with leader election, log replication, and quorum-based commitment. The leader batches transactions into epochs and proposes them through Raft. Followers receive committed entries and apply them to local storage. If the leader fails, a new leader is elected and the cluster continues serving requests.

**Optimistic concurrency control** happens in the storage layer. Each transaction declares a read set. Before applying writes, storage checks that no key in the read set was modified after the transaction's timestamp. Conflicting transactions are rejected deterministically on all nodes.

## Quick start

**Single node:**
```sh
go build -o bdb ./cmd/bdb
./bdb
```

**Three-node cluster:**
```sh
./bdb --port 8081 --raft-addr :9001 --node-id node1 --peers :9002,:9003 --bootstrap
./bdb --port 8082 --raft-addr :9002 --node-id node2 --peers :9001,:9003
./bdb --port 8083 --raft-addr :9003 --node-id node3 --peers :9001,:9002
```

## HTTP API

**Create or update a document:**
```sh
curl -X POST localhost:8080/documents \
  -d '{"id": "user1", "fields": {"name": "YWxpY2U="}}'
# Returns: {"timestamp": "1711484400000000000:0"}
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

**Cluster status:**
```sh
curl localhost:8080/cluster/status
```

Write operations return `201 Created` with a commit timestamp. Field values are byte arrays represented as base64 in JSON.

## Testing

```sh
go test ./...           # run all tests
go test -race ./...     # run with race detector
go test -v ./...        # verbose output
```

The test suite includes unit tests for each layer, single-node integration tests, and multi-node cluster tests covering leader election, failover, data replication, concurrent writes, and quorum loss detection.

## Architecture

| Layer | Files | Role |
|-------|-------|------|
| Query Coordinator | `coordinator.go` | Stateless compute. Writes block until Raft commits. Reads wait for storage catch-up. |
| Transaction Log | `log.go`, `log_memory.go` | In-memory log for single-node mode. |
| Raft Consensus | `raft_node.go`, `raft_state.go`, `raft_rpc.go`, `raft_log.go` | Custom Raft: leader election, log replication, quorum commitment. |
| Transport | `raft_transport.go` | HTTP/JSON for production, channel-based for testing. |
| Data Storage | `storage.go`, `storage_memory.go` | MVCC with point-in-time reads. Tails the log. OCC validation. |
| Hybrid Logical Clock | `hlc.go` | Distributed timestamps combining wall-clock and logical counters. |
| HTTP API | `http.go`, `http_cluster.go` | REST endpoints for documents, transactions, and cluster management. |
| CLI | `cmd/bdb/main.go` | Wires layers together. Supports single-node and clustered modes. |

## References

- [Calvin: Fast Distributed Transactions for Partitioned Database Systems](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) (Thomson et al., SIGMOD 2012)
- [Fauna Architectural Overview](https://fauna.com/architecture)
- [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) (Ongaro & Ousterhout, USENIX ATC 2014)
