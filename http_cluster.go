// ABOUTME: HTTP endpoints for cluster management including join, leave,
// ABOUTME: and status operations.
package bdb

import (
	"encoding/json"
	"net/http"
)

// ClusterStatus represents the current state of the cluster as seen
// by this node.
type ClusterStatus struct {
	NodeID    string   `json:"nodeId"`
	Role      string   `json:"role"`
	LeaderID  string   `json:"leaderId"`
	Peers     []string `json:"peers"`
	Term      uint64   `json:"term"`
	CommitIdx uint64   `json:"commitIndex"`
}

// RegisterClusterHandlers adds cluster management endpoints to the mux.
// Requires a RaftLog to access cluster state.
func RegisterClusterHandlers(mux *http.ServeMux, raftLog *RaftLog) {
	mux.HandleFunc("/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		node := raftLog.node
		role := "follower"
		switch node.Role() {
		case Leader:
			role = "leader"
		case Candidate:
			role = "candidate"
		}
		status := ClusterStatus{
			NodeID:    node.ID(),
			Role:      role,
			LeaderID:  node.LeaderID(),
			Peers:     node.peers,
			Term:      node.state.CurrentTerm(),
			CommitIdx: node.state.CommitIndex(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
}
