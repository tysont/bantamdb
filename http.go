// ABOUTME: HTTP/JSON API handler that exposes the BantamDB coordinator
// ABOUTME: as a RESTful service for documents and transactions.
package bdb

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// Handler translates HTTP requests into Coordinator calls.
type Handler struct {
	coordinator *Coordinator
	mux         *http.ServeMux
}

// NewHandler creates an HTTP handler backed by the given coordinator.
func NewHandler(coordinator *Coordinator) http.Handler {
	h := &Handler{coordinator: coordinator}
	mux := http.NewServeMux()
	mux.HandleFunc("/documents", h.handleDocuments)
	mux.HandleFunc("/documents/", h.handleDocument)
	mux.HandleFunc("/transactions", h.handleTransactions)
	h.mux = mux
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// handleDocuments handles POST /documents (create) and GET /documents (scan).
func (h *Handler) handleDocuments(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.postDocument(w, r)
	case http.MethodGet:
		h.scanDocuments(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDocument handles GET /documents/:id and DELETE /documents/:id.
func (h *Handler) handleDocument(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/documents/")
	if id == "" {
		http.Error(w, "document ID required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.getDocument(w, id)
	case http.MethodDelete:
		h.deleteDocument(w, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) postDocument(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Id     string            `json:"id"`
		Fields map[string][]byte `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if err := h.coordinator.Put(req.Id, req.Fields); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) getDocument(w http.ResponseWriter, id string) {
	doc, err := h.coordinator.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc)
}

func (h *Handler) deleteDocument(w http.ResponseWriter, id string) {
	if err := h.coordinator.Delete(id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) scanDocuments(w http.ResponseWriter, _ *http.Request) {
	docs, err := h.coordinator.Scan()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(docs)
}

// handleTransactions handles POST /transactions.
func (h *Handler) handleTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ReadSet []string    `json:"readSet"`
		Writes  []*Document `json:"writes"`
		Deletes []string    `json:"deletes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	txn := NewTransaction(req.ReadSet, req.Writes, req.Deletes)
	if err := h.coordinator.Transact(txn); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
