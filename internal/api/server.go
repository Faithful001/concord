// Package api implements the client-facing HTTP API for Concord.
//
// Endpoints:
//
//	GET    /v1/kv/{key}          — read a key (served locally; may be slightly stale on followers)
//	PUT    /v1/kv/{key}          — set a key; body: {"value":"..."}
//	DELETE /v1/kv/{key}          — delete a key
//	GET    /v1/status            — node role + leader info
//	GET    /healthz              — liveness probe
//
// Writes (PUT/DELETE) are routed to the current leader.  If this node is the
// leader the request is handled directly via node.Submit.  If this node is a
// follower it reverse-proxies the request to the leader's API address.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Faithful001/concord.git/internal/command"
	"github.com/Faithful001/concord.git/pkg/raft"
	"github.com/Faithful001/concord.git/internal/storage"
)

// RaftNode is the subset of raft.Node the API layer depends on.
// Using an interface keeps the API package testable without a real Raft node.
type RaftNode interface {
	IsLeader() bool
	CurrentLeaderID() string
	Submit(cmd []byte) error
}

// Server is the HTTP API server.
type Server struct {
	nodeID   string
	node     RaftNode
	store    *storage.Store
	apiPeers map[string]string // peer-id → "host:port" (no scheme)
	client   *http.Client
}

// New creates a new API Server.
// apiPeers maps peer node IDs to their API "host:port" address (without http://).
func New(nodeID string, node RaftNode, store *storage.Store, apiPeers map[string]string) *Server {
	return &Server{
		nodeID:   nodeID,
		node:     node,
		store:    store,
		apiPeers: apiPeers,
		client:   &http.Client{Timeout: 6 * time.Second},
	}
}

// Serve starts the HTTP server on addr. Blocks until it exits.
func (s *Server) Serve(addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Printf("[%s] API server listening on %s", s.nodeID, addr)
	return srv.ListenAndServe()
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/kv/", s.handleKV)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

// ── Route dispatch ────────────────────────────────────────────────────────────

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.doGet(w, r, key)
	case http.MethodPut:
		s.doPut(w, r, key)
	case http.MethodDelete:
		s.doDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func (s *Server) doGet(w http.ResponseWriter, _ *http.Request, key string) {
	// Reads are served locally. Followers may return data that is up to one
	// heartbeat interval stale. For linearisable reads, clients should target
	// the leader directly or use /v1/status to discover it.
	value, err := s.store.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": string(value)})
}

func (s *Server) doPut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	if !s.node.IsLeader() {
		s.forwardToLeader(w, r, body)
		return
	}

	var req struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	cmd := command.EncodeSet(key, []byte(req.Value))
	if err := s.node.Submit(cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": req.Value})
}

func (s *Server) doDelete(w http.ResponseWriter, r *http.Request, key string) {
	if !s.node.IsLeader() {
		s.forwardToLeader(w, r, nil)
		return
	}

	cmd := command.EncodeDelete(key)
	if err := s.node.Submit(cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, nil)
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	role := "follower"
	if s.node.IsLeader() {
		role = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"node_id":   s.nodeID,
		"role":      role,
		"leader_id": s.node.CurrentLeaderID(),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// ── Leader forwarding ─────────────────────────────────────────────────────────

// forwardToLeader reverse-proxies the current request to the leader's API.
// body is the already-read request body (may be nil for bodyless methods).
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) {
	leaderID := s.node.CurrentLeaderID()
	if leaderID == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader known; cluster may be electing"})
		return
	}
	if leaderID == s.nodeID {
		// Race between IsLeader check and leaderID lookup; retry on client.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership transition in progress"})
		return
	}
	apiAddr, ok := s.apiPeers[leaderID]
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leader API address not configured"})
		return
	}

	target := "http://" + apiAddr + r.URL.RequestURI()

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, target, bodyReader)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "proxy request build failed"})
		return
	}
	proxyReq.Header = r.Header.Clone()

	resp, err := s.client.Do(proxyReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "leader unreachable: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
