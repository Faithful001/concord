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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
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
	Peers() []string
	Submit(cmd []byte) error
	IsLearner() bool
	ReadIndex(ctx context.Context) (int, error)
	WaitApplied(ctx context.Context, index int) error
	CommitIndex() int
	LastApplied() int
}

// Server is the HTTP API server.
type Server struct {
	nodeID   string
	node     RaftNode
	store    *storage.Store
	mu       sync.RWMutex
	apiPeers map[string]string // peer-id → "host:port" (no scheme)
	client   *http.Client
}

// New creates a new API Server.
// apiPeers maps peer node IDs to their API "host:port" address (without http://).
func New(nodeID string, node RaftNode, store *storage.Store, apiPeers map[string]string) *Server {
	peersCopy := make(map[string]string, len(apiPeers))
	for k, v := range apiPeers {
		peersCopy[k] = v
	}
	return &Server{
		nodeID:   nodeID,
		node:     node,
		store:    store,
		apiPeers: peersCopy,
		client:   &http.Client{Timeout: 6 * time.Second},
	}
}

// AddAPIPeer registers a peer's API address.
func (s *Server) AddAPIPeer(id, apiAddr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apiPeers[id] = apiAddr
}

// RemoveAPIPeer unregisters a peer's API address.
func (s *Server) RemoveAPIPeer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.apiPeers, id)
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
	mux.HandleFunc("/v1/read-index", s.handleReadIndex)
	mux.HandleFunc("/v1/members", s.handleMembers)
	mux.HandleFunc("/v1/members/", s.handleMemberDelete)
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

func (s *Server) doGet(w http.ResponseWriter, r *http.Request, key string) {
	linearizable := r.URL.Query().Get("linearizable") == "true" || r.URL.Query().Get("consistent") == "true"
	if linearizable {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		if s.node.IsLeader() {
			if _, err := s.node.ReadIndex(ctx); err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					s.forwardToLeader(w, r, nil)
					return
				}
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
				return
			}
		} else {
			readIndex, err := s.fetchLeaderReadIndex(ctx)
			if err != nil {
				s.forwardToLeader(w, r, nil)
				return
			}
			if err := s.node.WaitApplied(ctx, readIndex); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
				return
			}
		}
	}

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

func (s *Server) handleReadIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.node.IsLeader() {
		s.forwardToLeader(w, r, nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	idx, err := s.node.ReadIndex(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"read_index": idx})
}

func (s *Server) fetchLeaderReadIndex(ctx context.Context) (int, error) {
	leaderID := s.node.CurrentLeaderID()
	if leaderID == "" {
		return 0, errors.New("no leader known")
	}
	s.mu.RLock()
	apiAddr, ok := s.apiPeers[leaderID]
	s.mu.RUnlock()
	if !ok {
		return 0, errors.New("leader API address not found")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+apiAddr+"/v1/read-index", nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("read-index returned status %d", resp.StatusCode)
	}

	var res struct {
		ReadIndex int `json:"read_index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}
	return res.ReadIndex, nil
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

// ── Cluster membership handlers ───────────────────────────────────────────────

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.doGetMembers(w, r)
	case http.MethodPost:
		s.doAddMember(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleMemberDelete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/members/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "member id is required"})
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.doRemoveMember(w, r, id)
}

type MemberInfo struct {
	ID        string `json:"id"`
	APIAddr   string `json:"api_addr,omitempty"`
	IsLeader  bool   `json:"is_leader"`
	IsSelf    bool   `json:"is_self"`
	IsLearner bool   `json:"is_learner,omitempty"`
}

func (s *Server) doGetMembers(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	leaderID := s.node.CurrentLeaderID()
	peers := s.node.Peers()

	members := make([]MemberInfo, 0, len(peers)+1)
	members = append(members, MemberInfo{
		ID:        s.nodeID,
		IsLeader:  s.node.IsLeader(),
		IsSelf:    true,
		IsLearner: s.node.IsLearner(),
	})

	for _, p := range peers {
		members = append(members, MemberInfo{
			ID:       p,
			APIAddr:  s.apiPeers[p],
			IsLeader: p == leaderID,
			IsSelf:   false,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"leader_id": leaderID,
		"members":   members,
	})
}

func (s *Server) doAddMember(w http.ResponseWriter, r *http.Request) {
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
		ID        string `json:"id"`
		RaftAddr  string `json:"raft_addr"`
		APIAddr   string `json:"api_addr"`
		IsLearner bool   `json:"is_learner"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.ID == "" || req.RaftAddr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id and raft_addr are required"})
		return
	}

	var cmd []byte
	if req.IsLearner {
		cmd = command.EncodeAddLearner(req.ID, req.RaftAddr, req.APIAddr)
	} else {
		cmd = command.EncodeAddPeer(req.ID, req.RaftAddr, req.APIAddr)
	}

	if err := s.node.Submit(cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "added",
		"id":         req.ID,
		"raft_addr":  req.RaftAddr,
		"api_addr":   req.APIAddr,
		"is_learner": req.IsLearner,
	})
}

func (s *Server) doRemoveMember(w http.ResponseWriter, r *http.Request, id string) {
	if !s.node.IsLeader() {
		s.forwardToLeader(w, r, nil)
		return
	}

	cmd := command.EncodeRemovePeer(id)
	if err := s.node.Submit(cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, nil)
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "removed",
		"id":     id,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	role := "follower"
	if s.node.IsLearner() {
		role = "learner"
	} else if s.node.IsLeader() {
		role = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":      s.nodeID,
		"role":         role,
		"leader_id":    s.node.CurrentLeaderID(),
		"is_learner":   s.node.IsLearner(),
		"commit_index": s.node.CommitIndex(),
		"last_applied": s.node.LastApplied(),
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
