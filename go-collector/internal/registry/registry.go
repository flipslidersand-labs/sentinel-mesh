package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AgentNode represents a registered eBPF agent.
type AgentNode struct {
	NodeID     string    `json:"node_id"`
	Hostname   string    `json:"hostname"`
	IP         string    `json:"ip"`
	Version    string    `json:"version"`
	Registered time.Time `json:"registered"`
	LastSeen   time.Time `json:"last_seen"`
	Status     string    `json:"status"` // "active" | "inactive"
}

// Registry holds agent nodes in memory (backed by BadgerDB in Phase 4).
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*AgentNode
}

func New() *Registry {
	return &Registry{nodes: map[string]*AgentNode{}}
}

func (r *Registry) Register(nodeID, hostname, ip, version string) error {
	if nodeID == "" {
		return fmt.Errorf("node_id required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := r.nodes[nodeID]; ok {
		existing.LastSeen = now
		existing.Status = "active"
		return nil
	}
	r.nodes[nodeID] = &AgentNode{
		NodeID:     nodeID,
		Hostname:   hostname,
		IP:         ip,
		Version:    version,
		Registered: now,
		LastSeen:   now,
		Status:     "active",
	}
	return nil
}

func (r *Registry) Heartbeat(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		n.LastSeen = time.Now().UTC()
	}
}

func (r *Registry) SetInactive(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		n.Status = "inactive"
	}
}

// ReapInactive marks any active node whose LastSeen is older than timeout as
// inactive. It returns the node IDs that were transitioned, so the caller can
// log them. Intended to be called periodically from a background ticker.
func (r *Registry) ReapInactive(timeout time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().UTC().Add(-timeout)
	var reaped []string
	for id, n := range r.nodes {
		if n.Status == "active" && n.LastSeen.Before(cutoff) {
			n.Status = "inactive"
			reaped = append(reaped, id)
		}
	}
	return reaped
}

func (r *Registry) List() []AgentNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentNode, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

func (r *Registry) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.List())
}
