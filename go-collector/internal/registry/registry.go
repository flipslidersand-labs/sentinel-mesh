package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultRegion is assigned when an agent registers without a region.
const DefaultRegion = "default"

// AgentNode represents a registered eBPF agent.
type AgentNode struct {
	NodeID     string    `json:"node_id"`
	Hostname   string    `json:"hostname"`
	IP         string    `json:"ip"`
	Version    string    `json:"version"`
	Region     string    `json:"region"`
	Registered time.Time `json:"registered"`
	LastSeen   time.Time `json:"last_seen"`
	Status     string    `json:"status"` // "active" | "inactive"
}

// normalizeRegion maps an empty region to DefaultRegion.
func normalizeRegion(region string) string {
	if region == "" {
		return DefaultRegion
	}
	return region
}

// Registry holds agent nodes in memory (backed by BadgerDB in Phase 4).
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*AgentNode
}

func New() *Registry {
	return &Registry{nodes: map[string]*AgentNode{}}
}

func (r *Registry) Register(nodeID, hostname, ip, version, region string) error {
	if nodeID == "" {
		return fmt.Errorf("node_id required")
	}
	region = normalizeRegion(region)
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := r.nodes[nodeID]; ok {
		existing.LastSeen = now
		existing.Status = "active"
		existing.Region = region
		return nil
	}
	r.nodes[nodeID] = &AgentNode{
		NodeID:     nodeID,
		Hostname:   hostname,
		IP:         ip,
		Version:    version,
		Region:     region,
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

func (r *Registry) List() []AgentNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentNode, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

// ListByRegion returns nodes in the given region. An empty region argument is
// normalized to DefaultRegion, matching how nodes are stored.
func (r *Registry) ListByRegion(region string) []AgentNode {
	region = normalizeRegion(region)
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentNode, 0)
	for _, n := range r.nodes {
		if n.Region == region {
			out = append(out, *n)
		}
	}
	return out
}

// Regions returns the sorted, unique set of regions currently registered.
func (r *Registry) Regions() []string {
	r.mu.RLock()
	seen := make(map[string]struct{}, len(r.nodes))
	for _, n := range r.nodes {
		seen[n.Region] = struct{}{}
	}
	r.mu.RUnlock()

	out := make([]string, 0, len(seen))
	for region := range seen {
		out = append(out, region)
	}
	sort.Strings(out)
	return out
}

func (r *Registry) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.List())
}

// StartHeartbeatChecker runs a background goroutine that marks agents inactive
// when their LastSeen is older than timeout. It ticks every interval.
// The goroutine stops when ctx is cancelled.
func (r *Registry) StartHeartbeatChecker(ctx context.Context, timeout, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.markStaleInactive(timeout)
			}
		}
	}()
}

func (r *Registry) markStaleInactive(timeout time.Duration) {
	deadline := time.Now().UTC().Add(-timeout)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.nodes {
		if n.Status == "active" && n.LastSeen.Before(deadline) {
			n.Status = "inactive"
		}
	}
}
