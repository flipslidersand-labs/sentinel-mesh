// Package model holds the Event/Alert domain types, independent of any
// persistence implementation. internal/store keeps compatibility aliases.
package model

import (
	"encoding/json"
	"time"
)

// Event is the normalized event form.
type Event struct {
	EventID   string          `json:"event_id"`
	NodeID    string          `json:"node_id"`
	Timestamp time.Time       `json:"timestamp"`
	Type      string          `json:"type"` // "exec" | "tcp" | "file"
	Payload   json.RawMessage `json:"payload"`
	// ReceivedAt is the collector's own receive time, stamped on save and
	// returned by /api/events so aggregators can order across regions without
	// trusting the agent clock (#473). Zero (omitted) on events stored before
	// this field existed; consumers then fall back to Timestamp.
	ReceivedAt time.Time `json:"received_at,omitzero"`
}

// Alert represents a triggered alert.
type Alert struct {
	AlertID   string    `json:"alert_id"`
	RuleID    string    `json:"rule_id"`
	NodeID    string    `json:"node_id"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Severity  string    `json:"severity"`
}
