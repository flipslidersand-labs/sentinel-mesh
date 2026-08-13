// Package anomaly implements statistical frequency-based anomaly detection.
//
// Each incoming event increments a per-(node, event_type) sliding-window counter.
// When the count within any configured window exceeds its threshold, an alert is
// emitted. No external dependencies are required — the detector is fully in-memory.
package anomaly

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// WindowConfig defines a time window and the event-count threshold for anomaly detection.
type WindowConfig struct {
	// Duration is the length of the sliding window (e.g. time.Minute).
	Duration time.Duration
	// Threshold is the maximum number of events per (node, type) allowed in Duration.
	// Exceeding this count triggers an anomaly alert.
	Threshold int
}

// DefaultWindows are the windows used when none are specified.
var DefaultWindows = []WindowConfig{
	{Duration: time.Minute, Threshold: 30},
	{Duration: 5 * time.Minute, Threshold: 100},
}

// windowKey identifies a per-node, per-event-type sliding window.
type windowKey struct {
	NodeID    string
	EventType string
}

// Detector maintains sliding-window event counters and emits alerts on threshold breach.
type Detector struct {
	mu      sync.Mutex
	windows []WindowConfig
	// timestamps stores the recorded event times for each (node, type) key.
	// Old entries are pruned on each Record call.
	timestamps map[windowKey][]time.Time
}

// New returns a Detector with the given window configurations.
// If windows is nil or empty, DefaultWindows is used.
func New(windows []WindowConfig) *Detector {
	if len(windows) == 0 {
		windows = DefaultWindows
	}
	return &Detector{
		windows:    windows,
		timestamps: make(map[windowKey][]time.Time),
	}
}

// Record registers an event and returns any anomaly alerts triggered by the event.
// It is safe to call Record concurrently.
func (d *Detector) Record(event store.Event) []store.Alert {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := windowKey{NodeID: event.NodeID, EventType: event.Type}
	now := time.Now()

	d.timestamps[key] = append(d.timestamps[key], now)
	d.prune(key, now)

	var alerts []store.Alert
	for _, w := range d.windows {
		count := d.countWithin(key, now, w.Duration)
		if count > w.Threshold {
			alerts = append(alerts, store.Alert{
				AlertID:   uuid.New().String(),
				RuleID:    fmt.Sprintf("anomaly_%s", event.Type),
				NodeID:    event.NodeID,
				EventID:   event.EventID,
				Timestamp: now.UTC(),
				Message: fmt.Sprintf(
					"anomaly: %d %q events in %s exceeds threshold %d",
					count, event.Type, w.Duration, w.Threshold,
				),
				Severity: "warning",
			})
			break // one alert per event even if multiple windows breach
		}
	}
	return alerts
}

// WindowStats returns the current window counts aggregated across all nodes.
// The returned map has the structure: event_type → window_label → count.
// Window labels are formatted as "1m", "5m", "1h", etc.
func (d *Detector) WindowStats() map[string]map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	result := make(map[string]map[string]int)

	for key, ts := range d.timestamps {
		if result[key.EventType] == nil {
			result[key.EventType] = make(map[string]int)
		}
		for _, w := range d.windows {
			label := durationLabel(w.Duration)
			count := 0
			cutoff := now.Add(-w.Duration)
			for _, t := range ts {
				if !t.Before(cutoff) {
					count++
				}
			}
			result[key.EventType][label] += count
		}
	}
	return result
}

// prune removes timestamps older than the longest configured window.
// Must be called with d.mu held.
func (d *Detector) prune(key windowKey, now time.Time) {
	maxDur := d.maxDuration()
	cutoff := now.Add(-maxDur)
	ts := d.timestamps[key]
	i := 0
	for i < len(ts) && ts[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		d.timestamps[key] = ts[i:]
	}
}

// countWithin returns how many timestamps for key fall within dur from now.
// Must be called with d.mu held.
func (d *Detector) countWithin(key windowKey, now time.Time, dur time.Duration) int {
	cutoff := now.Add(-dur)
	count := 0
	for _, t := range d.timestamps[key] {
		if !t.Before(cutoff) {
			count++
		}
	}
	return count
}

func (d *Detector) maxDuration() time.Duration {
	var max time.Duration
	for _, w := range d.windows {
		if w.Duration > max {
			max = w.Duration
		}
	}
	return max
}

func durationLabel(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}
