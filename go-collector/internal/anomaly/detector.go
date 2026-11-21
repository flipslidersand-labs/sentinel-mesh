// Package anomaly implements statistical frequency-based anomaly detection.
//
// Each incoming event increments a per-(node, event_type) sliding-window counter.
// When the count within any configured window exceeds its threshold, an alert is
// emitted. No external dependencies are required — the detector is fully in-memory.
package anomaly

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
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

// ring is a fixed-size ring of per-second event counters for one key.
// bucket for second s lives at index s mod len(buckets); it is valid only for
// seconds in (head-len, head]. Memory is len(buckets)*4 bytes regardless of
// event rate.
type ring struct {
	buckets []uint32
	head    int64 // newest second (Unix) recorded into the ring
}

func newRing(size int, sec int64) *ring {
	return &ring{buckets: make([]uint32, size), head: sec}
}

func (r *ring) idx(sec int64) int {
	n := int64(len(r.buckets))
	return int(((sec % n) + n) % n)
}

// add records one event at second sec. Stale buckets passed over while
// advancing are reset, so a key idle for longer than the ring starts clean.
// An older sec (clock stepped backwards) is treated as the head second, so the
// event is counted in the current bucket and stays visible to count (which
// clamps its sec the same way) instead of being dropped or hidden.
func (r *ring) add(sec int64) {
	if sec > r.head {
		gap := sec - r.head
		if gap > int64(len(r.buckets)) {
			gap = int64(len(r.buckets))
		}
		for i := int64(1); i <= gap; i++ {
			r.buckets[r.idx(r.head+i)] = 0
		}
		r.head = sec
	}
	r.buckets[r.idx(r.head)]++
}

// count returns the events in the last windowSecs seconds ending at second sec
// (inclusive), i.e. seconds sec-windowSecs+1 .. sec.
// A sec before head (clock stepped backwards) is clamped to head, matching add.
func (r *ring) count(sec, windowSecs int64) int {
	if sec < r.head {
		sec = r.head
	}
	lo := sec - windowSecs + 1
	if min := r.head - int64(len(r.buckets)) + 1; lo < min {
		lo = min
	}
	hi := sec
	if hi > r.head {
		hi = r.head
	}
	total := 0
	for s := lo; s <= hi; s++ {
		total += int(r.buckets[r.idx(s)])
	}
	return total
}

// Detector maintains sliding-window event counters and emits alerts on threshold breach.
// Counting has 1-second granularity: a window of N seconds covers the current
// second and the N-1 before it.
type Detector struct {
	mu      sync.Mutex
	windows []WindowConfig
	// winSecs[i] is windows[i].Duration in whole seconds (rounded up, min 1).
	winSecs []int64
	// ringSize is the number of 1s buckets per key (longest window).
	ringSize int
	// rings holds the per-second counters for each (node, type) key.
	rings map[windowKey]*ring
	// lastAlert records when each (node, type, window) last fired, so the same
	// window does not re-alert until its Duration has elapsed (cooldown).
	// Entries whose cooldown has expired are removed by gc.
	lastAlert map[alertKey]time.Time
	// now returns the current time; replaced in tests.
	now func() time.Time
}

// alertKey identifies the cooldown state of one window (by index in windows)
// for one (node, type) key.
type alertKey struct {
	windowKey
	window int
}

// New returns a Detector with the given window configurations.
// If windows is nil or empty, DefaultWindows is used.
func New(windows []WindowConfig) *Detector {
	if len(windows) == 0 {
		windows = DefaultWindows
	}
	d := &Detector{
		windows:   windows,
		winSecs:   make([]int64, len(windows)),
		rings:     make(map[windowKey]*ring),
		lastAlert: make(map[alertKey]time.Time),
		now:       time.Now,
	}
	d.ringSize = 1
	for i, w := range windows {
		s := int64((w.Duration + time.Second - 1) / time.Second)
		if s < 1 {
			s = 1
		}
		d.winSecs[i] = s
		if int(s) > d.ringSize {
			d.ringSize = int(s)
		}
	}
	return d
}

// Record registers an event and returns any anomaly alerts triggered by the event.
// It is safe to call Record concurrently.
func (d *Detector) Record(event model.Event) []model.Alert {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := windowKey{NodeID: event.NodeID, EventType: event.Type}
	now := d.now()

	sec := now.Unix()
	r := d.rings[key]
	if r == nil {
		r = newRing(d.ringSize, sec)
		d.rings[key] = r
	}
	r.add(sec)

	var alerts []model.Alert
	for i, w := range d.windows {
		count := r.count(sec, d.winSecs[i])
		if count > w.Threshold {
			ak := alertKey{windowKey: key, window: i}
			if last, ok := d.lastAlert[ak]; ok && now.Sub(last) < w.Duration {
				continue // cooldown: already alerted for this window recently
			}
			d.lastAlert[ak] = now
			alerts = append(alerts, model.Alert{
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

	now := d.now()
	result := make(map[string]map[string]int)

	sec := now.Unix()
	for key, r := range d.rings {
		if result[key.EventType] == nil {
			result[key.EventType] = make(map[string]int)
		}
		for i, w := range d.windows {
			label := durationLabel(w.Duration)
			result[key.EventType][label] += r.count(sec, d.winSecs[i])
		}
	}
	return result
}

// StartGC runs a background goroutine that periodically removes (node, type)
// keys with no events recorded within the longest configured window, so that
// nodes which stop sending events (crash, uninstall, node_id rotation, etc.)
// do not leak entries in rings forever. It stops when ctx is done.
func (d *Detector) StartGC(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.gc()
			}
		}
	}()
}

// gc removes keys whose events are all older than the longest configured
// window (i.e. keys that have gone idle for at least the ring length).
func (d *Detector) gc() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.now()
	sec := now.Unix()
	for key, r := range d.rings {
		if sec-r.head >= int64(d.ringSize) {
			delete(d.rings, key)
		}
	}
	// Drop expired cooldown entries.
	for ak, last := range d.lastAlert {
		if now.Sub(last) >= d.windows[ak.window].Duration {
			delete(d.lastAlert, ak)
		}
	}
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
