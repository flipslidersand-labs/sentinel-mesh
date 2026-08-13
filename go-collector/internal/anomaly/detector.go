// Package anomaly provides statistical spike detection over event streams.
// It maintains per-(node, event_type) sliding windows of 1-minute buckets
// and fires an alert when the current rate exceeds mean + k*stddev of history.
package anomaly

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

const (
	bucketDuration = time.Minute
	historyLen     = 10  // number of past buckets used for baseline
	sigmaK         = 3.0 // alert threshold: mean + k*stddev
	minBaseline    = 5   // minimum total events in history to start alerting
)

// key identifies a unique (node, event_type) stream.
type key struct {
	node      string
	eventType string
}

// window holds a circular buffer of per-minute event counts.
type window struct {
	counts  [historyLen]int64
	current int64     // accumulator for the in-progress bucket
	head    int       // index of the oldest bucket
	filled  int       // how many buckets are valid (0..historyLen)
	lastFlush time.Time
}

func (w *window) add(n int64) {
	w.current += n
}

// flush finalises the current bucket and rotates the ring.
func (w *window) flush() int64 {
	completed := w.current
	w.counts[w.head] = completed
	w.head = (w.head + 1) % historyLen
	if w.filled < historyLen {
		w.filled++
	}
	w.current = 0
	return completed
}

// stats returns (mean, stddev, total) of the stored history buckets.
func (w *window) stats() (mean, stddev float64, total int64) {
	if w.filled == 0 {
		return 0, 0, 0
	}
	for i := 0; i < w.filled; i++ {
		total += w.counts[i]
	}
	mean = float64(total) / float64(w.filled)
	var variance float64
	for i := 0; i < w.filled; i++ {
		d := float64(w.counts[i]) - mean
		variance += d * d
	}
	if w.filled > 1 {
		stddev = math.Sqrt(variance / float64(w.filled-1))
	}
	return mean, stddev, total
}

// WindowStat is returned by Stats for the /api/stats/windows endpoint.
type WindowStat struct {
	Node      string  `json:"node"`
	EventType string  `json:"event_type"`
	Current   int64   `json:"current_count"`
	Mean      float64 `json:"mean_per_min"`
	Stddev    float64 `json:"stddev"`
	Threshold float64 `json:"threshold"`
}

// Detector is a stateful anomaly detector.
type Detector struct {
	mu      sync.Mutex
	windows map[key]*window
	st      *store.Store
	log     *zap.Logger
}

// New creates a Detector backed by the given store.
func New(st *store.Store, log *zap.Logger) *Detector {
	return &Detector{
		windows: make(map[key]*window),
		st:      st,
		log:     log,
	}
}

// Feed records one event for the given node and event type.
func (d *Detector) Feed(node, eventType string) {
	d.mu.Lock()
	k := key{node, eventType}
	w, ok := d.windows[k]
	if !ok {
		w = &window{lastFlush: time.Now().Truncate(bucketDuration)}
		d.windows[k] = w
	}
	w.add(1)
	d.mu.Unlock()
}

// Run starts the background evaluation loop, flushing buckets every minute.
func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(bucketDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.evaluate()
		}
	}
}

func (d *Detector) evaluate() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for k, w := range d.windows {
		mean, stddev, total := w.stats()
		completed := w.flush()

		if total < int64(minBaseline) {
			// Not enough history to establish a baseline.
			continue
		}

		threshold := mean + sigmaK*stddev
		if stddev == 0 {
			// No variance yet — use 2× mean as a simple heuristic.
			threshold = mean * 2
		}

		if float64(completed) > threshold && threshold > 0 {
			alert := store.Alert{
				AlertID:   uuid.New().String(),
				NodeID:    k.node,
				RuleID:    "anomaly:spike",
				Severity:  "warning",
				Message: fmt.Sprintf(
					"event spike detected: %s/%s — %d events/min (threshold %.1f, mean %.1f, σ %.1f)",
					k.node, k.eventType, completed, threshold, mean, stddev,
				),
				Timestamp: time.Now().UTC(),
			}
			if err := d.st.SaveAlert(alert); err != nil {
				d.log.Warn("anomaly: failed to save alert", zap.Error(err))
			} else {
				d.log.Info("anomaly alert",
					zap.String("node", k.node),
					zap.String("type", k.eventType),
					zap.Int64("count", completed),
					zap.Float64("threshold", threshold),
				)
			}
		}
	}
}

// Stats returns current window statistics for all tracked streams.
func (d *Detector) Stats() []WindowStat {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]WindowStat, 0, len(d.windows))
	for k, w := range d.windows {
		mean, stddev, _ := w.stats()
		threshold := mean + sigmaK*stddev
		if stddev == 0 {
			threshold = mean * 2
		}
		out = append(out, WindowStat{
			Node:      k.node,
			EventType: k.eventType,
			Current:   w.current,
			Mean:      math.Round(mean*100) / 100,
			Stddev:    math.Round(stddev*100) / 100,
			Threshold: math.Round(threshold*100) / 100,
		})
	}
	return out
}
