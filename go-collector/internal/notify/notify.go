// Package notify dispatches triggered alerts to external channels (Slack, email).
//
// Secrets (webhook URLs, SMTP credentials) are read from environment variables,
// never from the rules YAML or CLI flags, so they never land in version control
// or process listings. A per-(rule, node) cooldown suppresses duplicate spam,
// and a minimum-severity filter keeps low-signal alerts off the channels.
package notify

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Notifier delivers a single alert to one external channel.
type Notifier interface {
	// Name identifies the channel for logging (e.g. "slack", "email").
	Name() string
	// Send delivers the alert. It should return an error on failure so the
	// dispatcher can log it; it must not panic.
	Send(ctx context.Context, a store.Alert) error
}

// knownSeverity reports whether s is a canonical severity (alerting.Severity*).
func knownSeverity(s string) bool {
	_, ok := alerting.SeverityRank(s)
	return ok
}

// rankOf orders severities via the alerting vocabulary. An unknown alert
// severity ranks as 0 (lowest) so it is only sent when the threshold is also
// at its lowest; unknown minimum severities are rejected at startup by
// DispatcherFromEnv.
func rankOf(severity string) int {
	r, _ := alerting.SeverityRank(severity)
	return r
}

// maxConcurrentDispatch bounds how many Dispatch goroutines DispatchAsync
// will run at once. Without a bound, a flood of alerts (e.g. from a
// misbehaving or malicious agent) spawning one goroutine per alert could
// exhaust the collector's goroutines/memory (#75).
const maxConcurrentDispatch = 32

// Dispatcher fans an alert out to all configured notifiers, applying a
// severity floor and a per-(rule, node) cooldown. It is safe for concurrent use.
type Dispatcher struct {
	notifiers []Notifier
	cooldown  time.Duration
	minRank   int
	timeout   time.Duration
	log       *zap.Logger

	mu       sync.Mutex
	lastSent map[string]time.Time
	// lastSweep is when expired lastSent keys were last pruned (#319).
	lastSweep time.Time
	dropped   uint64
	// now is overridable in tests; defaults to time.Now.
	now func() time.Time

	// sem bounds concurrent DispatchAsync goroutines (#75).
	sem chan struct{}
}

// defaultSendTimeout is the per-notifier send timeout.
const defaultSendTimeout = 10 * time.Second

// Options configures a Dispatcher.
type Options struct {
	Notifiers   []Notifier
	Cooldown    time.Duration // minimum gap between notifications for the same (rule, node)
	MinSeverity string        // alerts below this severity are dropped (default "warning")
	Logger      *zap.Logger
}

// NewDispatcher builds a Dispatcher from Options.
func NewDispatcher(opts Options) *Dispatcher {
	log := opts.Logger
	if log == nil {
		log = zap.NewNop()
	}
	minSev := opts.MinSeverity
	if minSev == "" {
		minSev = alerting.SeverityWarning
	}
	return &Dispatcher{
		notifiers: opts.Notifiers,
		cooldown:  opts.Cooldown,
		minRank:   rankOf(minSev),
		timeout:   defaultSendTimeout,
		log:       log,
		lastSent:  make(map[string]time.Time),
		now:       time.Now,
		sem:       make(chan struct{}, maxConcurrentDispatch),
	}
}

// Enabled reports whether any notifier is configured.
func (d *Dispatcher) Enabled() bool {
	return d != nil && len(d.notifiers) > 0
}

// shouldSend applies the severity floor and cooldown. When it returns true it
// claims the key (records the send time) so subsequent calls within the
// cooldown are suppressed, including concurrent in-flight Dispatch calls. The
// returned release func (never nil) un-claims the key; Dispatch calls it when
// every notifier failed so the alert can be retried right away (#319).
// Keys older than the cooldown are pruned at most once per cooldown interval,
// bounding the map for throwaway node IDs / anomaly_<type> rule IDs.
func (d *Dispatcher) shouldSend(a store.Alert) (bool, func()) {
	noop := func() {}
	if rankOf(a.Severity) < d.minRank {
		return false, noop
	}
	if d.cooldown <= 0 {
		return true, noop
	}
	key := a.RuleID + "\x00" + a.NodeID
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()
	if now.Sub(d.lastSweep) >= d.cooldown {
		for k, t := range d.lastSent {
			if now.Sub(t) >= d.cooldown {
				delete(d.lastSent, k)
			}
		}
		d.lastSweep = now
	}
	if last, ok := d.lastSent[key]; ok && now.Sub(last) < d.cooldown {
		return false, noop
	}
	d.lastSent[key] = now
	release := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		// Only roll back our own claim; a newer claim for the key stays.
		if t, ok := d.lastSent[key]; ok && t.Equal(now) {
			delete(d.lastSent, key)
		}
	}
	return true, release
}

// DispatchAsync delivers the alert asynchronously, bounded by
// maxConcurrentDispatch. If that many Dispatch calls are already in flight,
// the alert notification is dropped (logged + counted, not silently lost)
// rather than spawning an unbounded goroutine (#75). Safe to call from a
// hot path (e.g. the gRPC StreamEvents loop) — it never blocks.
func (d *Dispatcher) DispatchAsync(a store.Alert) {
	if !d.Enabled() {
		return
	}
	select {
	case d.sem <- struct{}{}:
		go func() {
			defer func() { <-d.sem }()
			d.Dispatch(a)
		}()
	default:
		d.mu.Lock()
		d.dropped++
		n := d.dropped
		d.mu.Unlock()
		if n == 1 || n%100 == 0 {
			d.log.Warn("alert dispatch concurrency limit reached, dropping notification",
				zap.String("rule_id", a.RuleID),
				zap.String("node_id", a.NodeID),
				zap.Uint64("dropped_total", n),
			)
		}
	}
}

// Dispatch delivers the alert to every notifier if it passes the severity and
// cooldown gates. It blocks until all notifiers have been attempted; callers
// that must not block (e.g. the gRPC hot path) should use DispatchAsync instead.
func (d *Dispatcher) Dispatch(a store.Alert) {
	if !d.Enabled() {
		return
	}
	ok, release := d.shouldSend(a)
	if !ok {
		d.log.Debug("alert notification suppressed",
			zap.String("rule_id", a.RuleID),
			zap.String("node_id", a.NodeID),
			zap.String("severity", a.Severity),
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	// The claim stays recorded during Send (d.mu is not held) so concurrent
	// Dispatch calls for the same key stay suppressed and no extra duplicate
	// window opens; it is released only after every notifier has failed.
	delivered := false
	defer func() {
		if !delivered {
			release()
		}
	}()
	for _, n := range d.notifiers {
		if err := n.Send(ctx, a); err != nil {
			d.log.Error("alert notification failed",
				zap.String("channel", n.Name()),
				zap.String("rule_id", a.RuleID),
				zap.Error(err),
			)
			continue
		}
		delivered = true
		d.log.Info("alert notification sent",
			zap.String("channel", n.Name()),
			zap.String("rule_id", a.RuleID),
			zap.String("node_id", a.NodeID),
			zap.String("severity", a.Severity),
		)
	}
}
