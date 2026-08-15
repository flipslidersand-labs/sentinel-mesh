// Package notify dispatches triggered alerts to external channels (Slack, email).
//
// Secrets (webhook URLs, SMTP credentials) are read from environment variables,
// never from the rules YAML or CLI flags, so they never land in version control
// or process listings. A per-(rule, node) cooldown suppresses duplicate spam,
// and a minimum-severity filter keeps low-signal alerts off the channels.
package notify

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

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

// severityRank orders severities so a minimum-severity filter can be applied.
// Unknown severities rank as 0 (lowest) so they are only sent when the
// threshold is also at its lowest.
var severityRank = map[string]int{
	"info":     0,
	"low":      0,
	"warning":  1,
	"warn":     1,
	"medium":   1,
	"high":     2,
	"critical": 2,
	"error":    2,
}

func rankOf(severity string) int {
	return severityRank[strings.ToLower(strings.TrimSpace(severity))]
}

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
	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// Options configures a Dispatcher.
type Options struct {
	Notifiers   []Notifier
	Cooldown    time.Duration // minimum gap between notifications for the same (rule, node)
	MinSeverity string        // alerts below this severity are dropped (default "warning")
	SendTimeout time.Duration // per-notifier send timeout (default 10s)
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
		minSev = "warning"
	}
	timeout := opts.SendTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Dispatcher{
		notifiers: opts.Notifiers,
		cooldown:  opts.Cooldown,
		minRank:   rankOf(minSev),
		timeout:   timeout,
		log:       log,
		lastSent:  make(map[string]time.Time),
		now:       time.Now,
	}
}

// Enabled reports whether any notifier is configured.
func (d *Dispatcher) Enabled() bool {
	return d != nil && len(d.notifiers) > 0
}

// shouldSend applies the severity floor and cooldown. When it returns true it
// records the send time so subsequent calls within the cooldown are suppressed.
func (d *Dispatcher) shouldSend(a store.Alert) bool {
	if rankOf(a.Severity) < d.minRank {
		return false
	}
	if d.cooldown <= 0 {
		return true
	}
	key := a.RuleID + "\x00" + a.NodeID
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()
	if last, ok := d.lastSent[key]; ok && now.Sub(last) < d.cooldown {
		return false
	}
	d.lastSent[key] = now
	return true
}

// Dispatch delivers the alert to every notifier if it passes the severity and
// cooldown gates. It blocks until all notifiers have been attempted; callers
// that must not block (e.g. the gRPC hot path) should invoke it in a goroutine.
func (d *Dispatcher) Dispatch(a store.Alert) {
	if !d.Enabled() {
		return
	}
	if !d.shouldSend(a) {
		d.log.Debug("alert notification suppressed",
			zap.String("rule_id", a.RuleID),
			zap.String("node_id", a.NodeID),
			zap.String("severity", a.Severity),
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	for _, n := range d.notifiers {
		if err := n.Send(ctx, a); err != nil {
			d.log.Error("alert notification failed",
				zap.String("channel", n.Name()),
				zap.String("rule_id", a.RuleID),
				zap.Error(err),
			)
			continue
		}
		d.log.Info("alert notification sent",
			zap.String("channel", n.Name()),
			zap.String("rule_id", a.RuleID),
			zap.String("node_id", a.NodeID),
			zap.String("severity", a.Severity),
		)
	}
}
