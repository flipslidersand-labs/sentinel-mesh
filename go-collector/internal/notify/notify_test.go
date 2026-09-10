package notify

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// fakeNotifier records the alerts it receives.
type fakeNotifier struct {
	mu   sync.Mutex
	got  []store.Alert
	fail bool
}

func (f *fakeNotifier) Name() string { return "fake" }

func (f *fakeNotifier) Send(_ context.Context, a store.Alert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, a)
	if f.fail {
		return context.DeadlineExceeded
	}
	return nil
}

func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func alert(rule, node, sev string) store.Alert {
	return store.Alert{
		AlertID:   "a-" + rule,
		RuleID:    rule,
		NodeID:    node,
		EventID:   "e-1",
		Timestamp: time.Unix(0, 0).UTC(),
		Message:   "test",
		Severity:  sev,
	}
}

func TestDispatch_SeverityFilter(t *testing.T) {
	f := &fakeNotifier{}
	d := NewDispatcher(Options{Notifiers: []Notifier{f}, MinSeverity: "warning"})

	d.Dispatch(alert("r1", "n1", "info")) // below floor → dropped
	if f.count() != 0 {
		t.Fatalf("info alert should be dropped, got %d sends", f.count())
	}

	d.Dispatch(alert("r2", "n1", "warning"))  // at floor → sent
	d.Dispatch(alert("r3", "n1", "critical")) // above floor → sent
	if f.count() != 2 {
		t.Fatalf("expected 2 sends, got %d", f.count())
	}
}

func TestDispatch_Cooldown(t *testing.T) {
	f := &fakeNotifier{}
	now := time.Unix(1000, 0)
	d := NewDispatcher(Options{
		Notifiers:   []Notifier{f},
		Cooldown:    time.Minute,
		MinSeverity: "info",
	})
	d.now = func() time.Time { return now }

	d.Dispatch(alert("r1", "n1", "warning")) // first → sent
	d.Dispatch(alert("r1", "n1", "warning")) // within cooldown → suppressed
	if f.count() != 1 {
		t.Fatalf("cooldown should suppress duplicate, got %d", f.count())
	}

	// Same rule, different node is a distinct key → sent.
	d.Dispatch(alert("r1", "n2", "warning"))
	if f.count() != 2 {
		t.Fatalf("different node should not be suppressed, got %d", f.count())
	}

	// Advance past the cooldown → sent again.
	now = now.Add(2 * time.Minute)
	d.Dispatch(alert("r1", "n1", "warning"))
	if f.count() != 3 {
		t.Fatalf("post-cooldown alert should send, got %d", f.count())
	}
}

func TestDispatch_NoCooldownAlwaysSends(t *testing.T) {
	f := &fakeNotifier{}
	d := NewDispatcher(Options{Notifiers: []Notifier{f}, MinSeverity: "info"})
	for i := 0; i < 3; i++ {
		d.Dispatch(alert("r1", "n1", "warning"))
	}
	if f.count() != 3 {
		t.Fatalf("expected 3 sends with no cooldown, got %d", f.count())
	}
}

func TestDispatch_FanOutAndErrorIsolation(t *testing.T) {
	ok := &fakeNotifier{}
	bad := &fakeNotifier{fail: true}
	// bad is listed first; its failure must not stop ok from receiving.
	d := NewDispatcher(Options{Notifiers: []Notifier{bad, ok}, MinSeverity: "info"})

	d.Dispatch(alert("r1", "n1", "critical"))
	if bad.count() != 1 || ok.count() != 1 {
		t.Fatalf("both notifiers should be attempted: bad=%d ok=%d", bad.count(), ok.count())
	}
}

func TestEnabled(t *testing.T) {
	if NewDispatcher(Options{}).Enabled() {
		t.Fatal("dispatcher with no notifiers should be disabled")
	}
	if !NewDispatcher(Options{Notifiers: []Notifier{&fakeNotifier{}}}).Enabled() {
		t.Fatal("dispatcher with a notifier should be enabled")
	}
	var nilD *Dispatcher
	if nilD.Enabled() {
		t.Fatal("nil dispatcher should be disabled")
	}
}

// blockingNotifier's Send blocks until release is closed, letting a test
// saturate DispatchAsync's concurrency limit deterministically.
type blockingNotifier struct {
	mu      sync.Mutex
	started int
	release chan struct{}
}

func (b *blockingNotifier) Name() string { return "blocking" }

func (b *blockingNotifier) Send(_ context.Context, _ store.Alert) error {
	b.mu.Lock()
	b.started++
	b.mu.Unlock()
	<-b.release
	return nil
}

func (b *blockingNotifier) startedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started
}

// TestDispatchAsync_BoundedConcurrency verifies that once maxConcurrentDispatch
// Dispatch calls are in flight, further DispatchAsync calls are dropped
// (not queued, not spawning an unbounded goroutine) (#75).
func TestDispatchAsync_BoundedConcurrency(t *testing.T) {
	n := &blockingNotifier{release: make(chan struct{})}
	d := NewDispatcher(Options{Notifiers: []Notifier{n}, MinSeverity: "info"})

	// Saturate the concurrency limit.
	for i := 0; i < maxConcurrentDispatch; i++ {
		d.DispatchAsync(alert("r", "n", "critical"))
	}
	deadline := time.Now().Add(2 * time.Second)
	for n.startedCount() < maxConcurrentDispatch && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := n.startedCount(); got != maxConcurrentDispatch {
		t.Fatalf("expected %d in-flight sends, got %d", maxConcurrentDispatch, got)
	}

	// One more beyond the limit must be dropped, not block or queue.
	d.DispatchAsync(alert("r", "n", "critical"))
	time.Sleep(20 * time.Millisecond) // let it settle if it were (incorrectly) processed

	d.mu.Lock()
	dropped := d.dropped
	d.mu.Unlock()
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if got := n.startedCount(); got != maxConcurrentDispatch {
		t.Fatalf("extra alert should not have started a send: started = %d, want %d", got, maxConcurrentDispatch)
	}

	close(n.release) // unblock everything so the test can exit cleanly
}

func TestRankOf(t *testing.T) {
	cases := map[string]int{
		"info": 0, "INFO": 0, " warning ": 1, "critical": 2,
		"unknown": 0, "": 0,
	}
	for in, want := range cases {
		if got := rankOf(in); got != want {
			t.Errorf("rankOf(%q) = %d, want %d", in, got, want)
		}
	}
}
