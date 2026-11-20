package anomaly

import (
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// seed fills the detector's history for key with n timestamps evenly spread over
// the span ending 'ago' before now (oldest first).
func seed(d *Detector, key windowKey, n int, span, ago time.Duration) {
	now := time.Now().Add(-ago)
	ts := make([]time.Time, n)
	for i := range ts {
		ts[i] = now.Add(-span + span*time.Duration(i)/time.Duration(n))
	}
	d.timestamps[key] = ts
}

// Characterization: countWithin counts entries at or after now-dur (inclusive boundary).
func TestCountWithin_Boundary(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 1000}})
	key := windowKey{"n", "exec"}
	now := time.Now()
	d.timestamps[key] = []time.Time{
		now.Add(-90 * time.Second),
		now.Add(-61 * time.Second),
		now.Add(-time.Minute), // exactly on the cutoff: counted
		now.Add(-30 * time.Second),
		now,
	}
	if got := d.countWithin(key, now, time.Minute); got != 3 {
		t.Errorf("countWithin = %d, want 3", got)
	}
	if got := d.countWithin(key, now, time.Hour); got != 5 {
		t.Errorf("countWithin(1h) = %d, want 5", got)
	}
	if got := d.countWithin(key, now, 0); got != 1 {
		t.Errorf("countWithin(0) = %d, want 1", got)
	}
	if got := d.countWithin(windowKey{"x", "y"}, now, time.Minute); got != 0 {
		t.Errorf("countWithin(missing key) = %d, want 0", got)
	}
}

// Spec (#422): alert fires on the crossing, then is suppressed for the rest of
// the window (cooldown) even though the count stays above the threshold.
func TestRecord_AlertOncePerWindowWhileExceeding(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 3}})
	ev := makeEvent("n", "exec")
	for i := 1; i <= 8; i++ {
		alerts := d.Record(ev)
		if want := i == 4; (len(alerts) == 1) != want {
			t.Fatalf("event %d: alerts=%d, want alert=%v", i, len(alerts), want)
		}
	}
}

// fakeClock is an adjustable time source for Detector.now.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newClocked(w []WindowConfig) (*Detector, *fakeClock) {
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	d := New(w)
	d.now = c.now
	return d, c
}

func TestRecord_CooldownThenRefire(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 2}})
	ev := makeEvent("n", "exec")
	n := 0
	for i := 0; i < 3; i++ { // 3rd event crosses
		n += len(d.Record(ev))
		c.advance(time.Second)
	}
	if n != 1 {
		t.Fatalf("alerts after crossing = %d, want 1", n)
	}
	for i := 0; i < 10; i++ { // still in cooldown
		if a := d.Record(ev); len(a) != 0 {
			t.Fatalf("alert during cooldown (i=%d)", i)
		}
		c.advance(time.Second)
	}
	c.advance(time.Minute) // cooldown elapsed; old events out of window
	for i := 0; i < 2; i++ {
		d.Record(ev)
		c.advance(time.Second)
	}
	if a := d.Record(ev); len(a) != 1 {
		t.Fatalf("alerts after cooldown = %d, want 1", len(a))
	}
}

func TestRecord_CooldownIndependentPerKey(t *testing.T) {
	d, _ := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1}})
	for _, ev := range []store.Event{
		makeEvent("n1", "exec"), makeEvent("n2", "exec"), makeEvent("n1", "tcp"),
	} {
		d.Record(ev)
		if a := d.Record(ev); len(a) != 1 {
			t.Fatalf("%s/%s: alerts=%d, want 1", ev.NodeID, ev.Type, len(a))
		}
		if a := d.Record(ev); len(a) != 0 {
			t.Fatalf("%s/%s: alert during cooldown", ev.NodeID, ev.Type)
		}
	}
}

func TestRecord_CooldownPerWindow(t *testing.T) {
	d, c := newClocked([]WindowConfig{
		{Duration: time.Minute, Threshold: 2},
		{Duration: 5 * time.Minute, Threshold: 4},
	})
	ev := makeEvent("n", "exec")
	total := 0
	for i := 0; i < 5; i++ { // 1m fires at #3; 5m crosses at #5 (1m in cooldown)
		total += len(d.Record(ev))
		c.advance(time.Second)
	}
	if total != 2 {
		t.Fatalf("alerts = %d, want 2 (one per window)", total)
	}
}

func TestGC_RemovesExpiredCooldown(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1}})
	ev := makeEvent("n", "exec")
	d.Record(ev)
	d.Record(ev)
	if len(d.lastAlert) != 1 {
		t.Fatalf("lastAlert len = %d, want 1", len(d.lastAlert))
	}
	d.gc()
	if len(d.lastAlert) != 1 {
		t.Fatal("active cooldown must be kept by gc")
	}
	c.advance(2 * time.Minute)
	d.gc()
	if len(d.lastAlert) != 0 {
		t.Fatalf("lastAlert len = %d, want 0 after expiry", len(d.lastAlert))
	}
}

// Characterization: multiple windows; old events count only for the wider window.
func TestRecord_MultipleWindowsOldEvents(t *testing.T) {
	d := New([]WindowConfig{
		{Duration: time.Minute, Threshold: 3},
		{Duration: 5 * time.Minute, Threshold: 6},
	})
	key := windowKey{"n", "exec"}
	ev := makeEvent("n", "exec")

	// 6 events 2-3 minutes ago: outside the 1m window, inside 5m.
	seed(d, key, 6, time.Minute, 2*time.Minute)
	if alerts := d.Record(ev); len(alerts) != 1 { // 5m count = 7 > 6
		t.Fatalf("expected 5m-window alert, got %d", len(alerts))
	}

	// 5 events 2-3 minutes ago + new one: 5m count 6, not > 6; 1m count 1.
	d = New(d.windows)
	seed(d, key, 5, time.Minute, 2*time.Minute)
	if alerts := d.Record(ev); len(alerts) != 0 {
		t.Fatalf("expected no alert, got %d", len(alerts))
	}

	// Events older than the longest window are pruned.
	d = New(d.windows)
	seed(d, key, 10, time.Minute, 10*time.Minute)
	if alerts := d.Record(ev); len(alerts) != 0 {
		t.Fatalf("expected no alert for expired events, got %d", len(alerts))
	}
	if n := len(d.timestamps[key]); n != 1 {
		t.Errorf("history len = %d, want 1 after prune", n)
	}
}

const benchN = 600_000

func benchRecord(b *testing.B, windows []WindowConfig) {
	d := New(windows)
	key := windowKey{"n", "exec"}
	seed(d, key, benchN, 4*time.Minute, 0)
	ev := makeEvent("n", "exec")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Record(ev)
	}
}

// 600k events held for one key, thresholds never reached (pure counting cost).
func BenchmarkRecord_600k_NoAlert(b *testing.B) {
	benchRecord(b, []WindowConfig{
		{Duration: time.Minute, Threshold: 1 << 30},
		{Duration: 5 * time.Minute, Threshold: 1 << 30},
	})
}

// 600k events, threshold exceeded on every call.
func BenchmarkRecord_600k_Exceeding(b *testing.B) {
	benchRecord(b, DefaultWindows)
}
