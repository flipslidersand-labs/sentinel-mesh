package anomaly

import (
	"testing"
	"time"
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

// Characterization: alert fires on every event while the count stays above the
// threshold (not only on the crossing), and reports the in-window count.
func TestRecord_AlertsEveryEventWhileExceeding(t *testing.T) {
	d := New([]WindowConfig{{Duration: time.Minute, Threshold: 3}})
	ev := makeEvent("n", "exec")
	for i := 1; i <= 8; i++ {
		alerts := d.Record(ev)
		if want := i > 3; (len(alerts) == 1) != want {
			t.Fatalf("event %d: alerts=%d, want alert=%v", i, len(alerts), want)
		}
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
