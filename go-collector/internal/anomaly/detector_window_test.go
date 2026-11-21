package anomaly

import (
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

// recordN records n events for key at the detector's current (fake) time.
func recordN(d *Detector, key windowKey, n int) {
	ev := makeEvent(key.NodeID, key.EventType)
	for i := 0; i < n; i++ {
		d.Record(ev)
	}
}

// Window boundary at 1-second granularity: an N-second window covers the current
// second and the N-1 before it. An event recorded 59s ago is inside a 60s
// window; one recorded 60s ago is outside. (Before #469 the 60s mark was
// inclusive at nanosecond precision.)
func TestRing_WindowBoundary(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1000}})
	key := windowKey{"n", "exec"}
	recordN(d, key, 1) // t0
	c.advance(59 * time.Second)
	if got := d.rings[key].count(c.now().Unix(), 60); got != 1 {
		t.Errorf("count at +59s = %d, want 1", got)
	}
	c.advance(time.Second)
	if got := d.rings[key].count(c.now().Unix(), 60); got != 0 {
		t.Errorf("count at +60s = %d, want 0", got)
	}
	// Sub-second offsets within the same second are the same bucket.
	d2, c2 := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1000}})
	recordN(d2, key, 1)
	c2.advance(900 * time.Millisecond)
	recordN(d2, key, 1)
	if got := d2.rings[key].count(c2.now().Unix(), 60); got != 2 {
		t.Errorf("same-second count = %d, want 2", got)
	}
}

// Memory per key is bounded by the ring length, independent of event rate.
func TestRing_BoundedMemory(t *testing.T) {
	d, c := newClocked(DefaultWindows)
	key := windowKey{"n", "exec"}
	for i := 0; i < 100_000; i++ {
		d.Record(makeEvent("n", "exec"))
		if i%1000 == 0 {
			c.advance(time.Second)
		}
	}
	r := d.rings[key]
	if len(r.buckets) != 300 || cap(r.buckets) != 300 {
		t.Fatalf("ring len/cap = %d/%d, want 300/300", len(r.buckets), cap(r.buckets))
	}
	if d.ringSize != 300 {
		t.Fatalf("ringSize = %d, want 300 (longest window)", d.ringSize)
	}
}

// A key idle for longer than the ring must not resurrect old counts when the
// ring wraps onto the same bucket indices.
func TestRing_IdleLongerThanRingResets(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1000}})
	key := windowKey{"n", "exec"}
	recordN(d, key, 50)
	c.advance(10 * time.Minute) // > ring length, lands on arbitrary index
	recordN(d, key, 1)
	if got := d.rings[key].count(c.now().Unix(), 60); got != 1 {
		t.Errorf("count after long idle = %d, want 1", got)
	}
	// Exactly one ring length later: same bucket index as the old events.
	c.advance(60 * time.Second)
	recordN(d, key, 1)
	if got := d.rings[key].count(c.now().Unix(), 60); got != 1 {
		t.Errorf("count after exact wrap = %d, want 1", got)
	}
	// Partial gap: buckets skipped over are cleared.
	c.advance(30 * time.Second)
	recordN(d, key, 1)
	if got := d.rings[key].count(c.now().Unix(), 60); got != 2 {
		t.Errorf("count after partial gap = %d, want 2", got)
	}
}

// Timestamps older than the newest recorded second (clock stepped back) are not
// dropped: they are counted in the current bucket.
func TestRing_OlderTimestampCountedInCurrentBucket(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 1000}})
	key := windowKey{"n", "exec"}
	recordN(d, key, 1)
	c.advance(10 * time.Second)
	recordN(d, key, 1)
	c.advance(-5 * time.Second)
	recordN(d, key, 1)
	r := d.rings[key]
	if r.head != c.now().Unix()+5 {
		t.Errorf("head moved backwards: %d", r.head)
	}
	if got := r.count(r.head, 60); got != 3 {
		t.Errorf("count = %d, want 3", got)
	}
	if got := r.buckets[r.idx(r.head)]; got != 2 {
		t.Errorf("head bucket = %d, want 2", got)
	}
}

// Multiple windows: old events count only toward the wider window.
func TestRing_MultipleWindows(t *testing.T) {
	d, c := newClocked([]WindowConfig{
		{Duration: time.Minute, Threshold: 1000},
		{Duration: 5 * time.Minute, Threshold: 1000},
	})
	key := windowKey{"n", "exec"}
	recordN(d, key, 4)
	c.advance(2 * time.Minute)
	recordN(d, key, 1)
	sec := c.now().Unix()
	if got := d.rings[key].count(sec, d.winSecs[0]); got != 1 {
		t.Errorf("1m count = %d, want 1", got)
	}
	if got := d.rings[key].count(sec, d.winSecs[1]); got != 5 {
		t.Errorf("5m count = %d, want 5", got)
	}
	st := d.WindowStats()["exec"]
	if st["1m"] != 1 || st["5m"] != 5 {
		t.Errorf("WindowStats = %v, want 1m=1 5m=5", st)
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
	for _, ev := range []model.Event{
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

// Old events count only for the wider window; events past the longest window are gone.
func TestRecord_MultipleWindowsOldEvents(t *testing.T) {
	ws := []WindowConfig{
		{Duration: time.Minute, Threshold: 3},
		{Duration: 5 * time.Minute, Threshold: 6},
	}
	key := windowKey{"n", "exec"}
	ev := makeEvent("n", "exec")

	// 6 events 2 minutes ago: outside the 1m window, inside 5m.
	d, c := newClocked(ws)
	recordN(d, key, 6)
	c.advance(2 * time.Minute)
	if alerts := d.Record(ev); len(alerts) != 1 { // 5m count = 7 > 6
		t.Fatalf("expected 5m-window alert, got %d", len(alerts))
	}

	// 5 events 2 minutes ago + new one: 5m count 6, not > 6; 1m count 1.
	d, c = newClocked(ws)
	recordN(d, key, 5)
	c.advance(2 * time.Minute)
	if alerts := d.Record(ev); len(alerts) != 0 {
		t.Fatalf("expected no alert, got %d", len(alerts))
	}

	// Events older than the longest window no longer count.
	d, c = newClocked(ws)
	recordN(d, key, 10)
	c.advance(10 * time.Minute)
	if alerts := d.Record(ev); len(alerts) != 0 {
		t.Fatalf("expected no alert for expired events, got %d", len(alerts))
	}
	if got := d.rings[key].count(c.now().Unix(), d.winSecs[1]); got != 1 {
		t.Errorf("count = %d, want 1 after expiry", got)
	}
}

const benchN = 600_000

func benchRecord(b *testing.B, windows []WindowConfig) {
	d, _ := newClocked(windows)
	key := windowKey{"n", "exec"}
	recordN(d, key, benchN)
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

// Clock jumps forward then back: events recorded after the step back must still
// be visible to detection (Record alerts) and to WindowStats.
func TestRecord_ClockStepBackStillDetects(t *testing.T) {
	d, c := newClocked([]WindowConfig{{Duration: time.Minute, Threshold: 5}})
	ev := makeEvent("n", "exec")
	d.Record(ev)
	c.advance(time.Hour)
	d.Record(ev)
	c.advance(-time.Hour) // wall clock steps back
	alerts := 0
	for i := 0; i < 20; i++ {
		alerts += len(d.Record(ev))
	}
	if alerts == 0 {
		t.Fatal("no alert after clock stepped back; events were recorded but not counted")
	}
	if got := d.WindowStats()["exec"]["1m"]; got < 20 {
		t.Errorf("WindowStats 1m = %d, want >= 20 after clock step back", got)
	}
}
