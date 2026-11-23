package receiver

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/otel"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

var (
	countMPOnce sync.Once
	countMP     *otel.MetricsProvider
	countMPErr  error
)

// countTestProvider returns a metrics provider created once per test process
// (the counters live on the Prometheus default registry).
func countTestProvider(t *testing.T) *otel.MetricsProvider {
	t.Helper()
	countMPOnce.Do(func() { countMP, countMPErr = otel.NewMetricsProvider(nil) })
	if countMPErr != nil {
		t.Fatal(countMPErr)
	}
	return countMP
}

// eventsTotalFor returns sentinel_events_total summed over series of nodeID.
func eventsTotalFor(t *testing.T, nodeID string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() != "sentinel_events_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "node" && lp.GetValue() == nodeID {
					sum += m.GetCounter().GetValue()
				}
			}
		}
	}
	return sum
}

func countEvent(node, id string) *pb.KernelEvent {
	ev := execEvent(id)
	ev.NodeId = node
	return ev
}

func sendOne(t *testing.T, s *server, ev *pb.KernelEvent) bool {
	t.Helper()
	st := &fakeStream{events: []*pb.KernelEvent{ev}}
	if err := s.StreamEvents(st); err != nil {
		t.Fatal(err)
	}
	return st.acks[0].Ok
}

func windowCount(det *anomaly.Detector) int { return det.WindowStats()["exec"]["1m"] }

// #548: an event whose save fails N times and then succeeds is counted once
// (detector window and sentinel_events_total), not once per attempt; resends
// after the successful save are duplicates and are not counted either.
func TestStreamEvents_RetriedEventCountedOnce(t *testing.T) {
	const node = "node-548-a"
	fs := &fakeStorage{eventErrs: 3}
	s, det := newAnomalyServer(fs, &countNotifier{})
	s.metrics = countTestProvider(t)
	before := eventsTotalFor(t, node)

	for i := 0; i < 3; i++ {
		if sendOne(t, s, countEvent(node, "e1")) {
			t.Fatalf("attempt %d ack ok=true while the event save fails", i)
		}
		if got := windowCount(det); got != 0 {
			t.Fatalf("attempt %d: detector count = %d, want 0 (not saved yet)", i, got)
		}
		if got := eventsTotalFor(t, node) - before; got != 0 {
			t.Fatalf("attempt %d: events_total delta = %v, want 0", i, got)
		}
	}
	for i := 0; i < 3; i++ { // 1st succeeds, the rest are duplicates
		if !sendOne(t, s, countEvent(node, "e1")) {
			t.Fatalf("send %d ack ok=false after the store recovered", i)
		}
	}
	if got := windowCount(det); got != 1 {
		t.Fatalf("detector count = %d, want 1", got)
	}
	if got := eventsTotalFor(t, node) - before; got != 1 {
		t.Fatalf("events_total delta = %v, want 1", got)
	}
}

// #548 / #325: the save reports an error but the write landed, so the resend
// is a duplicate. That resend is the first successful delivery the collector
// sees and counts the event (once); later duplicates do not.
func TestStreamEvents_SavedDespiteErrorCountedOnResend(t *testing.T) {
	const node = "node-548-b"
	fs := &fakeStorage{eventErrsSaved: 1}
	s, det := newAnomalyServer(fs, &countNotifier{})
	s.metrics = countTestProvider(t)
	before := eventsTotalFor(t, node)

	if sendOne(t, s, countEvent(node, "e1")) {
		t.Fatal("first ack ok=true, want false")
	}
	if got := windowCount(det); got != 0 {
		t.Fatalf("detector count after failed ack = %d, want 0", got)
	}
	for i := 0; i < 3; i++ {
		if !sendOne(t, s, countEvent(node, "e1")) {
			t.Fatalf("resend %d ack ok=false", i)
		}
	}
	if len(fs.events) != 1 {
		t.Fatalf("events stored = %d, want 1", len(fs.events))
	}
	if got := windowCount(det); got != 1 {
		t.Fatalf("detector count = %d, want 1 (counted on the duplicate resend, once)", got)
	}
	if got := eventsTotalFor(t, node) - before; got != 1 {
		t.Fatalf("events_total delta = %v, want 1", got)
	}
}

// #548: the event save and the anomaly alert save both fail on the first
// delivery. The alert must end up saved (exactly once) after resends, and the
// event is counted once.
func TestStreamEvents_EventAndAlertSaveFailThenResend(t *testing.T) {
	fs := &fakeStorage{eventErrs: 1, alertErrs: 1}
	s, det := newAnomalyServer(fs, &countNotifier{})

	ok := false
	for i := 0; i < 6 && !ok; i++ {
		ok = sendOne(t, s, countEvent("node-548-c", "e1"))
	}
	if !ok {
		t.Fatal("no resend was acked ok")
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(fs.alerts))
	}
	if got := windowCount(det); got != 1 {
		t.Fatalf("detector count = %d, want 1", got)
	}
}

// #548: a pending anomaly alert is saved by the next resend of its event even
// when that resend is not a duplicate (its event save fails again) and without
// counting the event a second time.
func TestStreamEvents_PendingAnomalySavedOnNonDuplicateResend(t *testing.T) {
	fs := &fakeStorage{alertErrs: 1}
	s, det := newAnomalyServer(fs, &countNotifier{})

	if sendOne(t, s, countEvent("node-548-d", "e1")) {
		t.Fatal("first ack ok=true, want false (alert save fails)")
	}
	fs.eventErrs = 1 // the resend's event save fails: not a duplicate
	if sendOne(t, s, countEvent("node-548-d", "e1")) {
		t.Fatal("resend ack ok=true, want false (event save fails)")
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1 (pending alert saved by the resend)", len(fs.alerts))
	}
	if got := windowCount(det); got != 1 {
		t.Fatalf("detector count = %d, want 1", got)
	}
}

// An event without event_id cannot be matched to a pending alert: it must not
// pick up (or be skipped because of) another event's entry under the empty key.
func TestPendingAnomalyFor_EmptyEventIDNeverMatches(t *testing.T) {
	s := &server{}
	s.rememberAnomaly(model.Alert{NodeID: "n", EventID: ""})
	if _, ok := s.pendingAnomalyFor(model.Event{NodeID: "n"}); ok {
		t.Fatal("empty event_id matched a pending anomaly")
	}
}

// #548 review: an event already counted on its first delivery must not be
// counted again when a resend's save fails (busy) and the next resend is then
// reported as a duplicate. With and without a failed alert save (pending anomaly).
func TestStreamEvents_CountedEventNotRecountedAfterFailedResend(t *testing.T) {
	for _, tc := range []struct {
		name      string
		node      string
		alertErrs int
		wantAlert int
	}{
		{"alert saved", "node-548-e", 0, 1},
		{"alert save failed first", "node-548-f", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStorage{alertErrs: tc.alertErrs}
			s, det := newAnomalyServer(fs, &countNotifier{})
			s.metrics = countTestProvider(t)
			before := eventsTotalFor(t, tc.node)

			first := sendOne(t, s, countEvent(tc.node, "e1"))
			if first != (tc.alertErrs == 0) {
				t.Fatalf("first ack ok=%v", first)
			}
			fs.eventErrs = 1 // the resend's event save fails
			if sendOne(t, s, countEvent(tc.node, "e1")) {
				t.Fatal("second ack ok=true while the event save fails")
			}
			for i := 0; i < 2; i++ { // duplicate resends: saved before, counted before
				if !sendOne(t, s, countEvent(tc.node, "e1")) {
					t.Fatalf("resend %d ack ok=false", i)
				}
			}
			if got := windowCount(det); got != 1 {
				t.Fatalf("detector count = %d, want 1", got)
			}
			if got := eventsTotalFor(t, tc.node) - before; got != 1 {
				t.Fatalf("events_total delta = %v, want 1", got)
			}
			if len(fs.alerts) != tc.wantAlert {
				t.Fatalf("alerts = %d, want %d", len(fs.alerts), tc.wantAlert)
			}
		})
	}
}

// The count-state set is bounded: at the cap the oldest entry is dropped
// (an evicted uncounted event is then not counted late), newer ones stay.
func TestCountState_EvictsOldestFirst(t *testing.T) {
	s := &server{maxPending: 2}
	ev := func(id string) model.Event { return model.Event{NodeID: "n", EventID: id} }
	s.noteSaveFailed(ev("a"))
	s.noteSaveFailed(ev("b"))
	s.noteSaveFailed(ev("c"))
	if s.claimCount(ev("a"), true) {
		t.Fatal("oldest entry survived the cap")
	}
	if !s.claimCount(ev("b"), true) || !s.claimCount(ev("c"), true) {
		t.Fatal("newer entries were evicted")
	}
	if s.claimCount(ev("b"), true) {
		t.Fatal("claim did not mark the event counted")
	}
}
