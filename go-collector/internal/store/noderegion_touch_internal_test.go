package store

import (
	"context"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// fakeClock is a manually advanced clock for the in-memory write bookkeeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTouchStore(t *testing.T, retention time.Duration) (*Store, *fakeClock) {
	t.Helper()
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	st.retention = retention
	st.now = clk.Now
	return st, clk
}

// #530: while the record still has more than half of its TTL left, touching
// must not write; once less than half is left it must write exactly once.
func TestTouchNodeRegion_WritesOnlyWhenTTLBelowHalf(t *testing.T) {
	ctx := context.Background()
	st, clk := newTouchStore(t, 100*time.Hour)

	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	base := st.nodeRegionWrites.Load()

	clk.Advance(49 * time.Hour)
	for i := 0; i < 5; i++ {
		if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
			t.Fatal(err)
		}
	}
	if got := st.nodeRegionWrites.Load() - base; got != 0 {
		t.Fatalf("writes while TTL > half = %d, want 0", got)
	}

	clk.Advance(2 * time.Hour) // 51h elapsed: less than half of the TTL remains
	for i := 0; i < 5; i++ {
		if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
			t.Fatal(err)
		}
	}
	if got := st.nodeRegionWrites.Load() - base; got != 1 {
		t.Fatalf("writes after TTL < half = %d, want exactly 1", got)
	}

	// The refresh restarts the interval.
	clk.Advance(49 * time.Hour)
	if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if got := st.nodeRegionWrites.Load() - base; got != 1 {
		t.Fatalf("writes right after a refresh = %d, want still 1", got)
	}
}

// reopenWithRecord returns a store that holds a record for node-a but has no
// in-memory bookkeeping, as after a collector restart.
func reopenWithRecord(t *testing.T, retention time.Duration) (*Store, *fakeClock) {
	t.Helper()
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveNodeRegion(context.Background(), "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	st.retention = retention
	st.now = clk.Now
	return st, clk
}

// After a collector restart the in-memory bookkeeping is empty, so the first
// touch of a node with a stored record writes (its remaining TTL is unknown);
// later touches within half the retention do not.
func TestTouchNodeRegion_FirstTouchAfterRestartWrites(t *testing.T) {
	ctx := context.Background()
	st, clk := reopenWithRecord(t, 100*time.Hour)
	for i := 0; i < 3; i++ {
		if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Hour)
	}
	if got := st.nodeRegionWrites.Load(); got != 1 {
		t.Fatalf("writes = %d, want 1", got)
	}
}

// Touching must never create a record: a node without one (never saved or
// expired) keeps getting the default region instead of a persisted guess (#512).
func TestTouchNodeRegion_DoesNotCreateMissingRecord(t *testing.T) {
	ctx := context.Background()
	st, clk := newTouchStore(t, 100*time.Hour)
	for i := 0; i < 3; i++ {
		if err := st.TouchNodeRegion(ctx, "node-a", "default"); err != nil {
			t.Fatal(err)
		}
		clk.Advance(60 * time.Hour) // also past the re-check interval
	}
	if got := st.nodeRegionWrites.Load(); got != 0 {
		t.Fatalf("writes = %d, want 0", got)
	}
	if _, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || ok {
		t.Fatalf("NodeRegion = _, %v, %v; want absent", ok, err)
	}
	// A later Register-time save is honoured and then refreshed normally.
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(60 * time.Hour)
	if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if got := st.nodeRegionWrites.Load(); got != 2 {
		t.Fatalf("writes = %d, want 2 (save + refresh)", got)
	}
}

// Many concurrent events of one node past the threshold must produce one write.
func TestTouchNodeRegion_ConcurrentTouchesWriteOnce(t *testing.T) {
	ctx := context.Background()
	st, clk := newTouchStore(t, 100*time.Hour)
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	base := st.nodeRegionWrites.Load()
	clk.Advance(60 * time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = st.TouchNodeRegion(ctx, "node-a", "eu-west")
		}()
	}
	wg.Wait()
	if got := st.nodeRegionWrites.Load() - base; got != 1 {
		t.Fatalf("writes = %d, want 1", got)
	}
}

// A gap of a whole retention or more means the record has expired; touching
// must not resurrect it, whether or not the node's mark was pruned meanwhile.
func TestTouchNodeRegion_ExpiredRecordNotRecreated(t *testing.T) {
	ctx := context.Background()
	st, clk := newTouchStore(t, 100*time.Hour)
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	// Simulate the expiry by removing the record (the fake clock cannot age Badger TTLs).
	if err := st.db.Update(func(tx *badger.Txn) error { return tx.Delete([]byte(nodeRegionPrefix + "node-a")) }); err != nil {
		t.Fatal(err)
	}
	base := st.nodeRegionWrites.Load()
	clk.Advance(101 * time.Hour) // node-a's own mark is still in the map
	if err := st.TouchNodeRegion(ctx, "node-a", "default"); err != nil {
		t.Fatal(err)
	}
	if got := st.nodeRegionWrites.Load() - base; got != 0 {
		t.Fatalf("writes = %d, want 0", got)
	}
	if _, ok, _ := st.NodeRegion(ctx, "node-a"); ok {
		t.Fatal("expired record was re-created")
	}
}

// A failed attempt must not suppress the next one.
func TestTouchNodeRegion_FailedAttemptIsRetried(t *testing.T) {
	st, _ := reopenWithRecord(t, 100*time.Hour)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = st.TouchNodeRegion(cancelled, "node-a", "eu-west") // may fail; must not leave a claim behind
	if err := st.TouchNodeRegion(context.Background(), "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if got := st.nodeRegionWrites.Load(); got != 1 {
		t.Fatalf("writes = %d, want 1 (by the first attempt if it got through, else by the retry)", got)
	}
}

// The #530 scenario with the real clock and a short TTL: a node that keeps
// sending events for longer than the retention, without re-registering, must
// still have its region after a restart (reopen).
func TestTouchNodeRegion_SurvivesReopenAfterLongConnection(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.retention = 3 * time.Second
	if err := st.SaveNodeRegion(ctx, "node-a", "eu-west"); err != nil { // Register
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second) // well past one retention
	for time.Now().Before(deadline) {
		if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil { // event
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || !ok || r != "eu-west" {
		t.Fatalf("NodeRegion after long connection + reopen = %q, %v, %v; want eu-west", r, ok, err)
	}
}

// #559: a stale touch (a stream that still carries the node's old region) must
// extend the TTL only; it must not overwrite the region a newer Save stored.
func TestTouchNodeRegion_StaleTouchDoesNotOverwriteNewerRegion(t *testing.T) {
	ctx := context.Background()
	st, clk := newTouchStore(t, 100*time.Hour)

	if err := st.SaveNodeRegion(ctx, "node-a", "region-a"); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveNodeRegion(ctx, "node-a", "region-b"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(51 * time.Hour) // a refresh is due
	base := st.nodeRegionWrites.Load()
	if err := st.TouchNodeRegion(ctx, "node-a", "region-a"); err != nil {
		t.Fatal(err)
	}
	if got := st.nodeRegionWrites.Load() - base; got != 1 {
		t.Fatalf("refresh writes = %d, want 1 (the TTL must still be extended)", got)
	}
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || !ok || r != "region-b" {
		t.Fatalf("NodeRegion = %q, %v, %v; want region-b, true, nil", r, ok, err)
	}
}

// Same after a collector restart, when nothing is remembered about the node.
func TestTouchNodeRegion_StaleTouchAfterRestartKeepsStoredRegion(t *testing.T) {
	ctx := context.Background()
	st, _ := reopenWithRecord(t, 100*time.Hour) // stored: eu-west
	if err := st.TouchNodeRegion(ctx, "node-a", "us-east"); err != nil {
		t.Fatal(err)
	}
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || !ok || r != "eu-west" {
		t.Fatalf("NodeRegion = %q, %v, %v; want eu-west, true, nil", r, ok, err)
	}
}

// A Save that lands between a touch's claim and its write must win. The hook
// runs after the touch has claimed the work and before it writes.
func TestTouchNodeRegion_SaveDuringRefreshWins(t *testing.T) {
	ctx := context.Background()
	st, _ := reopenWithRecord(t, 100*time.Hour) // stored: eu-west, first touch is due
	st.beforeRefreshWrite = func() {
		st.beforeRefreshWrite = nil
		if err := st.SaveNodeRegion(ctx, "node-a", "ap-south"); err != nil {
			t.Error(err)
		}
	}
	if err := st.TouchNodeRegion(ctx, "node-a", "eu-west"); err != nil {
		t.Fatal(err)
	}
	if r, ok, err := st.NodeRegion(ctx, "node-a"); err != nil || !ok || r != "ap-south" {
		t.Fatalf("NodeRegion = %q, %v, %v; want ap-south, true, nil", r, ok, err)
	}
}
