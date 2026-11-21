package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

// DefaultRetention bounds how long events/alerts are kept before BadgerDB
// expires them. Without a TTL the store grows without limit (#77).
const DefaultRetention = 7 * 24 * time.Hour

// vlogGCInterval is how often the background goroutine invokes
// db.RunValueLogGC. TTL-expired entries are only excluded from reads until
// value log GC actually reclaims the disk space they occupy (#149).
// A var, not a const, so tests can shrink it to exercise the ticker branch
// of runValueLogGC without waiting 5 real minutes.
var vlogGCInterval = 5 * time.Minute

// vlogGCDiscardRatio is the ratio passed to RunValueLogGC: a file is
// rewritten if this fraction of it is estimated to be discardable.
const vlogGCDiscardRatio = 0.5

// maxNodeFilterScan bounds how many keys ListEvents/ListAlerts will read
// while looking for matches to a node filter. Without this cap, a node
// filter that matches few (or zero) records forces a full scan of the
// retention window on every request — a client-controlled "limit=1" query
// could otherwise force reading the store's entire contents (#153). This is
// a short-term mitigation; the proper fix is a secondary index keyed by
// node so a node-filtered lookup is O(matches) instead of O(scanned), which
// is tracked separately as it's a larger structural change.
// A var, not a const, so tests can shrink it rather than saving 10000+
// records to exercise the cap.
var maxNodeFilterScan = 10000

type Store struct {
	db        *badger.DB
	retention time.Duration

	// inflight counts runWithContext goroutines still touching db, so Close
	// can wait for them instead of closing the DB underneath a running View
	// ("DB Closed" panic, #351).
	inflight sync.WaitGroup
	// sem bounds how many run goroutines may exist at once. runWithContext
	// leaves fn running after ctx is done, so without a cap a stalled disk
	// plus repeated timeouts piles up goroutines without limit (#452).
	sem chan struct{}

	// countersMu/counters back Stats(): an in-memory per-type count,
	// incremented in SaveEvent (after a successful write) and seeded once at startup by loadCounters,
	// so Stats() no longer has to scan every event on every call (#77).
	// Counts may run slightly high relative to what's still on disk once
	// TTL expires old events; loadCounters corrects this on next restart.
	countersMu sync.Mutex
	counters   map[string]int

	// tokensMu/tokenIndex back LookupNodeID (#232): token hash (hex sha256)
	// -> node_id for active (non-revoked) tokens only, seeded once at
	// startup by loadTokenIndex and kept in sync by IssueToken/RevokeToken.
	// BadgerDB's exclusive directory lock means no other process can write
	// token records while this Store is open, so these are the only write
	// paths. tokenIndexErr is the startup load error, if any: auth then fails
	// closed instead of treating every token as unknown.
	tokensMu      sync.RWMutex
	tokenIndex    map[string]string
	tokenIndexErr error
	// putTokenFn is a test seam for injecting token write failures; nil in production.
	putTokenFn func(ctx context.Context, rec AgentToken) error

	// beforeUpdateFn is a test seam run inside SaveEvent's background write
	// just before db.Update; nil in production.
	beforeUpdateFn func()

	gcStop chan struct{}
	gcDone chan struct{}
}

func New(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:        db,
		retention: DefaultRetention,
		counters:  map[string]int{"exec": 0, "tcp": 0, "file": 0},
		sem:       make(chan struct{}, maxInflight),
		gcStop:    make(chan struct{}),
		gcDone:    make(chan struct{}),
	}
	if err := s.loadCounters(); err != nil {
		db.Close() //nolint:errcheck
		return nil, err
	}
	s.loadTokenIndex()
	go s.runValueLogGC()
	return s, nil
}

// runValueLogGC periodically reclaims value log disk space for entries whose
// TTL has expired. Without this, RunValueLogGC is never called and expired
// entries are merely excluded from reads while their data stays on disk
// forever (#149). It stops when Close() closes gcStop.
func (s *Store) runValueLogGC() {
	defer close(s.gcDone)
	ticker := time.NewTicker(vlogGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.gcStop:
			return
		case <-ticker.C:
			for {
				err := s.db.RunValueLogGC(vlogGCDiscardRatio)
				if err != nil {
					// badger.ErrNoRewrite / badger.ErrRejected are expected:
					// either nothing left worth rewriting right now, or GC
					// is already running/DB is closing. Anything else we
					// treat the same way — just stop this round; the next
					// tick tries again.
					break
				}
			}
		}
	}
}

// loadCounters scans existing events once, at startup, to seed the in-memory
// type counters Stats() serves from thereafter (#77).
func (s *Store) loadCounters() error {
	skipped := 0
	err := s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte("event:")); it.ValidForPrefix([]byte("event:")); it.Next() {
			var e model.Event
			var decodeErr error
			if err := it.Item().Value(func(v []byte) error {
				decodeErr = json.Unmarshal(v, &e)
				return nil
			}); err != nil {
				return err
			}
			if decodeErr != nil {
				skipped++
				continue
			}
			s.counters[e.Type]++
		}
		return nil
	})
	// A record that is not valid JSON is skipped so one damaged event can't
	// stop the collector from starting (#372).
	if err == nil && skipped > 0 {
		slog.Warn("event records could not be decoded and were skipped from the counters", "count", skipped)
	}
	return err
}

// runWithContext runs fn on its own goroutine and returns as soon as either
// fn completes or ctx is cancelled — whichever comes first. If ctx is
// cancelled first, ctx.Err() is returned immediately and fn keeps running in
// the background until BadgerDB finishes (or fails) on its own; the result is
// discarded. This bounds how long a caller (e.g. the StreamEvents goroutine
// handling a request during shutdown) can be blocked by a slow db.Update/View
// — a BadgerDB value-log GC pause or disk I/O stall no longer wedges the
// caller, and therefore the whole graceful-shutdown sequence, indefinitely
// (#155). It does not abort the underlying BadgerDB transaction — Badger has
// no such primitive — so this is a "don't block the caller" fix, not true
// cancellation.
func runWithContext(ctx context.Context, fn func() error) error {
	errCh := make(chan error, 1)
	go func() { errCh <- fn() }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maxInflight caps concurrent run goroutines, including ones still finishing
// in the background after their caller's ctx expired (#452).
const maxInflight = 256

// ErrBusy is returned without touching the db when maxInflight operations
// are already in flight (typically writes stuck behind a stalled disk).
var ErrBusy = errors.New("store: too many operations in flight")

// run is runWithContext, tracked so Close waits for the goroutine fn runs on.
// fn must not start a db transaction once ctx is done. If the in-flight limit
// is reached, run fails fast with ErrBusy instead of queueing.
func (s *Store) run(ctx context.Context, fn func() error) error {
	select {
	case s.sem <- struct{}{}:
	default:
		return ErrBusy
	}
	s.inflight.Add(1)
	return runWithContext(ctx, func() error {
		defer func() {
			<-s.sem
			s.inflight.Done()
		}()
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn()
	})
}

// timeKeyLayout is RFC3339 with a fixed 9-digit fraction. RFC3339Nano trims
// trailing zeros, so "…:00Z" sorted after "…:00.5Z" (byte 'Z' > '.') and
// events within the same second came back in the wrong order, letting a
// limit drop the newest rows (#250). A fixed width keeps byte order equal to
// time order.
const timeKeyLayout = "2006-01-02T15:04:05.000000000Z"

// timeKey formats t (normalised to UTC, so offsets can't reorder keys) as a
// fixed-width, lexicographically sortable key segment. Keys written before
// #250 use RFC3339Nano; they are not migrated (reads are prefix scans, so both
// forms stay readable). Old and new keys within the same second may order
// imperfectly, but only until the retention TTL expires the old ones.
func timeKey(t time.Time) string {
	return t.UTC().Format(timeKeyLayout)
}

// eventIDKey is the dedup index entry for (node, event_id). The node length
// prefix keeps ("a:b","c") and ("a","b:c") distinct, since both fields may
// contain ':'. It sits under "eventid:", which "event:" prefix scans skip.
func eventIDKey(nodeID, eventID string) []byte {
	return []byte(fmt.Sprintf("eventid:%d:%s:%s", len(nodeID), nodeID, eventID))
}

// maxConflictRetries bounds retries when concurrent saves of the same
// event_id collide in Badger's conflict detection.
const maxConflictRetries = 3

// SaveEvent persists e. A non-nil error does NOT guarantee the event was not
// stored: if ctx is cancelled or times out while the write is in flight, the
// error is ctx.Err() but the write may still complete in the background (see
// runWithContext). Callers that report failure to a peer (e.g. an Ack with
// ok=false) must treat it as "unknown, possibly saved". Retrying is safe: an
// event whose (node_id, event_id) was already saved within the retention
// window is skipped and reported as success (#452). Events with an empty
// event_id cannot be deduplicated. Stats() counts the event once the write
// actually succeeds.
func (s *Store) SaveEvent(ctx context.Context, e model.Event) error {
	_, err := s.SaveEventDedup(ctx, e)
	return err
}

// SaveEventDedup is SaveEvent that also reports whether e was skipped as a
// duplicate of an already stored (node_id, event_id) (#457). duplicate is
// meaningful only when err is nil.
func (s *Store) SaveEventDedup(ctx context.Context, e model.Event) (duplicate bool, err error) {
	val, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	// Key by the collector's own receive time, not e.Timestamp — that field
	// is agent-supplied and untrusted. Keying by it let a malicious agent
	// pin its events to the front of ListEvents forever with a far-future
	// timestamp, or overwrite unrelated events via a timestamp+EventID
	// collision (#78). e.Timestamp is still stored in the record itself for
	// display; it's just no longer trusted for ordering/storage identity.
	received := time.Now().UTC()
	key := []byte(fmt.Sprintf("event:%s:%s", timeKey(received), e.EventID))
	// The counter is bumped inside fn, right after db.Update succeeds: if ctx
	// is cancelled while the write is in flight, run returns an error to the
	// caller but the write still lands, and Stats() must count it (#325).
	var dup bool // written in the write goroutine; read only when run returns nil
	err = s.run(ctx, func() error {
		if s.beforeUpdateFn != nil {
			s.beforeUpdateFn()
		}
		var idKey []byte
		if e.EventID != "" {
			idKey = eventIDKey(e.NodeID, e.EventID)
		}
		var duplicate bool
		update := func(tx *badger.Txn) error {
			duplicate = false
			if idKey != nil {
				_, err := tx.Get(idKey)
				if err == nil {
					duplicate = true
					return nil
				}
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if err := tx.SetEntry(badger.NewEntry(idKey, key).WithTTL(s.retention)); err != nil {
					return err
				}
			}
			return tx.SetEntry(badger.NewEntry(key, val).WithTTL(s.retention))
		}
		var err error
		for i := 0; i < maxConflictRetries; i++ {
			// A concurrent save of the same event_id wins the index write;
			// retrying then sees it and reports a duplicate.
			if err = s.db.Update(update); !errors.Is(err, badger.ErrConflict) {
				break
			}
		}
		if err != nil {
			return err
		}
		if duplicate {
			dup = true
			return nil
		}
		s.countersMu.Lock()
		s.counters[e.Type]++
		s.countersMu.Unlock()
		return nil
	})
	if err != nil {
		return false, err
	}
	return dup, nil
}

// ListEvents returns up to limit events, newest first.
// If node is non-empty, only events with matching NodeID are returned, and
// the scan for matches stops after maxNodeFilterScan records even if fewer
// than limit matches were found (#153).
func (s *Store) ListEvents(ctx context.Context, node string, limit int) ([]model.Event, error) {
	return s.ListEventsWhere(ctx, nodeMatcher(node), limit)
}

// ListEventsWhere is ListEvents with an arbitrary node predicate (nil keeps
// everything). The scan continues until limit matches are found, bounded by
// maxNodeFilterScan records when keep is non-nil (#324).
func (s *Store) ListEventsWhere(ctx context.Context, keep func(nodeID string) bool, limit int) ([]model.Event, error) {
	return scanNewestFirst(ctx, s, "event:", "event", func(e model.Event) string { return e.NodeID }, keep, limit)
}

// nodeMatcher returns a predicate for a single node ID; nil when node is empty.
func nodeMatcher(node string) func(string) bool {
	if node == "" {
		return nil
	}
	return func(id string) bool { return id == node }
}

// scanNewestFirst walks the keys under prefix newest first, decoding each as T
// and collecting up to limit records whose node passes keep. With a non-nil
// keep the scan reads at most maxNodeFilterScan records (#153), so a rarely
// matching filter cannot force a full walk of the retention window.
func scanNewestFirst[T any](ctx context.Context, s *Store, prefixStr, label string, nodeOf func(T) string, keep func(string) bool, limit int) ([]T, error) {
	// Non-nil so callers that JSON-marshal the result return "[]" not "null".
	out := make([]T, 0)
	err := s.run(ctx, func() error {
		return s.db.View(func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Reverse = true
			it := tx.NewIterator(opts)
			defer it.Close()

			prefix := []byte(prefixStr)
			it.Seek(append(prefix, 0xFF))
			scanned := 0
			skipped := 0
			for ; it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
				if keep != nil && scanned >= maxNodeFilterScan {
					break
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				scanned++
				var rec T
				var decodeErr error
				if err := it.Item().Value(func(v []byte) error {
					decodeErr = json.Unmarshal(v, &rec)
					return nil
				}); err != nil {
					return err
				}
				if decodeErr != nil {
					skipped++
					continue
				}
				if keep != nil && !keep(nodeOf(rec)) {
					continue
				}
				out = append(out, rec)
			}
			// One warning per call, not per record (#372).
			if skipped > 0 {
				slog.Warn(label+" records could not be decoded and were skipped", "count", skipped)
			}
			return nil
		})
	})
	if err != nil {
		// On ctx expiry the View goroutine may still be appending to out;
		// do not read it (data race, #351).
		return nil, err
	}
	return out, nil
}

// Stats returns event counts per type, from the in-memory counters (#77) —
// no longer a full key scan on every call.
func (s *Store) Stats() (map[string]int, error) {
	s.countersMu.Lock()
	defer s.countersMu.Unlock()
	out := make(map[string]int, len(s.counters))
	for t, n := range s.counters {
		out[t] = n
	}
	return out, nil
}

// SaveAlert persists an alert to the store.
func (s *Store) SaveAlert(ctx context.Context, a model.Alert) error {
	val, err := json.Marshal(a)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("alert:%s:%s", timeKey(a.Timestamp), a.AlertID))
	return s.run(ctx, func() error {
		return s.db.Update(func(tx *badger.Txn) error {
			return tx.SetEntry(badger.NewEntry(key, val).WithTTL(s.retention))
		})
	})
}

// ListAlerts returns up to limit alerts, newest first.
// If node is non-empty, only alerts with matching NodeID are returned, and
// the scan for matches stops after maxNodeFilterScan records even if fewer
// than limit matches were found (#153).
func (s *Store) ListAlerts(ctx context.Context, node string, limit int) ([]model.Alert, error) {
	return s.ListAlertsWhere(ctx, nodeMatcher(node), limit)
}

// ListAlertsWhere is ListAlerts with an arbitrary node predicate; see
// ListEventsWhere (#324).
func (s *Store) ListAlertsWhere(ctx context.Context, keep func(nodeID string) bool, limit int) ([]model.Alert, error) {
	return scanNewestFirst(ctx, s, "alert:", "alert", func(a model.Alert) string { return a.NodeID }, keep, limit)
}

func (s *Store) Close() error {
	close(s.gcStop)
	<-s.gcDone
	s.inflight.Wait()
	return s.db.Close()
}
