package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// DefaultRetention bounds how long events/alerts are kept before BadgerDB
// expires them. Without a TTL the store grows without limit (#77).
const DefaultRetention = 7 * 24 * time.Hour

// vlogGCInterval is how often the background goroutine invokes
// db.RunValueLogGC. TTL-expired entries are only excluded from reads until
// value log GC actually reclaims the disk space they occupy (#149).
const vlogGCInterval = 5 * time.Minute

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

// Event is the normalized form stored in BadgerDB.
type Event struct {
	EventID   string          `json:"event_id"`
	NodeID    string          `json:"node_id"`
	Timestamp time.Time       `json:"timestamp"`
	Type      string          `json:"type"` // "exec" | "tcp" | "file"
	Payload   json.RawMessage `json:"payload"`
}

type Store struct {
	db        *badger.DB
	retention time.Duration

	// countersMu/counters back Stats(): an in-memory per-type count,
	// incremented in SaveEvent and seeded once at startup by loadCounters,
	// so Stats() no longer has to scan every event on every call (#77).
	// Counts may run slightly high relative to what's still on disk once
	// TTL expires old events; loadCounters corrects this on next restart.
	countersMu sync.Mutex
	counters   map[string]int

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
		gcStop:    make(chan struct{}),
		gcDone:    make(chan struct{}),
	}
	if err := s.loadCounters(); err != nil {
		db.Close() //nolint:errcheck
		return nil, err
	}
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
	return s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte("event:")); it.ValidForPrefix([]byte("event:")); it.Next() {
			var e Event
			if err := it.Item().Value(func(v []byte) error {
				return json.Unmarshal(v, &e)
			}); err != nil {
				return err
			}
			s.counters[e.Type]++
		}
		return nil
	})
}

func (s *Store) SaveEvent(e Event) error {
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// Key by the collector's own receive time, not e.Timestamp — that field
	// is agent-supplied and untrusted. Keying by it let a malicious agent
	// pin its events to the front of ListEvents forever with a far-future
	// timestamp, or overwrite unrelated events via a timestamp+EventID
	// collision (#78). e.Timestamp is still stored in the record itself for
	// display; it's just no longer trusted for ordering/storage identity.
	received := time.Now().UTC()
	key := []byte(fmt.Sprintf("event:%s:%s", received.Format(time.RFC3339Nano), e.EventID))
	if err := s.db.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(badger.NewEntry(key, val).WithTTL(s.retention))
	}); err != nil {
		return err
	}
	s.countersMu.Lock()
	s.counters[e.Type]++
	s.countersMu.Unlock()
	return nil
}

// ListEvents returns up to limit events, newest first.
// If node is non-empty, only events with matching NodeID are returned, and
// the scan for matches stops after maxNodeFilterScan records even if fewer
// than limit matches were found (#153).
func (s *Store) ListEvents(node string, limit int) ([]Event, error) {
	var events []Event
	err := s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := []byte("event:")
		it.Seek(append(prefix, 0xFF))
		scanned := 0
		for ; it.ValidForPrefix(prefix) && len(events) < limit; it.Next() {
			// A node filter can match rarely (or never); without a scan cap
			// this loop would otherwise keep reading until it exhausts the
			// whole "event:" prefix (#153).
			if node != "" && scanned >= maxNodeFilterScan {
				break
			}
			scanned++
			var e Event
			if err := it.Item().Value(func(v []byte) error {
				return json.Unmarshal(v, &e)
			}); err != nil {
				return err
			}
			if node != "" && e.NodeID != node {
				continue
			}
			events = append(events, e)
		}
		return nil
	})
	return events, err
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

// Alert represents a triggered alert.
type Alert struct {
	AlertID   string    `json:"alert_id"`
	RuleID    string    `json:"rule_id"`
	NodeID    string    `json:"node_id"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Severity  string    `json:"severity"`
}

// SaveAlert persists an alert to the store.
func (s *Store) SaveAlert(a Alert) error {
	val, err := json.Marshal(a)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("alert:%s:%s", a.Timestamp.Format(time.RFC3339Nano), a.AlertID))
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.SetEntry(badger.NewEntry(key, val).WithTTL(s.retention))
	})
}

// ListAlerts returns up to limit alerts, newest first.
// If node is non-empty, only alerts with matching NodeID are returned, and
// the scan for matches stops after maxNodeFilterScan records even if fewer
// than limit matches were found (#153).
func (s *Store) ListAlerts(node string, limit int) ([]Alert, error) {
	var alerts []Alert
	err := s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := []byte("alert:")
		it.Seek(append(prefix, 0xFF))
		scanned := 0
		for ; it.ValidForPrefix(prefix) && len(alerts) < limit; it.Next() {
			// See the matching comment in ListEvents: cap the scan so a
			// rarely (or never) matching node filter can't force a full
			// walk of the "alert:" prefix (#153).
			if node != "" && scanned >= maxNodeFilterScan {
				break
			}
			scanned++
			var a Alert
			if err := it.Item().Value(func(v []byte) error {
				return json.Unmarshal(v, &a)
			}); err != nil {
				return err
			}
			if node != "" && a.NodeID != node {
				continue
			}
			alerts = append(alerts, a)
		}
		return nil
	})
	return alerts, err
}

func (s *Store) Close() error {
	close(s.gcStop)
	<-s.gcDone
	return s.db.Close()
}
