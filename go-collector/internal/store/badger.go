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
	}
	if err := s.loadCounters(); err != nil {
		db.Close() //nolint:errcheck
		return nil, err
	}
	return s, nil
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
// If node is non-empty, only events with matching NodeID are returned.
func (s *Store) ListEvents(node string, limit int) ([]Event, error) {
	var events []Event
	err := s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := []byte("event:")
		it.Seek(append(prefix, 0xFF))
		for ; it.ValidForPrefix(prefix) && len(events) < limit; it.Next() {
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
// If node is non-empty, only alerts with matching NodeID are returned.
func (s *Store) ListAlerts(node string, limit int) ([]Alert, error) {
	var alerts []Alert
	err := s.db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := []byte("alert:")
		it.Seek(append(prefix, 0xFF))
		for ; it.ValidForPrefix(prefix) && len(alerts) < limit; it.Next() {
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
	return s.db.Close()
}
