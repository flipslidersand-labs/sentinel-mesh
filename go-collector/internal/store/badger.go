package store

import (
	"encoding/json"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// Event is the normalized form stored in BadgerDB.
type Event struct {
	EventID   string          `json:"event_id"`
	NodeID    string          `json:"node_id"`
	Timestamp time.Time       `json:"timestamp"`
	Type      string          `json:"type"` // "exec" | "tcp" | "file"
	Payload   json.RawMessage `json:"payload"`
}

type Store struct {
	db *badger.DB
}

func New(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) SaveEvent(e Event) error {
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("event:%s:%s", e.Timestamp.Format(time.RFC3339Nano), e.EventID))
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, val)
	})
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

// Stats returns event counts per type.
func (s *Store) Stats() (map[string]int, error) {
	counts := map[string]int{"exec": 0, "tcp": 0, "file": 0}
	err := s.db.View(func(tx *badger.Txn) error {
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
			counts[e.Type]++
		}
		return nil
	})
	return counts, err
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
		return tx.Set(key, val)
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
