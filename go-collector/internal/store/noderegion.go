package store

import (
	"context"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
)

// nodeRegionPrefix keys the persisted node -> region map (#512). The value is
// the bare region string. It does not start with "event:" or "alert:", so the
// event/alert prefix scans skip it.
const nodeRegionPrefix = "node:"

// SaveNodeRegion remembers nodeID's region so it survives registry eviction
// and collector restarts. Like events it expires after the retention window;
// every save refreshes the TTL. Unlike agent tokens it is observability
// metadata, not a credential, so expiry is acceptable.
func (s *Store) SaveNodeRegion(ctx context.Context, nodeID, region string) error {
	if nodeID == "" || region == "" {
		return errors.New("node_id and region required")
	}
	return s.run(ctx, func() error {
		return s.db.Update(func(tx *badger.Txn) error {
			return tx.SetEntry(badger.NewEntry([]byte(nodeRegionPrefix+nodeID), []byte(region)).WithTTL(s.retention))
		})
	})
}

// NodeRegion returns the persisted region of nodeID; ok is false when none is
// stored (never saved or expired). An error means "unknown", not "absent".
func (s *Store) NodeRegion(ctx context.Context, nodeID string) (region string, ok bool, err error) {
	// region/ok are written by the goroutine and read only after runRead
	// returned nil (#382).
	var got string
	var found bool
	err = s.runRead(ctx, func() error {
		return s.db.View(func(tx *badger.Txn) error {
			item, err := tx.Get([]byte(nodeRegionPrefix + nodeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				got, found = string(v), true
				return nil
			})
		})
	})
	if err != nil {
		return "", false, err
	}
	return got, found, nil
}
