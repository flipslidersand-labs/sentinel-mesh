package store

import (
	"context"
	"errors"
	"time"

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
	return s.writeNodeRegion(ctx, nodeID, region, s.now())
}

// writeNodeRegion stores the record and, on success, remembers at (taken before
// the write, so the remembered age is never younger than the real one) as the
// time of the last write.
func (s *Store) writeNodeRegion(ctx context.Context, nodeID, region string, at time.Time) error {
	err := s.run(ctx, func() error {
		return s.db.Update(func(tx *badger.Txn) error {
			return tx.SetEntry(badger.NewEntry([]byte(nodeRegionPrefix+nodeID), []byte(region)).WithTTL(s.retention))
		})
	})
	if err != nil {
		return err
	}
	s.nodeRegionWrites.Add(1)
	s.nodeRegionMu.Lock()
	if prev, ok := s.nodeRegionAt[nodeID]; !ok || prev.absent || at.After(prev.at) {
		s.nodeRegionAt[nodeID] = nodeRegionMark{at: at}
	}
	s.nodeRegionMu.Unlock()
	return nil
}

// nodeRegionMark is what the store remembers about one node's record: when it
// was last written, or (absent) when it was last found to be missing.
type nodeRegionMark struct {
	at     time.Time
	absent bool
}

// TouchNodeRegion keeps the record of a node that is still active from
// expiring, so a node that stays connected longer than the retention window
// does not lose its region (#530). It is cheap to call on every event: it
// writes only when the node's last write is older than half the retention (less
// than half of the TTL left), or on the first touch since this process started
// (the remaining TTL is unknown). It only refreshes a record that exists: a
// missing one (never saved, or expired) is not created, so a guessed default
// region is never made permanent (#512); the absence is re-checked after half a
// retention. Concurrent callers claim the work under a lock, so one due
// refresh is written once; a failed attempt releases the claim so the next call
// retries. No I/O happens under the lock.
func (s *Store) TouchNodeRegion(ctx context.Context, nodeID, region string) error {
	if nodeID == "" || region == "" {
		return errors.New("node_id and region required")
	}
	now := s.now()
	s.nodeRegionMu.Lock()
	prev, known := s.nodeRegionAt[nodeID]
	if known && now.Sub(prev.at) < s.retention/2 {
		s.nodeRegionMu.Unlock()
		return nil
	}
	claim := nodeRegionMark{at: now}
	s.nodeRegionAt[nodeID] = claim // concurrent touches see this one as fresh
	s.pruneNodeRegionAtLocked(now)
	s.nodeRegionMu.Unlock()

	err := s.refreshNodeRegion(ctx, nodeID, region, now, !known || prev.absent)
	if err != nil {
		s.nodeRegionMu.Lock()
		if s.nodeRegionAt[nodeID] == claim { // still our claim
			delete(s.nodeRegionAt, nodeID)
		}
		s.nodeRegionMu.Unlock()
	}
	return err
}

// refreshNodeRegion writes the record. When checkExists is set it first looks
// the record up and, if it is missing, leaves it missing and marks the node
// absent instead.
func (s *Store) refreshNodeRegion(ctx context.Context, nodeID, region string, now time.Time, checkExists bool) error {
	if checkExists {
		_, ok, err := s.NodeRegion(ctx, nodeID)
		if err != nil {
			return err
		}
		if !ok {
			s.nodeRegionMu.Lock()
			if cur := s.nodeRegionAt[nodeID]; cur.at.Equal(now) { // not replaced by a Save meanwhile
				s.nodeRegionAt[nodeID] = nodeRegionMark{at: now, absent: true}
			}
			s.nodeRegionMu.Unlock()
			return nil
		}
	}
	return s.writeNodeRegion(ctx, nodeID, region, now)
}

// pruneNodeRegionAtLocked drops bookkeeping of nodes not written for a whole
// retention window: their record has expired anyway, and it bounds the map.
// Caller holds nodeRegionMu.
func (s *Store) pruneNodeRegionAtLocked(now time.Time) {
	for id, m := range s.nodeRegionAt {
		if now.Sub(m.at) >= s.retention {
			delete(s.nodeRegionAt, id)
		}
	}
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
