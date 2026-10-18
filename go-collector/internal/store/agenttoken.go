package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// ErrTokenExists is returned by IssueToken when node_id already has a
// non-revoked token — callers must RevokeToken first to reissue, so a
// stale token can never be silently replaced without an explicit revoke
// step in the audit trail.
var ErrTokenExists = errors.New("store: node_id already has an active token")

// AgentToken is the persisted record for a per-agent credential (#183/#189,
// per ADR-005). Only the SHA-256 hash of the token is stored — the plaintext
// token is returned once, at issue time, and never written to disk or logs.
type AgentToken struct {
	NodeID    string     `json:"node_id"`
	TokenHash string     `json:"token_hash"` // hex-encoded sha256
	IssuedAt  time.Time  `json:"issued_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

func agentTokenKey(nodeID string) []byte {
	return []byte(fmt.Sprintf("agenttoken:%s", nodeID))
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// generateToken returns a random, URL-safe-ish hex token. 32 random bytes
// (256 bits) matches the entropy of the existing shared SENTINEL_API_TOKEN
// convention documented in httpauth.
func generateToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// IssueToken generates and persists a new token for nodeID, returning the
// plaintext token. It fails with ErrTokenExists if nodeID already has a
// non-revoked token — RevokeToken it first to reissue, so replacing a
// credential always leaves an explicit revoke in the record rather than
// silently overwriting it.
func (s *Store) IssueToken(ctx context.Context, nodeID string) (string, error) {
	existing, err := s.getAgentToken(ctx, nodeID)
	if err != nil {
		return "", err
	}
	if existing != nil && existing.RevokedAt == nil {
		return "", ErrTokenExists
	}

	token, err := generateToken()
	if err != nil {
		return "", err
	}
	rec := AgentToken{
		NodeID:    nodeID,
		TokenHash: hashToken(token),
		IssuedAt:  time.Now().UTC(),
	}
	if err := s.putAgentToken(ctx, rec); err != nil {
		return "", err
	}
	return token, nil
}

// RevokeToken marks nodeID's current token as revoked. It is a no-op
// (returns nil) if nodeID has no token or is already revoked, so callers
// don't need to check existence first.
func (s *Store) RevokeToken(ctx context.Context, nodeID string) error {
	rec, err := s.getAgentToken(ctx, nodeID)
	if err != nil {
		return err
	}
	if rec == nil || rec.RevokedAt != nil {
		return nil
	}
	now := time.Now().UTC()
	rec.RevokedAt = &now
	return s.putAgentToken(ctx, *rec)
}

// ListTokens returns all agent token records (revoked and active), for the
// `token list` CLI (#191). Tokens are returned by node_id, ascending.
func (s *Store) ListTokens(ctx context.Context) ([]AgentToken, error) {
	tokens := make([]AgentToken, 0)
	err := runWithContext(ctx, func() error {
		return s.db.View(func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			prefix := []byte("agenttoken:")
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				var rec AgentToken
				if err := it.Item().Value(func(v []byte) error {
					return json.Unmarshal(v, &rec)
				}); err != nil {
					return err
				}
				tokens = append(tokens, rec)
			}
			return nil
		})
	})
	return tokens, err
}

// LookupNodeID resolves a presented plaintext token to its owning node_id.
// ok is false if the token doesn't match any active (non-revoked) record —
// this covers both "unknown token" and "revoked token" uniformly, so
// callers (the gRPC/REST auth paths, #190) can't accidentally branch
// differently on those two cases and leak which one it was.
//
// Every stored hash is compared in constant time via
// crypto/subtle.ConstantTimeCompare, matching the existing BearerAuth
// convention — but which record wins (if any) still leaks through timing
// across the full scan. That's an accepted, tracked tradeoff of the
// unindexed lookup below, not a masked one.
func (s *Store) LookupNodeID(ctx context.Context, token string) (nodeID string, ok bool, err error) {
	want := []byte(hashToken(token))

	tokens, err := s.ListTokens(ctx)
	if err != nil {
		return "", false, err
	}
	for _, rec := range tokens {
		if rec.RevokedAt != nil {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(rec.TokenHash), want) == 1 {
			nodeID = rec.NodeID
			ok = true
		}
	}
	return nodeID, ok, nil
}

func (s *Store) getAgentToken(ctx context.Context, nodeID string) (*AgentToken, error) {
	var rec *AgentToken
	err := runWithContext(ctx, func() error {
		return s.db.View(func(tx *badger.Txn) error {
			item, err := tx.Get(agentTokenKey(nodeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				var r AgentToken
				if err := json.Unmarshal(v, &r); err != nil {
					return err
				}
				rec = &r
				return nil
			})
		})
	})
	return rec, err
}

func (s *Store) putAgentToken(ctx context.Context, rec AgentToken) error {
	val, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	// Agent token records are credentials, not observability data — they
	// must not expire under the same retention TTL as events/alerts (#77),
	// or a long-lived agent's token would silently stop authenticating.
	return runWithContext(ctx, func() error {
		return s.db.Update(func(tx *badger.Txn) error {
			return tx.Set(agentTokenKey(rec.NodeID), val)
		})
	})
}
