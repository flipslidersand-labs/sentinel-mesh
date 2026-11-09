package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// TokenLength is the length of every issued token: hex of 32 random bytes.
// A presented value of any other length can't match, so callers can reject
// it before hashing (#232).
const TokenLength = 64

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
	// Held across read-check-write so concurrent issue/revoke for the same
	// node can't leave the index disagreeing with what's on disk.
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()

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
	// WithoutCancel: the write's result must be known so the index matches
	// disk (runWithContext would otherwise return on cancel while the write
	// still commits in the background).
	if err := s.putAgentToken(context.WithoutCancel(ctx), rec); err != nil {
		return "", err
	}
	s.tokenIndex[rec.TokenHash] = nodeID
	return token, nil
}

// RevokeToken marks nodeID's current token as revoked. It is a no-op
// (returns nil) if nodeID has no token or is already revoked, so callers
// don't need to check existence first.
func (s *Store) RevokeToken(ctx context.Context, nodeID string) error {
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()

	rec, err := s.getAgentToken(ctx, nodeID)
	if err != nil {
		return err
	}
	if rec == nil || rec.RevokedAt != nil {
		return nil
	}
	now := time.Now().UTC()
	rec.RevokedAt = &now
	// Fail closed: drop the token from the index before writing, and write
	// with WithoutCancel so a cancelled ctx can't return while the revoke
	// commits in the background. The index entry is restored only when the
	// write definitely failed (the record on disk is then still active).
	activeNodeID, wasActive := s.tokenIndex[rec.TokenHash]
	delete(s.tokenIndex, rec.TokenHash)
	if err := s.putAgentToken(context.WithoutCancel(ctx), *rec); err != nil {
		if wasActive {
			s.tokenIndex[rec.TokenHash] = activeNodeID
		}
		return err
	}
	return nil
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
// The lookup is a single map access keyed by the token's SHA-256 hash
// (#232) — no record scan or JSON decode — so it's cheap enough to run
// before any rate limiting. Map lookup isn't constant time, but it compares
// hashes, not the secret: timing can at most reveal a prefix of the
// presented token's own SHA-256, which doesn't help recover a stored token
// (preimage resistance). The plaintext token is never held in memory
// beyond this call.
func (s *Store) LookupNodeID(ctx context.Context, token string) (nodeID string, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if len(token) != TokenLength {
		return "", false, nil
	}
	want := hashToken(token)

	s.tokensMu.RLock()
	defer s.tokensMu.RUnlock()
	if s.tokenIndexErr != nil {
		return "", false, s.tokenIndexErr
	}
	nodeID, ok = s.tokenIndex[want]
	return nodeID, ok, nil
}

// loadTokenIndex seeds tokenIndex from disk at startup. A load failure is
// kept in tokenIndexErr rather than failing New, so `token list`/`revoke`
// can still open the store to inspect or repair it, while auth fails closed.
//
// A record that is not valid JSON is skipped (its token stays invalid, as it
// is absent from the index) so one damaged record can't lock out every other
// agent (#242). Other errors (iteration/IO) still fail closed. The warning
// carries only record keys and a count, never hashes or token values.
func (s *Store) loadTokenIndex() {
	idx := make(map[string]string)
	var brokenKeys []string
	err := runWithContext(context.Background(), func() error {
		return s.db.View(func(tx *badger.Txn) error {
			prefix := []byte("agenttoken:")
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				var rec AgentToken
				var decodeErr error
				if err := item.Value(func(v []byte) error {
					decodeErr = json.Unmarshal(v, &rec)
					return nil
				}); err != nil {
					return err
				}
				if decodeErr != nil {
					brokenKeys = append(brokenKeys, string(item.KeyCopy(nil)))
					continue
				}
				if rec.RevokedAt == nil {
					idx[rec.TokenHash] = rec.NodeID
				}
			}
			return nil
		})
	})
	if err != nil {
		idx = make(map[string]string)
	} else if len(brokenKeys) > 0 {
		slog.Warn("agent token records could not be decoded and were skipped; their tokens are invalid",
			"count", len(brokenKeys), "keys", brokenKeys)
	}
	s.tokensMu.Lock()
	s.tokenIndex = idx
	s.tokenIndexErr = err
	s.tokensMu.Unlock()
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
