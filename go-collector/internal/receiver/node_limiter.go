package receiver

import (
	"sync"
	"time"
)

const (
	// nodeLimiterIdleTTL is how long a node's bucket with no live stream is
	// kept. A bucket fully refills in burst/rate (2s), so dropping an entry
	// idle for far longer is equivalent to keeping it: the node would get a
	// full bucket either way.
	nodeLimiterIdleTTL = 10 * time.Minute
	// nodeLimiterSweepInterval bounds how often acquire scans for idle entries.
	nodeLimiterSweepInterval = time.Minute
)

// sharedBucket is a tokenBucket safe for use by several streams at once.
type sharedBucket struct {
	mu sync.Mutex
	tb *tokenBucket

	// guarded by nodeLimiters.mu
	active   int
	lastUsed time.Time
}

func (b *sharedBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tb.allow()
}

// nodeLimiters hands out one shared token bucket per authenticated node_id,
// so a single token cannot multiply its event rate by opening more
// connections or streams (#254; MaxConcurrentStreams is per connection).
// The zero value is ready to use. Keys must be authenticated node_ids only;
// the map is therefore bounded by the number of issued tokens, and idle
// entries are additionally swept.
type nodeLimiters struct {
	mu        sync.Mutex
	m         map[string]*sharedBucket
	lastSweep time.Time
	now       func() time.Time // test hook; nil means time.Now
}

func (n *nodeLimiters) clock() time.Time {
	if n.now != nil {
		return n.now()
	}
	return time.Now()
}

// acquire returns the shared bucket for nodeID and a release func that must
// be called when the stream ends.
func (n *nodeLimiters) acquire(nodeID string) (*sharedBucket, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := n.clock()
	if n.m == nil {
		n.m = make(map[string]*sharedBucket)
		n.lastSweep = now
	}
	if now.Sub(n.lastSweep) >= nodeLimiterSweepInterval {
		for k, b := range n.m {
			if b.active == 0 && now.Sub(b.lastUsed) > nodeLimiterIdleTTL {
				delete(n.m, k)
			}
		}
		n.lastSweep = now
	}
	b, ok := n.m[nodeID]
	if !ok {
		b = &sharedBucket{tb: newTokenBucket(streamEventsRateLimit, streamEventsRateBurst)}
		n.m[nodeID] = b
	}
	b.active++
	b.lastUsed = now
	var once sync.Once
	return b, func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			b.active--
			b.lastUsed = n.clock()
		})
	}
}
