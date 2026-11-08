package receiver

import (
	"sync"
	"time"
)

const (
	authFailBurst     = 5.0        // failures tolerated per source IP before throttling
	authFailPerSec    = 5.0 / 60.0 // refill: 5 failures per minute
	authFailMaxKeys   = 10000      // cap on tracked IPs (memory bound)
	authFailIdleAfter = 5 * time.Minute
)

// authFailLimiter throttles repeated authentication failures per source IP so
// an unauthenticated caller cannot force a full token scan on every RPC.
type authFailLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

func newAuthFailLimiter() *authFailLimiter {
	return &authFailLimiter{buckets: make(map[string]*tokenBucket)}
}

// blocked reports whether ip has exhausted its failure budget. It does not
// consume budget.
func (l *authFailLimiter) blocked(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[ip]
	if !ok {
		return false
	}
	tokens := b.tokens + time.Since(b.last).Seconds()*b.rate
	return tokens < 1
}

// fail records one authentication failure for ip.
func (l *authFailLimiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[ip]
	if !ok {
		if len(l.buckets) >= authFailMaxKeys {
			now := time.Now()
			for k, v := range l.buckets {
				if now.Sub(v.last) > authFailIdleAfter {
					delete(l.buckets, k)
				}
			}
			if len(l.buckets) >= authFailMaxKeys {
				return // still full: skip tracking rather than grow unbounded
			}
		}
		b = newTokenBucket(authFailPerSec, authFailBurst)
		l.buckets[ip] = b
	}
	b.allow()
}

// authFailures is the process-wide limiter consulted by checkAuth.
var authFailures = newAuthFailLimiter()
