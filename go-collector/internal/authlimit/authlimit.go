// Package authlimit is the failed-auth throttle shared by the gRPC receiver
// and the REST bearer-auth middleware (#223/#230, #386).
//
// Only authentication failures are counted per source (IPv4 /32, IPv6 /64).
// Token verification is O(1) (#232), so the caller verifies first and a valid
// token passes regardless of the source's budget.
package authlimit

import (
	"container/list"
	"net/netip"
	"sync"
	"time"
)

const (
	Burst   = 5.0        // consecutive failures tolerated per source
	PerSec  = 5.0 / 60.0 // refill rate: 5 per minute
	MaxKeys = 10000      // tracked sources; beyond this the least recently used is evicted
)

type entry struct {
	key    string
	tokens float64
	last   time.Time
}

// Limiter throttles failed auth attempts per source. Reserve decides and
// consumes in one critical section so concurrent requests cannot slip through.
type Limiter struct {
	mu      sync.Mutex
	entries map[string]*list.Element // value is *entry; list front is most recent
	order   *list.List
	Now     func() time.Time
	Burst   float64
	Rate    float64 // tokens/sec
	MaxKeys int
	OnFail  func(reason string) // optional: called once per rejected attempt
}

// New returns a Limiter with the default burst, rate and key cap.
func New() *Limiter {
	return &Limiter{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		Now:     time.Now,
		Burst:   Burst,
		Rate:    PerSec,
		MaxKeys: MaxKeys,
	}
}

// Key rounds an IP string to its limiting unit (IPv6 -> /64).
func Key(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	a = a.Unmap()
	if a.Is6() {
		p, err := a.Prefix(64)
		if err == nil {
			return p.String()
		}
	}
	return a.String()
}

func (e *entry) refill(now time.Time, rate, burst float64) {
	e.tokens += now.Sub(e.last).Seconds() * rate
	if e.tokens > burst {
		e.tokens = burst
	}
	e.last = now
}

// Reserve atomically consumes one token; false means no budget left.
// A nil Limiter is unlimited (always true).
func (l *Limiter) Reserve(ip string) bool {
	if l == nil {
		return true
	}
	key := Key(ip)
	now := l.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.entries[key]
	if !ok {
		if l.order.Len() >= l.MaxKeys {
			if oldest := l.order.Back(); oldest != nil {
				l.order.Remove(oldest)
				delete(l.entries, oldest.Value.(*entry).key)
			}
		}
		el = l.order.PushFront(&entry{key: key, tokens: l.Burst, last: now})
		l.entries[key] = el
	} else {
		l.order.MoveToFront(el)
	}
	e := el.Value.(*entry)
	e.refill(now, l.Rate, l.Burst)
	if e.tokens < 1 {
		return false
	}
	e.tokens--
	return true
}

// RecordFail reports a rejected attempt with a fixed reason label. Nil-safe.
func (l *Limiter) RecordFail(reason string) {
	if l != nil && l.OnFail != nil {
		l.OnFail(reason)
	}
}

// Len returns the number of tracked sources.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}

// Tracked reports whether key (see Key) is currently tracked.
func (l *Limiter) Tracked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.entries[key]
	return ok
}
