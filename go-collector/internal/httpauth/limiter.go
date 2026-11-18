package httpauth

import (
	"container/list"
	"net/netip"
	"sync"
	"time"
)

// Failed-auth throttle per source, mirroring the gRPC receiver's
// authFailLimiter (#223/#230): same burst, refill rate, key cap and
// IPv4 /32 / IPv6 /64 keying. Only failures consume budget.
const (
	failBurst   = 5.0
	failPerSec  = 5.0 / 60.0
	failMaxKeys = 10000
)

type failEntry struct {
	key    string
	tokens float64
	last   time.Time
}

type failLimiter struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List
	now     func() time.Time
	burst   float64
	rate    float64
	maxKeys int
}

func newFailLimiter() *failLimiter {
	return &failLimiter{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		now:     time.Now,
		burst:   failBurst,
		rate:    failPerSec,
		maxKeys: failMaxKeys,
	}
}

func failKey(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	a = a.Unmap()
	if a.Is6() {
		if p, err := a.Prefix(64); err == nil {
			return p.String()
		}
	}
	return a.String()
}

// reserve atomically consumes one failure slot; false means the source is
// over its limit.
func (l *failLimiter) reserve(ip string) bool {
	key := failKey(ip)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.entries[key]
	if !ok {
		if l.order.Len() >= l.maxKeys {
			if oldest := l.order.Back(); oldest != nil {
				l.order.Remove(oldest)
				delete(l.entries, oldest.Value.(*failEntry).key)
			}
		}
		el = l.order.PushFront(&failEntry{key: key, tokens: l.burst, last: now})
		l.entries[key] = el
	} else {
		l.order.MoveToFront(el)
	}
	e := el.Value.(*failEntry)
	e.tokens += now.Sub(e.last).Seconds() * l.rate
	if e.tokens > l.burst {
		e.tokens = l.burst
	}
	e.last = now
	if e.tokens < 1 {
		return false
	}
	e.tokens--
	return true
}
