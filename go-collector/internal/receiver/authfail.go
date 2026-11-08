package receiver

import (
	"container/list"
	"net/netip"
	"sync"
	"time"
)

// 認証失敗のみを送信元単位で数える(#223)。トークン照合は O(1)(#232)なので
// 照合を先に行い、有効なトークンは送信元の枠の状態に関係なく通す。
// これによりプロキシ/LB の背後で 1 台が失敗を繰り返しても他の台は止まらない。

const (
	authFailBurst   = 5.0        // 送信元ごとに許容する連続失敗数
	authFailPerSec  = 5.0 / 60.0 // 回復速度: 1 分あたり 5 回
	authFailMaxKeys = 10000      // 追跡する送信元数の上限(超過時は最終アクセス最古を追い出す)
)

type authFailEntry struct {
	key    string
	tokens float64
	last   time.Time
}

// authFailLimiter は送信元(IPv4 は /32、IPv6 は /64)単位で認証試行を制限する。
// 判定と消費を 1 つのクリティカルセクションで行い、同時リクエストの
// すり抜けを防ぐ。認証失敗時にのみ呼ばれる。
type authFailLimiter struct {
	mu      sync.Mutex
	entries map[string]*list.Element // 値は *authFailEntry、list は先頭が最新
	order   *list.List
	now     func() time.Time
	burst   float64
	rate    float64 // tokens/sec
	maxKeys int
}

func newAuthFailLimiter() *authFailLimiter {
	return &authFailLimiter{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		now:     time.Now,
		burst:   authFailBurst,
		rate:    authFailPerSec,
		maxKeys: authFailMaxKeys,
	}
}

// authFailKey は IP 文字列を制限単位のキーに丸める(IPv6 は /64)。
func authFailKey(ip string) string {
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

func (e *authFailEntry) refill(now time.Time, rate, burst float64) {
	e.tokens += now.Sub(e.last).Seconds() * rate
	if e.tokens > burst {
		e.tokens = burst
	}
	e.last = now
}

// reserve は 1 トークンを原子的に予約する。枠が無ければ false。
// nil 受信側は無制限(常に true)。
func (l *authFailLimiter) reserve(ip string) bool {
	if l == nil {
		return true
	}
	key := authFailKey(ip)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.entries[key]
	if !ok {
		if l.order.Len() >= l.maxKeys {
			if oldest := l.order.Back(); oldest != nil {
				l.order.Remove(oldest)
				delete(l.entries, oldest.Value.(*authFailEntry).key)
			}
		}
		el = l.order.PushFront(&authFailEntry{key: key, tokens: l.burst, last: now})
		l.entries[key] = el
	} else {
		l.order.MoveToFront(el)
	}
	e := el.Value.(*authFailEntry)
	e.refill(now, l.rate, l.burst)
	if e.tokens < 1 {
		return false
	}
	e.tokens--
	return true
}
