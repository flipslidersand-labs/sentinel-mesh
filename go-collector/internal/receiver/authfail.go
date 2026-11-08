package receiver

import (
	"container/list"
	"net/netip"
	"sync"
	"time"
)

// 既知の制限(#223 の暫定対応): 失敗は LookupNodeID の走査「前」に数えるため、
// 枠を使い切った送信元 IP(IPv6 は /64)からは有効なトークンも一時的に拒否される。
// プロキシ/LB の背後で全クライアントが同一 IP に見える構成では、1 台の失敗で
// 全台が止まりうる。根本対応(O(1) のトークン照合で走査自体を安くし、失敗のみを
// 数える)は別 Issue で追跡する。

const (
	authFailBurst   = 5.0        // 送信元ごとに許容する連続試行数
	authFailPerSec  = 5.0 / 60.0 // 回復速度: 1 分あたり 5 回
	authFailMaxKeys = 10000      // 追跡する送信元数の上限(超過時は最終アクセス最古を追い出す)
)

type authFailEntry struct {
	key    string
	tokens float64
	last   time.Time
}

// authFailLimiter は送信元(IPv4 は /32、IPv6 は /64)単位で認証試行を制限する。
// 判定と予約を 1 つのクリティカルセクションで行い、同時リクエストの
// すり抜けを防ぐ。認証成功時は release で予約を返す。
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

// release は認証成功時に予約を返す。満タンに戻った項目は表から外す。
func (l *authFailLimiter) release(ip string) {
	if l == nil {
		return
	}
	key := authFailKey(ip)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.entries[key]
	if !ok {
		return
	}
	e := el.Value.(*authFailEntry)
	e.refill(now, l.rate, l.burst)
	e.tokens++
	if e.tokens >= l.burst {
		l.order.Remove(el)
		delete(l.entries, key)
	}
}
