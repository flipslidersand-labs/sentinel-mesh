// Package aggregator implements a pull-based cross-region aggregation layer.
//
// In aggregate mode the collector does not receive events over gRPC. Instead it
// periodically polls the REST API of one or more per-region collectors and
// serves a unified, read-only view (nodes / events / alerts / regions). A region
// whose collector is unreachable is isolated: its last-known data is dropped and
// it is reported as unreachable, but other regions continue to serve.
package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/api"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// maxResponseBytes caps how much of an upstream response fetchJSON will
// read. Without a limit, a malicious/compromised upstream could stream an
// unbounded body and OOM the aggregator (#76).
const maxResponseBytes = 10 * 1024 * 1024 // 10MB

// eventLimit is how many recent events/alerts are fetched and cached per region.
const eventLimit = 100

// Upstream is a single per-region collector to poll.
type Upstream struct {
	Region string
	URL    string // base URL, e.g. http://192.0.2.10:8081
}

var regionNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ParseUpstreams parses "region=url" specs into Upstreams. Only http/https
// URLs with a host are accepted — this is operator-supplied config, not
// attacker input, but rejecting other schemes (file://, unix://, ...) up
// front is cheap and removes a class of misconfiguration (#76).
//
// A plaintext http:// upstream sends the bearer token unencrypted, so it is
// rejected unless the host is loopback (127.0.0.0/8, ::1, localhost) or
// allowInsecureHTTP is true (#359).
func ParseUpstreams(specs []string, allowInsecureHTTP bool) ([]Upstream, error) {
	out := make([]Upstream, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	// Error messages deliberately never include the spec or URL: the value
	// may carry credentials in userinfo (#364).
	for i, spec := range specs {
		region, rawURL, ok := strings.Cut(spec, "=")
		region, rawURL = strings.TrimSpace(region), strings.TrimSpace(rawURL)
		if !ok || region == "" || rawURL == "" || !regionNameRe.MatchString(region) {
			// region may actually be a URL fragment (no region= prefix), so
			// it is never echoed here.
			return nil, fmt.Errorf("invalid upstream #%d: expected region=url", i+1)
		}
		rawURL = strings.TrimRight(rawURL, "/")
		parsed, err := url.Parse(rawURL)
		if err != nil {
			// The parse error embeds the raw URL (and may echo fragments
			// of it), so report a fixed message instead.
			return nil, fmt.Errorf("invalid upstream %q: unparsable URL", region)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("invalid upstream %q: scheme must be http or https", region)
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("invalid upstream %q: missing host", region)
		}
		if parsed.User != nil {
			return nil, fmt.Errorf("invalid upstream %q: userinfo (user:pass@) is not allowed in the URL; use bearer auth", region)
		}
		if parsed.Scheme == "http" && !allowInsecureHTTP && !isLoopbackHost(parsed.Hostname()) {
			return nil, fmt.Errorf("invalid upstream %q: plaintext http:// to a non-loopback host would send the bearer token unencrypted; use https:// or pass --aggregate-allow-insecure-http", region)
		}
		if _, dup := seen[region]; dup {
			return nil, fmt.Errorf("invalid upstream %q: duplicate region", region)
		}
		seen[region] = struct{}{}
		out = append(out, Upstream{Region: region, URL: rawURL})
	}
	return out, nil
}

// RegionStatus is the per-region roll-up including reachability.
type RegionStatus struct {
	api.RegionSummary
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
}

type regionState struct {
	reachable bool
	lastErr   string
	nodes     []registry.AgentNode
	events    []model.Event
	alerts    []model.Alert
	stats     map[string]int
}

// Aggregator polls upstream collectors and caches a merged view.
type Aggregator struct {
	upstreams []Upstream
	client    *http.Client
	interval  time.Duration
	apiToken  string
	log       *zap.Logger

	mu    sync.RWMutex
	state map[string]*regionState
}

// New creates an Aggregator. A nil logger is replaced with a no-op logger.
// apiToken, when non-empty, is sent as an `Authorization: Bearer` header on
// every request to an upstream region collector's REST API — those
// endpoints are gated by the same SENTINEL_API_TOKEN-based bearer auth the
// aggregator's own REST API uses (see httpauth.BearerAuth), so without this
// the aggregator gets 401s and marks every region unreachable the moment an
// operator follows the project's own guidance to set that token (#176).
func New(upstreams []Upstream, interval time.Duration, apiToken string, log *zap.Logger) *Aggregator {
	if log == nil {
		log = zap.NewNop()
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	state := make(map[string]*regionState, len(upstreams))
	for _, u := range upstreams {
		state[u.Region] = &regionState{}
	}
	return &Aggregator{
		upstreams: upstreams,
		apiToken:  apiToken,
		client: &http.Client{
			Timeout: 5 * time.Second,
			// Never follow redirects: a compromised/malicious upstream could
			// 302 the aggregator's outbound request to an internal address
			// (e.g. a cloud metadata endpoint) it can otherwise reach — SSRF
			// via the aggregator's own network position (#76). Treat a
			// redirect response as a failed fetch instead.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		interval: interval,
		log:      log,
		state:    state,
	}
}

// Start polls all upstreams once immediately, then on every interval tick until
// ctx is cancelled.
func (a *Aggregator) Start(ctx context.Context) {
	a.PollOnce(ctx)
	go func() {
		ticker := time.NewTicker(a.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.PollOnce(ctx)
			}
		}
	}()
}

// PollOnce polls every upstream once, concurrently, and updates the cache.
func (a *Aggregator) PollOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, u := range a.upstreams {
		wg.Add(1)
		go func(u Upstream) {
			defer wg.Done()
			a.pollUpstream(ctx, u)
		}(u)
	}
	wg.Wait()
}

func (a *Aggregator) pollUpstream(ctx context.Context, u Upstream) {
	st := &regionState{reachable: true}

	if err := a.fetchJSON(ctx, u.URL+api.PathNodes, &st.nodes); err != nil {
		a.markUnreachable(u, "nodes", err)
		return
	}
	if err := a.fetchJSON(ctx, fmt.Sprintf("%s%s?limit=%d", u.URL, api.PathEvents, eventLimit), &st.events); err != nil {
		a.markUnreachable(u, "events", err)
		return
	}
	if err := a.fetchJSON(ctx, fmt.Sprintf("%s%s?limit=%d", u.URL, api.PathAlerts, eventLimit), &st.alerts); err != nil {
		a.markUnreachable(u, "alerts", err)
		return
	}

	// Best-effort: an upstream without /api/stats (older version) must not be
	// marked unreachable for it; it just contributes nothing to the totals.
	if err := a.fetchJSON(ctx, u.URL+"/api/stats", &st.stats); err != nil {
		a.log.Debug("upstream stats unavailable", zap.String("region", u.Region), zap.String("error", classifyErr(err)))
		st.stats = nil
	}

	a.mu.Lock()
	a.state[u.Region] = st
	a.mu.Unlock()
}

func (a *Aggregator) markUnreachable(u Upstream, path string, err error) {
	a.log.Warn("upstream unreachable",
		zap.String("region", u.Region), zap.String("url", u.URL),
		zap.String("path", path), zap.Error(err))
	a.mu.Lock()
	a.state[u.Region] = &regionState{reachable: false, lastErr: classifyErr(err)}
	a.mu.Unlock()
}

func (a *Aggregator) fetchJSON(ctx context.Context, url string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if a.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiToken)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return &statusError{code: resp.StatusCode}
	}
	// Cap how much we'll read — an unbounded body from a malicious/misbehaving
	// upstream could otherwise OOM the aggregator (#76).
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(target); err != nil {
		return &decodeError{err: err}
	}
	return nil
}

// statusError is a non-200 upstream response.
type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("upstream status %d", e.code) }

// decodeError is an unparseable / oversized upstream body.
type decodeError struct{ err error }

func (e *decodeError) Error() string { return e.err.Error() }
func (e *decodeError) Unwrap() error { return e.err }

// classifyErr maps an upstream failure to a fixed string safe to expose via
// REST. Raw error text embeds URLs / internal hostnames, so it must only go
// to the log (#254).
func classifyErr(err error) string {
	var se *statusError
	var de *decodeError
	switch {
	case errors.As(err, &se):
		return fmt.Sprintf("upstream error status %d", se.code)
	case errors.As(err, &de):
		return "invalid response"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "unreachable"
	}
}

// Nodes returns merged nodes across all reachable regions. Each node's Region
// is overwritten with the --upstreams key it was fetched from (the region's
// source of truth, as in Regions/EventsIn), not the value the upstream
// reported, so /api/nodes?region= agrees with /api/regions (#468).
func (a *Aggregator) Nodes() []registry.AgentNode {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]registry.AgentNode, 0)
	for region, s := range a.state {
		if !s.reachable {
			continue
		}
		for _, n := range s.nodes {
			n.Region = region
			out = append(out, n)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Region < out[j].Region
	})
	return out
}

// Stats returns event counts per type summed over reachable regions, the same
// shape as a normal collector's /api/stats.
func (a *Aggregator) Stats() map[string]int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]int)
	for _, s := range a.state {
		if !s.reachable {
			continue
		}
		for t, n := range s.stats {
			out[t] += n
		}
	}
	return out
}

// cachedEvents returns one reachable region's cached events, narrowed to node
// when non-empty. It is the fallback when a direct upstream fetch fails.
func (a *Aggregator) cachedEvents(region, node string) []model.Event {
	a.mu.RLock()
	defer a.mu.RUnlock()
	s := a.state[region]
	if s == nil || !s.reachable {
		return nil
	}
	out := make([]model.Event, 0, len(s.events))
	for _, e := range s.events {
		if node == "" || e.NodeID == node {
			out = append(out, e)
		}
	}
	return out
}

// withEventRegion returns a copy of evs with Region set to the --upstreams key.
// The cached slice is never modified.
func withEventRegion(evs []model.Event, region string) []model.Event {
	out := make([]model.Event, len(evs))
	for i, e := range evs {
		e.Region = region
		out[i] = e
	}
	return out
}

// EventsIn returns merged events across reachable regions, newest first,
// restricted to one region (empty = all regions). Each event's Region is the
// --upstreams key it came from, not the upstream's own value (#540).
func (a *Aggregator) EventsIn(region string) []model.Event {
	a.mu.RLock()
	defer a.mu.RUnlock()
	lists := make([][]model.Event, 0, len(a.upstreams))
	for _, u := range a.upstreams {
		if s := a.state[u.Region]; s != nil && s.reachable && (region == "" || u.Region == region) {
			lists = append(lists, withEventRegion(s.events, u.Region))
		}
	}
	return mergeEvents(lists)
}

// mergeEvents k-way merges per-region event lists. Each list is already in its
// collector's order (store receive time, newest first), which is trusted; the
// agent-supplied Timestamp is only used to interleave the heads of different
// regions, never to reorder events within one region. Re-sorting the union by
// Timestamp would let a node with a fast clock pin itself to the top (#78).
//
// The cross-region head comparison uses the collector's ReceivedAt (#473), so
// a fast-clock node cannot lift its region above others. Events from upstreams
// or stored records that predate received_at have a zero ReceivedAt and fall
// back to the agent Timestamp, so in a mixed-version deployment those regions
// can still be ranked by a skewed clock.
func mergeEvents(lists [][]model.Event) []model.Event {
	total := 0
	for _, l := range lists {
		total += len(l)
	}
	out := make([]model.Event, 0, total)
	pos := make([]int, len(lists))
	for len(out) < total {
		best := -1
		for i, l := range lists {
			if pos[i] >= len(l) {
				continue
			}
			if best < 0 {
				best = i
				continue
			}
			h, b := l[pos[i]], lists[best][pos[best]]
			ht, bt := mergeTime(h), mergeTime(b)
			if ht.After(bt) || (ht.Equal(bt) && h.EventID < b.EventID) {
				best = i
			}
		}
		out = append(out, lists[best][pos[best]])
		pos[best]++
	}
	return out
}

// mergeTime is the time used to compare events across regions: the collector
// receive time when present, else the agent Timestamp.
func mergeTime(e model.Event) time.Time {
	if !e.ReceivedAt.IsZero() {
		return e.ReceivedAt
	}
	return e.Timestamp
}

// FetchEvents queries the upstream collectors directly (bypassing the capped
// per-region cache) with node/limit forwarded, so a node whose events are
// older than the cache window, or a limit above the cache size, can still be
// served. region restricts to one upstream (empty = all). Upstreams that
// fail are skipped, like unreachable regions in the cached view.
func (a *Aggregator) FetchEvents(ctx context.Context, region, node string, limit int) []model.Event {
	return mergeEvents(fetchPerUpstream(ctx, a, region, node, limit, api.PathEvents, "events",
		a.cachedEvents, withEventRegion))
}

// FetchAlerts is the alert counterpart of FetchEvents: it asks the upstreams
// directly for node / a limit above the cache size and falls back, per
// upstream, to the node-filtered cache when the fetch fails (#553).
func (a *Aggregator) FetchAlerts(ctx context.Context, region, node string, limit int) []model.Alert {
	lists := fetchPerUpstream(ctx, a, region, node, limit, api.PathAlerts, "alerts",
		a.cachedAlerts, withAlertRegion)
	var out []model.Alert
	for _, l := range lists {
		out = append(out, l...)
	}
	sortAlerts(out)
	if out == nil {
		out = []model.Alert{}
	}
	return out
}

// fetchPerUpstream queries path on every upstream (or only region's) in
// parallel with limit and node forwarded. An upstream that fails is replaced
// by cached(region, node), and every list is stamped with its --upstreams key
// via withRegion. The result has one entry per upstream, in upstream order.
func fetchPerUpstream[T any](ctx context.Context, a *Aggregator, region, node string, limit int,
	path, kind string, cached func(region, node string) []T, withRegion func([]T, string) []T) [][]T {
	lists := make([][]T, len(a.upstreams))
	var wg sync.WaitGroup
	for i, u := range a.upstreams {
		if region != "" && u.Region != region {
			continue
		}
		wg.Add(1)
		go func(i int, u Upstream) {
			defer wg.Done()
			q := url.Values{"limit": {strconv.Itoa(limit)}}
			if node != "" {
				q.Set("node", node)
			}
			var items []T
			if err := a.fetchJSON(ctx, u.URL+path+"?"+q.Encode(), &items); err != nil {
				a.log.Warn("upstream "+kind+" fetch failed, falling back to cache",
					zap.String("region", u.Region), zap.String("url", u.URL), zap.Error(err))
				lists[i] = withRegion(cached(u.Region, node), u.Region)
				return
			}
			lists[i] = withRegion(items, u.Region)
		}(i, u)
	}
	wg.Wait()
	return lists
}

// AlertsIn returns merged alerts across reachable regions, newest first,
// restricted to one region (empty = all regions). Each alert's Region is the
// --upstreams key it came from, not the upstream's own value (#540).
func (a *Aggregator) AlertsIn(region string) []model.Alert {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]model.Alert, 0)
	for r, s := range a.state {
		if s.reachable && (region == "" || r == region) {
			for _, al := range s.alerts {
				al.Region = r
				out = append(out, al)
			}
		}
	}
	sortAlerts(out)
	return out
}

// sortAlerts orders alerts newest first, ties broken by AlertID.
func sortAlerts(out []model.Alert) {
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Timestamp.Equal(out[j].Timestamp) {
			return out[i].Timestamp.After(out[j].Timestamp)
		}
		return out[i].AlertID < out[j].AlertID
	})
}

// withAlertRegion returns a copy of als with Region set to the --upstreams
// key. The cached slice is never modified.
func withAlertRegion(als []model.Alert, region string) []model.Alert {
	out := make([]model.Alert, len(als))
	for i, al := range als {
		al.Region = region
		out[i] = al
	}
	return out
}

// cachedAlerts is the alert counterpart of cachedEvents.
func (a *Aggregator) cachedAlerts(region, node string) []model.Alert {
	a.mu.RLock()
	defer a.mu.RUnlock()
	s := a.state[region]
	if s == nil || !s.reachable {
		return nil
	}
	out := make([]model.Alert, 0, len(s.alerts))
	for _, al := range s.alerts {
		if node == "" || al.NodeID == node {
			out = append(out, al)
		}
	}
	return out
}

// Regions returns the per-region status roll-up, sorted by region name.
func (a *Aggregator) Regions() []RegionStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]RegionStatus, 0, len(a.state))
	for region, s := range a.state {
		rs := RegionStatus{
			RegionSummary: api.RegionSummary{
				Region:      region,
				NodeCount:   len(s.nodes),
				ActiveCount: registry.CountActive(s.nodes),
			},
			Reachable: s.reachable, Error: s.lastErr,
		}
		out = append(out, rs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Region < out[j].Region })
	return out
}

// isLoopbackHost reports whether host is "localhost" or a loopback IP
// literal (127.0.0.0/8, ::1). Other hostnames are never treated as loopback
// since resolution is under the resolver's control.
func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
