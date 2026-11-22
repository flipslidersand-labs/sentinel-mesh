package exporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// errData fails every listing with err.
type errData struct {
	fakeData
	err error
}

func (d errData) ListEventsMatching(context.Context, func(model.Event) bool, int) ([]model.Event, error) {
	return nil, d.err
}

func (d errData) ListAlertsWhere(context.Context, func(string) bool, int) ([]model.Alert, error) {
	return nil, d.err
}

func getStatus(t *testing.T, events EventLister, alerts AlertLister, path string) (int, string) {
	t.Helper()
	token := strings.Repeat("x", 32)
	r := Router(RouterConfig{
		Events: events, Alerts: alerts, Stats: fakeData{}, Nodes: registry.New(),
		StaticDir: t.TempDir(), APIToken: token,
	})
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w.Code, w.Body.String()
}

// A store that is saturated (ErrBusy) or closed (ErrClosed) is a transient
// condition: clients get 503 so they retry, not 500 (#533).
func TestStoreErrors_TransientIs503(t *testing.T) {
	for name, err := range map[string]error{
		"busy":         store.ErrBusy,
		"closed":       store.ErrClosed,
		"wrapped busy": fmt.Errorf("list: %w", store.ErrBusy),
	} {
		for _, path := range []string{"/api/events", "/api/alerts"} {
			d := errData{err: err}
			code, body := getStatus(t, d, d, path)
			if code != http.StatusServiceUnavailable {
				t.Errorf("%s %s: code=%d, want 503 (body=%s)", name, path, code, body)
			}
			if strings.TrimSpace(body) != `{"error":"service unavailable"}` {
				t.Errorf("%s %s: body=%s, want fixed service-unavailable message", name, path, body)
			}
		}
	}
}

// Other store errors keep the generic 500.
func TestStoreErrors_OtherIs500(t *testing.T) {
	d := errData{err: errors.New("disk on fire")}
	for _, path := range []string{"/api/events", "/api/alerts"} {
		if code, body := getStatus(t, d, d, path); code != http.StatusInternalServerError {
			t.Errorf("%s: code=%d, want 500 (body=%s)", path, code, body)
		}
	}
}

// End to end with a real store that has been closed.
func TestStoreErrors_ClosedRealStoreIs503(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/events", "/api/alerts"} {
		if code, body := getStatus(t, st, st, path); code != http.StatusServiceUnavailable {
			t.Errorf("%s: code=%d, want 503 (body=%s)", path, code, body)
		}
	}
}
