package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

func guardEvent(id string) model.Event {
	return model.Event{EventID: id, NodeID: "n1", Timestamp: time.Now(), Type: "exec", Payload: json.RawMessage(`{}`)}
}

// Calls after Close must fail with an error instead of reaching the closed
// DB (Badger panics with "DB Closed" on a goroutine nobody can recover, #422).
func TestAfterClose_CallsReturnErrorWithoutPanic(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()
	if _, err := st.SaveEventDedup(ctx, guardEvent("after-close")); err == nil {
		t.Error("SaveEventDedup after Close returned nil error")
	}
	if _, err := st.ListEventsWhere(ctx, nil, 10); err == nil {
		t.Error("ListEventsWhere after Close returned nil error")
	}
}

// Close may be called more than once (deferred Close plus a shutdown path).
func TestClose_MultipleCallsDoNotPanic(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Close racing with continuously running writers and readers must neither
// panic nor trip the race detector; every caller eventually gets an error.
func TestClose_ConcurrentWithOperations(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	var closed atomic.Bool // set once Close has returned; errors after that are expected
	started := make(chan struct{}, 16)
	for w := 0; w < 8; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				if _, err := st.SaveEventDedup(ctx, guardEvent(fmt.Sprintf("w%d-%d", w, i))); err != nil && closed.Load() {
					return
				}
				if i == 0 {
					started <- struct{}{}
				}
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				if _, err := st.ListEventsWhere(ctx, nil, 5); err != nil && closed.Load() {
					return
				}
				if i == 0 {
					started <- struct{}{}
				}
			}
		}()
	}
	for i := 0; i < 16; i++ {
		<-started
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed.Store(true)
	wg.Wait()
}

func TestAfterClose_ReturnsErrClosed(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()
	if _, err := st.SaveEventDedup(ctx, guardEvent("e")); !errors.Is(err, ErrClosed) {
		t.Errorf("SaveEventDedup after Close: err=%v; want ErrClosed", err)
	}
	if _, err := st.ListEventsWhere(ctx, nil, 10); !errors.Is(err, ErrClosed) {
		t.Errorf("ListEventsWhere after Close: err=%v; want ErrClosed", err)
	}
}
