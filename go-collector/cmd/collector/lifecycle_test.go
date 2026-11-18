package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRunHTTPServerUntilDone_ListenErrorReturned(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := newHTTPServer(ln.Addr().String(), http.NewServeMux())
	err = runHTTPServerUntilDone(context.Background(), srv, "test", zap.NewNop())
	if err == nil {
		t.Fatal("expected error for address in use, got nil")
	}
}

func TestRunHTTPServerUntilDone_WaitsForInflightRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
	})
	srv := newHTTPServer(addr, mux)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runHTTPServerUntilDone(ctx, srv, "test", zap.NewNop()) }()

	var resp *http.Response
	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		for i := 0; i < 100; i++ {
			r, err := http.Get("http://" + addr + "/slow")
			if err == nil {
				resp = r
				r.Body.Close()
				return
			}
			select {
			case <-started:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("returned before in-flight request finished")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after request finished")
	}
	<-reqDone
	_ = resp
}

func TestRunRESTAndGRPC_RESTFailureStopsGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	want := errors.New("bind failed")
	err := runRESTAndGRPC(ctx, cancel,
		func(context.Context) error { return want },
		func(ctx context.Context) error { <-ctx.Done(); return nil })
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestRunRESTAndGRPC_WaitsForRESTBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	restStopped := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runRESTAndGRPC(ctx, cancel,
			func(ctx context.Context) error {
				<-ctx.Done()
				<-release // simulates slow in-flight request drain
				close(restStopped)
				return nil
			},
			func(ctx context.Context) error { <-ctx.Done(); return nil })
	}()
	cancel()
	select {
	case <-done:
		t.Fatal("returned before REST finished stopping")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-restStopped:
	default:
		t.Fatal("REST not stopped at return")
	}
}
