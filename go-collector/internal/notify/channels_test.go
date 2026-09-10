package notify

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func sampleAlert() store.Alert {
	return store.Alert{
		AlertID:   "a1",
		RuleID:    "suspicious-exec",
		NodeID:    "yuki",
		EventID:   "e42",
		Timestamp: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
		Message:   "unexpected process",
		Severity:  "critical",
	}
}

func TestSlackNotifier_Send(t *testing.T) {
	var gotBody string
	var gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := NewSlackNotifier(srv.URL, srv.Client())
	if err := n.Send(context.Background(), sampleAlert()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotContentType != "application/json" {
		t.Errorf("content-type = %q", gotContentType)
	}
	for _, want := range []string{"critical", "unexpected process", "yuki", "suspicious-exec"} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("slack body missing %q: %s", want, gotBody)
		}
	}
}

func TestSlackNotifier_Non2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	n := NewSlackNotifier(srv.URL, srv.Client())
	if err := n.Send(context.Background(), sampleAlert()); err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestEmailNotifier_BuildMessage(t *testing.T) {
	n := NewEmailNotifier(EmailConfig{
		Addr: "smtp.example.com:587",
		From: "alerts@example.com",
		To:   []string{"ops@example.com", "oncall@example.com"},
	})
	msg := string(n.buildMessage(sampleAlert()))

	for _, want := range []string{
		"From: alerts@example.com",
		"To: ops@example.com, oncall@example.com",
		"Subject: [SentinelMesh][critical] unexpected process",
		"Node:     yuki",
		"Rule:     suspicious-exec",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("email message missing %q\n---\n%s", want, msg)
		}
	}
	// Headers must be separated from the body by a blank line.
	if !strings.Contains(msg, "\r\n\r\n") {
		t.Error("email message missing header/body separator")
	}
}

func TestEmailNotifier_Send(t *testing.T) {
	var gotAddr, gotFrom string
	var gotTo []string
	n := NewEmailNotifier(EmailConfig{
		Addr: "smtp.example.com:587",
		From: "alerts@example.com",
		To:   []string{"ops@example.com"},
	})
	n.sendMail = func(_ context.Context, addr string, _ smtp.Auth, from string, to []string, msg []byte) error {
		gotAddr, gotFrom, gotTo = addr, from, to
		if len(msg) == 0 {
			t.Error("empty message")
		}
		return nil
	}

	if err := n.Send(context.Background(), sampleAlert()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotAddr != "smtp.example.com:587" || gotFrom != "alerts@example.com" || len(gotTo) != 1 {
		t.Errorf("unexpected sendMail args: addr=%s from=%s to=%v", gotAddr, gotFrom, gotTo)
	}
}

// TestEmailNotifier_Send_RespectsContextDeadline verifies that Send does not
// hang past the caller's context deadline when the SMTP server accepts the
// TCP connection but never replies (issue #109).
func TestEmailNotifier_Send_RespectsContextDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// Accept connections but never write anything back, simulating a
	// hung SMTP server.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Keep the connection open without responding; close only
			// when the listener itself is closed.
			go func(c net.Conn) {
				<-make(chan struct{}) // block until process/test teardown
				_ = c
			}(conn)
		}
	}()

	n := NewEmailNotifier(EmailConfig{
		Addr: ln.Addr().String(),
		From: "alerts@example.com",
		To:   []string{"ops@example.com"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = n.Send(ctx, sampleAlert())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected Send to fail against an unresponsive SMTP server")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Send blocked for %v, want it bounded by the context deadline", elapsed)
	}
}

func TestEmailNotifier_NoRecipients(t *testing.T) {
	n := NewEmailNotifier(EmailConfig{Addr: "smtp.example.com:587", From: "a@b.c"})
	if err := n.Send(context.Background(), sampleAlert()); err == nil {
		t.Fatal("expected error when no recipients configured")
	}
}
