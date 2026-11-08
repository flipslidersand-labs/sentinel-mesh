package notify

import (
	"context"
	"net"
	"net/smtp"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// startSilentServer accepts connections and never replies. If greet is true
// it sends a 220 banner first and then stays silent.
func startSilentServer(t *testing.T, greet bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done); ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if greet {
					c.Write([]byte("220 hi\r\n"))
				}
				<-done
			}()
		}
	}()
	return ln.Addr().String()
}

// Regression guard for #109/#132: an unresponsive SMTP server must not block
// past the ctx deadline.
func TestSendMailContext_UnresponsiveServerHonorsDeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		greet bool
	}{
		{"silent before banner", false},
		{"silent after banner", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startSilentServer(t, tc.greet)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			res := make(chan error, 1)
			start := time.Now()
			go func() {
				res <- sendMailContext(ctx, addr, nil, "a@x", []string{"b@x"}, []byte("x\r\n"))
			}()
			select {
			case err := <-res:
				if err == nil {
					t.Fatal("expected error from unresponsive server")
				}
				if el := time.Since(start); el > 900*time.Millisecond {
					t.Fatalf("returned after %v, want close to the 200ms deadline", el)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("sendMailContext did not return after ctx deadline (hung)")
			}
		})
	}
}

func TestSendMailContext_DeliversMessage(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	err := sendMailContext(context.Background(), s.ln.Addr().String(), nil, "from@x", []string{"a@x", "b@x"}, []byte("Subject: hi\r\n\r\nhello-body\r\n"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.Contains(s.mailFrom, "from@x") {
		t.Errorf("MAIL FROM = %q", s.mailFrom)
	}
	if len(s.rcpts) != 2 || !strings.Contains(s.rcpts[0], "a@x") || !strings.Contains(s.rcpts[1], "b@x") {
		t.Errorf("rcpts = %v", s.rcpts)
	}
	if !strings.Contains(s.body.String(), "hello-body") {
		t.Errorf("body = %q", s.body.String())
	}
}

func TestSendMailContext_RcptRejected(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	s.rejectRcpt = "bad@x"
	err := sendMailContext(context.Background(), s.ln.Addr().String(), nil, "from@x", []string{"ok@x", "bad@x"}, []byte("x\r\n"))
	if err == nil || !strings.Contains(err.Error(), "550") {
		t.Fatalf("expected 550 error, got %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gotData {
		t.Error("DATA must not be sent after RCPT rejection")
	}
}

func TestSendMailContext_DialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens now
	if err := sendMailContext(context.Background(), addr, nil, "a@x", []string{"b@x"}, []byte("x")); err == nil {
		t.Fatal("expected dial error")
	}
}

// Auth is attempted on loopback when the server advertises AUTH, and skipped
// when it does not.
//
// The "auth + server lacks AUTH" case records CURRENT behavior only (the
// message is sent without authenticating). net/smtp.SendMail would instead
// return "server doesn't support AUTH". The intended spec is undecided and
// is tracked in #247; this expectation may change.
func TestSendMailContext_AuthBranches(t *testing.T) {
	cert, _ := newTestCert(t)
	for _, tc := range []struct {
		name     string
		noAuth   bool
		auth     smtp.Auth
		wantAuth bool
	}{
		{"auth + server advertises AUTH", false, plainAnyAuth{}, true},
		{"auth + server lacks AUTH", true, plainAnyAuth{}, false},
		{"no auth", false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeSMTP(t, false, cert)
			s.noAuthExt = tc.noAuth
			err := sendMailContext(context.Background(), s.ln.Addr().String(), tc.auth, "a@x", []string{"b@x"}, []byte("x\r\n"))
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.authPlainOK != tc.wantAuth {
				t.Errorf("auth attempted = %v, want %v", s.authPlainOK, tc.wantAuth)
			}
			if !s.gotData {
				t.Error("message not delivered")
			}
		})
	}
}

func TestIsLoopbackHost(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true, "127.0.0.1": true, "::1": true,
		"example.com": false, "10.0.0.1": false, "": false,
	} {
		if got := isLoopbackHost(host); got != want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestNewEmailNotifier_AuthSelection(t *testing.T) {
	if n := NewEmailNotifier(EmailConfig{Addr: "smtp.example.com:587"}); n.auth != nil {
		t.Error("auth must be nil without Username")
	}
	if n := NewEmailNotifier(EmailConfig{Addr: "smtp.example.com:587", Username: "u", Password: "p"}); n.auth == nil {
		t.Error("auth must be set with Username")
	}
}

// The host given to smtp.PlainAuth must equal the host net/smtp passes as
// ServerInfo.Name, otherwise PlainAuth fails with "wrong host name".
func TestNewEmailNotifier_AuthHost(t *testing.T) {
	for _, tc := range []struct{ addr, serverName string }{
		{"smtp.example.com:587", "smtp.example.com"},
		{"smtp.example.com", "smtp.example.com"}, // portless
		{"127.0.0.1:2525", "127.0.0.1"},
	} {
		n := NewEmailNotifier(EmailConfig{Addr: tc.addr, Username: "u", Password: "p"})
		_, _, err := n.auth.Start(&smtp.ServerInfo{Name: tc.serverName, TLS: true, Auth: []string{"PLAIN"}})
		if err != nil {
			t.Errorf("addr %q: auth host mismatch: %v", tc.addr, err)
		}
	}
}

func TestNewEmailNotifier_AuthHostIPv6(t *testing.T) {
	t.Skip("既知バグ #246 (email.go の LastIndex(\":\") で host が [::1] になる)。#246 修正時に Skip を外す")
	n := NewEmailNotifier(EmailConfig{Addr: "[::1]:25", Username: "u", Password: "p"})
	host, _, _ := net.SplitHostPort("[::1]:25")
	if _, _, err := n.auth.Start(&smtp.ServerInfo{Name: host, TLS: true, Auth: []string{"PLAIN"}}); err != nil {
		t.Fatalf("auth host mismatch: %v", err)
	}
}

// End to end: host extraction -> auth setup -> real SMTP dialogue.
func TestEmailNotifier_Send_EndToEndWithAuth(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	n := NewEmailNotifier(EmailConfig{
		Addr: s.ln.Addr().String(), Username: "u", Password: "p",
		From: "from@x", To: []string{"to@x"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Send(ctx, store.Alert{Severity: "critical", Message: "e2e-msg"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authPlainOK || !s.gotData {
		t.Fatalf("auth=%v data=%v", s.authPlainOK, s.gotData)
	}
	if len(s.rcpts) != 1 || !strings.Contains(s.rcpts[0], "to@x") || !strings.Contains(s.body.String(), "e2e-msg") {
		t.Errorf("rcpts=%v body=%q", s.rcpts, s.body.String())
	}
}
