package notify

import (
	"context"
	"net"
	"net/smtp"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
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
func TestSendMail_UnresponsiveServerHonorsDeadline(t *testing.T) {
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
				res <- sendMailWithOptions(ctx, addr, nil, "a@x", []string{"b@x"}, []byte("x\r\n"), smtpOptions{})
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
				t.Fatal("sendMailWithOptions did not return after ctx deadline (hung)")
			}
		})
	}
}

func TestSendMail_DeliversMessage(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), nil, "from@x", []string{"a@x", "b@x"}, []byte("Subject: hi\r\n\r\nhello-body\r\n"), smtpOptions{})
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

func TestSendMail_RcptRejected(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	s.rejectRcpt = "bad@x"
	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), nil, "from@x", []string{"ok@x", "bad@x"}, []byte("x\r\n"), smtpOptions{})
	if err == nil || !strings.Contains(err.Error(), "550") {
		t.Fatalf("expected 550 error, got %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gotData {
		t.Error("DATA must not be sent after RCPT rejection")
	}
}

func TestSendMail_DialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens now
	if err := sendMailWithOptions(context.Background(), addr, nil, "a@x", []string{"b@x"}, []byte("x"), smtpOptions{}); err == nil {
		t.Fatal("expected dial error")
	}
}

// Auth is attempted when the server advertises AUTH. With credentials but a
// server that does not advertise AUTH, sending fails closed (same as
// net/smtp.SendMail: "server doesn't support AUTH") and no message is
// delivered (issue #247). Without credentials, no auth is attempted.
func TestSendMail_AuthBranches(t *testing.T) {
	cert, _ := newTestCert(t)
	for _, tc := range []struct {
		name     string
		noAuth   bool
		auth     smtp.Auth
		wantAuth bool
		wantErr  bool
	}{
		{"auth + server advertises AUTH", false, plainAnyAuth{}, true, false},
		{"auth + server lacks AUTH", true, plainAnyAuth{}, false, true},
		{"no auth", false, nil, false, false},
		{"no auth + server lacks AUTH", true, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeSMTP(t, false, cert)
			s.noAuthExt = tc.noAuth
			err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), tc.auth, "a@x", []string{"b@x"}, []byte("x\r\n"), smtpOptions{})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "AUTH") {
					t.Fatalf("want AUTH error, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("send: %v", err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.authPlainOK != tc.wantAuth {
				t.Errorf("auth attempted = %v, want %v", s.authPlainOK, tc.wantAuth)
			}
			if s.gotData == tc.wantErr {
				t.Errorf("gotData = %v, want %v", s.gotData, !tc.wantErr)
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
	for _, tc := range []struct{ addr, serverName string }{
		{"[::1]:25", "::1"},
		{"[::1]", "::1"}, // bracketed, portless
		{"[2001:db8::1]:587", "2001:db8::1"},
		{"smtp.example.com:587", "smtp.example.com"},
		{"127.0.0.1:2525", "127.0.0.1"},
	} {
		n := NewEmailNotifier(EmailConfig{Addr: tc.addr, Username: "u", Password: "p"})
		if _, _, err := n.auth.Start(&smtp.ServerInfo{Name: tc.serverName, TLS: true, Auth: []string{"PLAIN"}}); err != nil {
			t.Errorf("addr %q: auth host mismatch: %v", tc.addr, err)
		}
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
	if err := n.Send(ctx, model.Alert{Severity: "critical", Message: "e2e-msg"}); err != nil {
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
