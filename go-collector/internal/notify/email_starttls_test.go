package notify

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/smtp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSMTP is a minimal SMTP server for STARTTLS tests.
type fakeSMTP struct {
	ln          net.Listener
	advertise   bool // advertise STARTTLS
	cert        tls.Certificate
	mu          sync.Mutex
	startedTLS  bool
	gotData     bool
	authPlainOK bool
	authOverTLS bool
	// optional behaviors / captures (used by email_smtp_test.go)
	rejectRcpt string // if non-empty, RCPT to this address gets 550
	noAuthExt  bool   // do not advertise AUTH
	mailFrom   string
	rcpts      []string
	body       strings.Builder
}

func newTestCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

func startFakeSMTP(t *testing.T, advertise bool, cert tls.Certificate) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSMTP{ln: ln, advertise: advertise, cert: cert}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *fakeSMTP) serve(c net.Conn) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(c)
	send := func(conn net.Conn, line string) { conn.Write([]byte(line + "\r\n")) }
	send(c, "220 fake ESMTP")
	conn := c
	inData := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if inData {
			if line != "." {
				s.mu.Lock()
				s.body.WriteString(line + "\n")
				s.mu.Unlock()
			}
			if line == "." {
				inData = false
				s.mu.Lock()
				s.gotData = true
				s.mu.Unlock()
				send(conn, "250 ok")
			}
			continue
		}
		up := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(up, "EHLO"):
			s.mu.Lock()
			tlsOn := s.startedTLS
			s.mu.Unlock()
			send(conn, "250-fake")
			if s.advertise && !tlsOn {
				send(conn, "250-STARTTLS")
			}
			if s.noAuthExt {
				send(conn, "250 OK")
			} else {
				send(conn, "250 AUTH PLAIN")
			}
		case up == "STARTTLS":
			send(conn, "220 go ahead")
			tc := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{s.cert}})
			if err := tc.Handshake(); err != nil {
				return
			}
			conn = tc
			r = bufio.NewReader(tc)
			s.mu.Lock()
			s.startedTLS = true
			s.mu.Unlock()
		case strings.HasPrefix(up, "AUTH"):
			s.mu.Lock()
			s.authPlainOK = true
			s.authOverTLS = s.startedTLS
			s.mu.Unlock()
			send(conn, "235 ok")
		case strings.HasPrefix(up, "MAIL"):
			s.mu.Lock()
			s.mailFrom = line
			s.mu.Unlock()
			send(conn, "250 ok")
		case strings.HasPrefix(up, "RCPT"):
			s.mu.Lock()
			s.rcpts = append(s.rcpts, line)
			rej := s.rejectRcpt != "" && strings.Contains(line, s.rejectRcpt)
			s.mu.Unlock()
			if rej {
				send(conn, "550 no such user")
			} else {
				send(conn, "250 ok")
			}
		case up == "DATA":
			inData = true
			send(conn, "354 go")
		case up == "QUIT":
			send(conn, "221 bye")
			return
		default:
			send(conn, "500 unknown")
		}
	}
}

func TestSendMail_STARTTLSUpgrade(t *testing.T) {
	cert, pool := newTestCert(t)
	s := startFakeSMTP(t, true, cert)
	opts := smtpOptions{tlsConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	auth := smtp.PlainAuth("", "u", "p", "127.0.0.1")

	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), auth, "a@x", []string{"b@x"}, []byte("Subject: hi\r\n\r\nbody\r\n"), opts)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.startedTLS {
		t.Fatal("STARTTLS was not performed")
	}
	if !s.authPlainOK || !s.gotData {
		t.Fatalf("auth=%v data=%v", s.authPlainOK, s.gotData)
	}
	if !s.authOverTLS {
		t.Fatal("AUTH was not performed over TLS")
	}
}

func TestSendMail_RejectsAuthWithoutSTARTTLSOnNonLoopback(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	opts := smtpOptions{isLoopback: func(string) bool { return false }}
	// Auth implementation that would happily send credentials in clear.
	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), plainAnyAuth{}, "a@x", []string{"b@x"}, []byte("x"), opts)
	if err == nil {
		t.Fatal("expected error when STARTTLS unavailable with credentials on non-loopback")
	}
	if !strings.Contains(err.Error(), "refusing to send credentials") {
		t.Fatalf("unexpected error: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.authPlainOK || s.gotData {
		t.Fatalf("credentials/data leaked: auth=%v data=%v", s.authPlainOK, s.gotData)
	}
}

func TestSendMail_LoopbackWithoutSTARTTLSStillWorks(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, false, cert)
	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), nil, "a@x", []string{"b@x"}, []byte("x\r\n"), smtpOptions{})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startedTLS || !s.gotData {
		t.Fatalf("startedTLS=%v data=%v", s.startedTLS, s.gotData)
	}
}

type plainAnyAuth struct{}

func (plainAnyAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "PLAIN", []byte("\x00u\x00p"), nil
}
func (plainAnyAuth) Next([]byte, bool) ([]byte, error) { return nil, nil }

func TestSendMail_STARTTLSHandshakeFailureDoesNotFallBackToPlaintext(t *testing.T) {
	cert, _ := newTestCert(t)
	s := startFakeSMTP(t, true, cert)
	// Empty RootCAs: the self-signed server cert is not trusted.
	opts := smtpOptions{tlsConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}
	auth := smtp.PlainAuth("", "u", "p", "127.0.0.1")

	err := sendMailWithOptions(context.Background(), s.ln.Addr().String(), auth, "a@x", []string{"b@x"}, []byte("x\r\n"), opts)
	if err == nil {
		t.Fatal("expected handshake error")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gotData || s.authPlainOK {
		t.Fatalf("plaintext fallback: auth=%v data=%v", s.authPlainOK, s.gotData)
	}
}
