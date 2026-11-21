package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

// writeSelfSignedCert generates a throwaway self-signed cert/key for
// 127.0.0.1 in dir and returns their paths (#359).
func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestRunHTTPServerTLSUntilDone_ServesTLS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t, t.TempDir())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	srv := newHTTPServer(addr, mux)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runHTTPServerTLSUntilDone(ctx, srv, "test", certPath, keyPath, zap.NewNop())
	}()

	certPEM, _ := os.ReadFile(certPath)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	tlsClient := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}}
	var resp *http.Response
	for i := 0; i < 50; i++ {
		resp, err = tlsClient.Get("https://" + addr + "/ok")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("https request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	// Plaintext HTTP to the TLS port must not be served as HTTP 200.
	plain := &http.Client{Timeout: 2 * time.Second}
	if r, err := plain.Get("http://" + addr + "/ok"); err == nil {
		r.Body.Close()
		if r.StatusCode == http.StatusOK {
			t.Error("plaintext request to TLS listener returned 200")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("shutdown error: %v", err)
	}
}

func TestRunHTTPServerUntilDone_ShutdownTimeoutForceCloses(t *testing.T) {
	old := httpShutdownTimeout
	httpShutdownTimeout = 100 * time.Millisecond
	defer func() { httpShutdownTimeout = old }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	started := make(chan struct{})
	ctxCancelled := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
			close(ctxCancelled)
		case <-time.After(10 * time.Second):
		}
	})
	srv := newHTTPServer(addr, mux)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runHTTPServerUntilDone(ctx, srv, "test", zap.NewNop()) }()

	clientErr := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 50; i++ {
			var resp *http.Response
			resp, err = http.Get("http://" + addr + "/slow")
			if err == nil {
				resp.Body.Close()
				break
			}
			select {
			case <-started:
				clientErr <- err
				return
			default:
			}
			time.Sleep(50 * time.Millisecond)
		}
		clientErr <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runHTTPServerUntilDone did not return after shutdown timeout")
	}
	select {
	case <-ctxCancelled:
	case <-time.After(2 * time.Second):
		t.Error("handler context not cancelled: connection was not force-closed")
	}
	select {
	case err := <-clientErr:
		if err == nil {
			t.Error("client got a response, want connection error")
		}
	case <-time.After(2 * time.Second):
		t.Error("client request still pending after force close")
	}
}
