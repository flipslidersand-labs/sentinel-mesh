package notify

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// EmailNotifier delivers alerts over SMTP.
type EmailNotifier struct {
	addr string // SMTP host:port
	auth smtp.Auth
	from string
	to   []string
	// sendMail is overridable in tests; defaults to smtp.SendMail.
	sendMail func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

// EmailConfig configures an EmailNotifier.
type EmailConfig struct {
	Addr     string   // host:port
	Username string   // optional; if empty no auth is used
	Password string   // optional
	From     string   // sender address
	To       []string // recipient addresses
}

// NewEmailNotifier builds an EmailNotifier. PLAIN auth is used when a username
// is supplied; otherwise the message is sent without authentication.
func NewEmailNotifier(cfg EmailConfig) *EmailNotifier {
	var auth smtp.Auth
	if cfg.Username != "" {
		host := cfg.Addr
		if i := strings.LastIndex(host, ":"); i >= 0 {
			host = host[:i]
		}
		auth = smtp.PlainAuth("", cfg.Username, cfg.Password, host)
	}
	return &EmailNotifier{
		addr:     cfg.Addr,
		auth:     auth,
		from:     cfg.From,
		to:       cfg.To,
		sendMail: smtp.SendMail,
	}
}

// Name implements Notifier.
func (e *EmailNotifier) Name() string { return "email" }

// buildMessage renders an RFC 5322 message for the alert.
func (e *EmailNotifier) buildMessage(a store.Alert) []byte {
	subject := fmt.Sprintf("[SentinelMesh][%s] %s", a.Severity, a.Message)
	body := fmt.Sprintf(
		"A SentinelMesh alert was triggered.\r\n\r\n"+
			"Severity: %s\r\n"+
			"Message:  %s\r\n"+
			"Node:     %s\r\n"+
			"Rule:     %s\r\n"+
			"Event:    %s\r\n"+
			"Time:     %s\r\n",
		a.Severity, a.Message, a.NodeID, a.RuleID, a.EventID,
		a.Timestamp.Format(time.RFC3339),
	)

	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", e.from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(e.to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=\"utf-8\"\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	return []byte(b.String())
}

// Send implements Notifier.
func (e *EmailNotifier) Send(_ context.Context, a store.Alert) error {
	if len(e.to) == 0 {
		return fmt.Errorf("no recipients configured")
	}
	if err := e.sendMail(e.addr, e.auth, e.from, e.to, e.buildMessage(a)); err != nil {
		return fmt.Errorf("send email: %w", err)
	}
	return nil
}
