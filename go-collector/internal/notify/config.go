package notify

import (
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Environment variables consumed by DispatcherFromEnv.
const (
	envSlackWebhook = "SENTINEL_SLACK_WEBHOOK_URL"
	envSMTPAddr     = "SENTINEL_SMTP_ADDR" // host:port
	envSMTPUser     = "SENTINEL_SMTP_USER"
	envSMTPPassword = "SENTINEL_SMTP_PASSWORD" //nolint:gosec // env var name, not a credential
	envSMTPFrom     = "SENTINEL_SMTP_FROM"
	envSMTPTo       = "SENTINEL_SMTP_TO" // comma-separated
	envCooldown     = "SENTINEL_NOTIFY_COOLDOWN"
	envMinSeverity  = "SENTINEL_NOTIFY_MIN_SEVERITY"
)

const defaultCooldown = 5 * time.Minute

// DispatcherFromEnv builds a Dispatcher whose notifiers are configured entirely
// from environment variables. Channels with missing configuration are skipped;
// the returned Dispatcher is always non-nil and its Enabled method reports
// whether any channel was configured.
func DispatcherFromEnv(logger *zap.Logger) *Dispatcher {
	var notifiers []Notifier

	if url := strings.TrimSpace(os.Getenv(envSlackWebhook)); url != "" {
		notifiers = append(notifiers, NewSlackNotifier(url, nil))
	}

	if addr := strings.TrimSpace(os.Getenv(envSMTPAddr)); addr != "" {
		to := splitAndTrim(os.Getenv(envSMTPTo))
		from := strings.TrimSpace(os.Getenv(envSMTPFrom))
		if from != "" && len(to) > 0 {
			notifiers = append(notifiers, NewEmailNotifier(EmailConfig{
				Addr:     addr,
				Username: strings.TrimSpace(os.Getenv(envSMTPUser)),
				Password: os.Getenv(envSMTPPassword),
				From:     from,
				To:       to,
			}))
		} else if logger != nil {
			logger.Warn("SMTP notifier skipped: " + envSMTPFrom + " and " + envSMTPTo + " are required")
		}
	}

	cooldown := defaultCooldown
	if v := strings.TrimSpace(os.Getenv(envCooldown)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cooldown = d
		} else if logger != nil {
			logger.Warn("invalid "+envCooldown+", using default", zap.String("value", v))
		}
	}

	return NewDispatcher(Options{
		Notifiers:   notifiers,
		Cooldown:    cooldown,
		MinSeverity: strings.TrimSpace(os.Getenv(envMinSeverity)),
		Logger:      logger,
	})
}

func splitAndTrim(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
