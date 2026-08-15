package notify

import (
	"testing"
	"time"
)

func TestDispatcherFromEnv_NoConfig(t *testing.T) {
	// Ensure a clean environment for every relevant var.
	for _, k := range []string{
		envSlackWebhook, envSMTPAddr, envSMTPUser, envSMTPPassword,
		envSMTPFrom, envSMTPTo, envCooldown, envMinSeverity,
	} {
		t.Setenv(k, "")
	}
	d := DispatcherFromEnv(nil)
	if d == nil {
		t.Fatal("DispatcherFromEnv must never return nil")
	}
	if d.Enabled() {
		t.Fatal("no channels configured → should be disabled")
	}
	if d.cooldown != defaultCooldown {
		t.Errorf("cooldown = %v, want default %v", d.cooldown, defaultCooldown)
	}
}

func TestDispatcherFromEnv_Slack(t *testing.T) {
	t.Setenv(envSlackWebhook, "https://hooks.slack.com/services/xxx")
	t.Setenv(envSMTPAddr, "")
	d := DispatcherFromEnv(nil)
	if !d.Enabled() || len(d.notifiers) != 1 {
		t.Fatalf("expected 1 slack notifier, got %d", len(d.notifiers))
	}
	if d.notifiers[0].Name() != "slack" {
		t.Errorf("notifier[0] = %s, want slack", d.notifiers[0].Name())
	}
}

func TestDispatcherFromEnv_EmailRequiresFromAndTo(t *testing.T) {
	t.Setenv(envSlackWebhook, "")
	t.Setenv(envSMTPAddr, "smtp.example.com:587")
	t.Setenv(envSMTPFrom, "") // missing → email skipped
	t.Setenv(envSMTPTo, "ops@example.com")
	if DispatcherFromEnv(nil).Enabled() {
		t.Fatal("email without From should be skipped")
	}

	t.Setenv(envSMTPFrom, "alerts@example.com")
	t.Setenv(envSMTPTo, "ops@example.com, oncall@example.com")
	d := DispatcherFromEnv(nil)
	if len(d.notifiers) != 1 || d.notifiers[0].Name() != "email" {
		t.Fatalf("expected 1 email notifier, got %v", d.notifiers)
	}
	email := d.notifiers[0].(*EmailNotifier)
	if len(email.to) != 2 {
		t.Errorf("expected 2 recipients, got %v", email.to)
	}
}

func TestDispatcherFromEnv_CooldownAndSeverity(t *testing.T) {
	t.Setenv(envSlackWebhook, "https://example.com/hook")
	t.Setenv(envCooldown, "30s")
	t.Setenv(envMinSeverity, "critical")
	d := DispatcherFromEnv(nil)
	if d.cooldown != 30*time.Second {
		t.Errorf("cooldown = %v, want 30s", d.cooldown)
	}
	if d.minRank != rankOf("critical") {
		t.Errorf("minRank = %d, want %d", d.minRank, rankOf("critical"))
	}
}

func TestDispatcherFromEnv_InvalidCooldownFallsBack(t *testing.T) {
	t.Setenv(envSlackWebhook, "https://example.com/hook")
	t.Setenv(envCooldown, "not-a-duration")
	if d := DispatcherFromEnv(nil); d.cooldown != defaultCooldown {
		t.Errorf("invalid cooldown should fall back to default, got %v", d.cooldown)
	}
}
