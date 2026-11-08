package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// SlackNotifier posts alerts to a Slack incoming webhook.
type SlackNotifier struct {
	webhookURL string
	client     *http.Client
}

// NewSlackNotifier creates a Slack notifier for the given incoming-webhook URL.
func NewSlackNotifier(webhookURL string, client *http.Client) *SlackNotifier {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &SlackNotifier{webhookURL: webhookURL, client: client}
}

// Name implements Notifier.
func (s *SlackNotifier) Name() string { return "slack" }

// slackPayload is the minimal Slack incoming-webhook body.
type slackPayload struct {
	Text string `json:"text"`
}

// slackEscaper neutralises Slack mrkdwn control characters: & < > are
// HTML-escaped (blocking <url|text> links and <!channel> mentions) and
// backticks are removed (blocking code-span breakout).
var slackEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "`", "")

// formatSlackText renders a human-readable alert line for Slack.
func formatSlackText(a store.Alert) string {
	e := slackEscaper.Replace
	return fmt.Sprintf(
		":rotating_light: *[%s]* %s\n> node: `%s`  rule: `%s`  event: `%s`  at %s",
		e(a.Severity), e(a.Message), e(a.NodeID), e(a.RuleID), e(a.EventID),
		a.Timestamp.Format(time.RFC3339),
	)
}

// Send implements Notifier.
func (s *SlackNotifier) Send(ctx context.Context, a store.Alert) error {
	body, err := json.Marshal(slackPayload{Text: formatSlackText(a)})
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		// *url.Error embeds the full request URL, which contains the secret
		// webhook path. Keep only the underlying cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("post to slack: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}
	return nil
}
