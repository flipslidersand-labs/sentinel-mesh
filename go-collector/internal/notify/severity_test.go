package notify

import (
	"testing"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
)

// Ordering must come from the alerting constants only; former aliases are no
// longer recognised.
func TestRankOf_UsesAlertingVocabularyOnly(t *testing.T) {
	if !(rankOf(alerting.SeverityInfo) < rankOf(alerting.SeverityWarning) &&
		rankOf(alerting.SeverityWarning) < rankOf(alerting.SeverityCritical)) {
		t.Fatal("info < warning < critical must hold")
	}
	for _, alias := range []string{"low", "warn", "medium", "high", "error", "CRITICAL", " warning "} {
		if knownSeverity(alias) {
			t.Errorf("%q must not be a known severity", alias)
		}
		if rankOf(alias) != 0 {
			t.Errorf("%q must rank as unknown (0)", alias)
		}
	}
}
