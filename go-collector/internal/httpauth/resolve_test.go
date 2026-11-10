package httpauth

import (
	"strings"
	"testing"
)

func TestParseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    bool
		wantErr bool
	}{
		{"required", true, false},
		{"disabled", false, false},
		{"", false, true},
		{"Required", false, true},
		{"none", false, true},
	}
	for _, c := range cases {
		got, err := ParseMode(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("ParseMode(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if err == nil && got != c.want {
			t.Errorf("ParseMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestResolveToken(t *testing.T) {
	t.Run("required without token is an error naming the remedies", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "")
		_, _, err := ResolveToken("required")
		if err == nil {
			t.Fatal("expected error when token unset and --http-auth=required")
		}
		for _, want := range []string{EnvAPIToken, "--http-auth=disabled"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should mention %q", err, want)
			}
		}
	})
	t.Run("whitespace-only token counts as unset", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "  ")
		if _, _, err := ResolveToken("required"); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("disabled without token is allowed and unauthenticated", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "")
		tok, unauth, err := ResolveToken("disabled")
		if err != nil || tok != "" || !unauth {
			t.Fatalf("got (%q, %v, %v), want (\"\", true, nil)", tok, unauth, err)
		}
	})
	t.Run("required with token is allowed", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "dummy-value")
		tok, unauth, err := ResolveToken("required")
		if err != nil || tok != "dummy-value" || unauth {
			t.Fatalf("got (%q, %v, %v)", tok, unauth, err)
		}
	})
	t.Run("disabled with token still enforces the token", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "dummy-value")
		tok, unauth, err := ResolveToken("disabled")
		if err != nil || tok != "dummy-value" || unauth {
			t.Fatalf("got (%q, %v, %v)", tok, unauth, err)
		}
	})
	t.Run("invalid mode is an error", func(t *testing.T) {
		t.Setenv(EnvAPIToken, "dummy-value")
		if _, _, err := ResolveToken("off"); err == nil {
			t.Fatal("expected error")
		}
	})
}
