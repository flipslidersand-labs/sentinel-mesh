package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew_CreatesDataDirWith0700(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o700 {
		t.Fatalf("new data dir perm = %o, want 700", got)
	}
}

func TestNew_DoesNotChmodExistingDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck
	fi, _ := os.Stat(dir)
	if got := fi.Mode().Perm(); got != 0o750 {
		t.Fatalf("existing dir perm changed to %o", got)
	}
}
