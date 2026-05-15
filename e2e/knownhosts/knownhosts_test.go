package knownhosts

import (
	"os"
	"testing"
)

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	if err := Save(dir, "my-server", "aabbccddeeff"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	m, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := m["my-server"]; got != "aabbccddeeff" {
		t.Errorf("got %q, want %q", got, "aabbccddeeff")
	}
}

func TestLoad_Missing(t *testing.T) {
	dir := t.TempDir()
	// Remove the file so it definitely doesn't exist
	os.Remove(hostsFile(dir))

	m, err := Load(dir)
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}
