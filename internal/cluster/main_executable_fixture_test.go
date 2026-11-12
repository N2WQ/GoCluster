package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMainFixtureCompanionAbsence(t *testing.T) {
	work, image := t.TempDir(), t.TempDir()
	if err := mainFixtureCompanionError(work, image); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{work, image} {
		for _, name := range []string{"peerdiag", "peerdiag.exe"} {
			path := filepath.Join(directory, name)
			if err := os.WriteFile(path, []byte("unexpected companion"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := mainFixtureCompanionError(work, image); err == nil {
				t.Fatalf("accepted companion %s", path)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	}
}
