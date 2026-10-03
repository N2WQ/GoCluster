//go:build sqlite3_qualification && windows

package v15probe

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestV15SQLiteWALJunctionAlias(t *testing.T) {
	originalDebug := os.Getenv("GODEBUG")
	for _, mode := range []string{"1", "0"} {
		t.Run("winsymlink="+mode, func(t *testing.T) {
			t.Setenv("GODEBUG", originalDebug+",winsymlink="+mode)
			root, err := filepath.Abs(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			target, link := filepath.Join(root, "real"), filepath.Join(root, "junction")
			for _, path := range []string{target, link} {
				rel, err := filepath.Rel(root, path)
				if err != nil || !filepath.IsAbs(path) || !filepath.IsLocal(rel) || rel == "." {
					t.Fatal("junction fixture escaped owned root", path, err)
				}
			}
			if err := os.Mkdir(target, 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$ErrorActionPreference = 'Stop'
New-Item -ItemType Junction -Path $env:GOCLUSTER_TEST_JUNCTION_LINK -Target $env:GOCLUSTER_TEST_JUNCTION_TARGET | Out-Null`)
			cmd.Env = append(os.Environ(), "GOCLUSTER_TEST_JUNCTION_LINK="+link, "GOCLUSTER_TEST_JUNCTION_TARGET="+target)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("create actual Windows junction: %v %s", err, output)
			}
			// Registered before child processes: they close first. Remove only
			// the junction entry before TempDir recursively removes its root.
			t.Cleanup(func() {
				if err := os.Remove(link); err != nil {
					t.Error(err)
				}
			})
			testWALProcessLocksAndCoherence(t, filepath.Join(target, "wal.db"), filepath.Join(link, "wal.db"))
		})
	}
}
