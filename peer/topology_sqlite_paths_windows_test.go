//go:build sqlite3_qualification && windows

package peer

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// A directory junction is actual Windows reparse coverage, but it does not
// replace the separate symbolic-link gate requiring SeCreateSymbolicLinkPrivilege.
func TestV15TopologyWindowsJunctionParity(t *testing.T) {
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, link := filepath.Join(root, "real"), filepath.Join(root, "junction")
	for _, path := range []string{target, link} {
		rel, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsAbs(path) || !filepath.IsLocal(rel) || rel == "." {
			t.Fatalf("fixture path escaped its owned root: %q %v", path, err)
		}
	}
	if err = os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	direct := filepath.Join(target, "kept.db")
	store, err := openTopologyStore(direct, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	observer := topologyTestDB(t, store)
	if _, err = observer.Exec("create table kept(v);insert into kept values('committed')"); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$ErrorActionPreference = 'Stop'
New-Item -ItemType Junction -Path $env:GOCLUSTER_TEST_JUNCTION_LINK -Target $env:GOCLUSTER_TEST_JUNCTION_TARGET | Out-Null`)
	command.Env = append(os.Environ(), "GOCLUSTER_TEST_JUNCTION_LINK="+link, "GOCLUSTER_TEST_JUNCTION_TARGET="+target)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual unprivileged Windows junction unavailable: %v %s", err, output)
	}
	// Remove only the junction entry before TempDir cleans its owned target.
	// There is no recursive operation through the reparse path.
	defer func() {
		if err := os.Remove(link); err != nil {
			t.Error("remove owned junction entry", err)
		}
	}()
	through := filepath.Join(link, "kept.db")
	originalDebug := os.Getenv("GODEBUG")
	for _, mode := range []string{"1", "0"} {
		t.Run("winsymlink="+mode, func(t *testing.T) {
			t.Setenv("GODEBUG", originalDebug+",winsymlink="+mode)
			resolved, resolveErr := filepath.EvalSymlinks(through)
			absolute, absErr := filepath.Abs(through)
			candidate, candidateErr := openTopologyStore(through, time.Hour)
			if candidate != nil {
				defer candidate.Close()
			}
			// The unchanged driver is the database-open compatibility oracle;
			// pinned Go's rejecting walker is only diagnostic evidence.
			control, err := sql.Open("sqlite", through)
			if err != nil {
				t.Fatal(err)
			}
			defer control.Close()
			var value string
			controlErr := control.QueryRow("select v from kept").Scan(&value)
			t.Logf("resolved=%q resolveErr=%v abs=%q absErr=%v candidateErr=%v moderncErr=%v value=%q", resolved, resolveErr, absolute, absErr, candidateErr, controlErr, value)
			if candidateErr != nil {
				t.Fatal("candidate refused an existing database through a junction", candidateErr)
			}
			if controlErr != nil || value != "committed" {
				t.Fatal("junction changed independently observed committed data", controlErr, value)
			}
			if err := candidate.db.run(t.Context(), func(conn *sqlite3.Conn) error {
				return conn.Exec("update kept set v='candidate'")
			}); err != nil {
				t.Fatal("candidate could not commit through junction", err)
			}
			if err := observer.QueryRow("select v from kept").Scan(&value); err != nil || value != "candidate" {
				t.Fatal("direct observer did not see junction commit", value, err)
			}
			if _, err := control.Exec("update kept set v='committed'"); err != nil {
				t.Fatal(err)
			}
		})
	}
	var integrity, value string
	if err = observer.QueryRow("pragma integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal("junction exercise changed saved database integrity", integrity, err)
	}
	if err = observer.QueryRow("select v from kept").Scan(&value); err != nil || value != "committed" {
		t.Fatal("junction exercise changed saved database value", value, err)
	}
	if topologyReservation.Load() != nil {
		t.Fatal("junction exercise retained a database owner")
	}
}
