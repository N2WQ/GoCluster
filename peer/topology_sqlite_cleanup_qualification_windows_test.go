//go:build sqlite3_qualification

package peer

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

func TestV15TopologyCleanupFailurePreventsReuse(t *testing.T) {
	const child = "GOCLUSTER_SQLITE_CLEANUP_CHILD"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15TopologyCleanupFailurePreventsReuse$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		if bytes, err := command.CombinedOutput(); err != nil {
			t.Fatalf("terminal owner subprocess: %v\n%s", err, bytes)
		}
		return
	}
	root := t.TempDir()
	path := filepath.Join(root, "main.db")
	reference := filepath.Join(root, "reference")
	if err := os.WriteFile(reference, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openTopologyDatabase(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	restore := vfs.FailMetadataCloseForQualification(reference)
	defer restore()
	uri := "file:" + filepath.ToSlash(filepath.Join(root, "attached.db")) + "?modeof=" + url.QueryEscape(reference)
	err = db.run(t.Context(), func(conn *sqlite3.Conn) error {
		first := conn.Exec("attach '" + strings.ReplaceAll(uri, "'", "''") + "' as broken")
		if !errors.Is(first, sqlite3.IOERR_CLOSE) {
			t.Fatal("fixture did not fail actual metadata close", first)
		}
		if next := conn.Exec("create table wrong(v)"); !errors.Is(next, sqlite3.IOERR_CLOSE) {
			t.Fatal("second SQL entered poisoned engine", next)
		}
		return first
	})
	if err == nil || !db.failedCleanup.Load() || db.conn == nil || !db.conn.CleanupFailed() {
		t.Fatal("adapter released failed owner", err)
	}
	called := false
	if err = db.run(t.Context(), func(*sqlite3.Conn) error { called = true; return nil }); err == nil || called {
		t.Fatal("later operation reached SQL", err, called)
	}
	if _, err = openTopologyDatabase(t.Context(), filepath.Join(root, "replacement.db")); !errors.Is(err, errTopologyReservation) {
		t.Fatal("replacement admitted", err)
	}
	if topologyReservation.Load() != db {
		t.Fatal("failed owner is not globally charged")
	}
	if db.Close() == nil {
		t.Fatal("terminal cleanup falsely completed")
	}
}
