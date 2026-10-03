//go:build sqlite3_qualification && windows

package v15probe

import (
	"database/sql"
	"errors"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// This uses actual prior-driver opens, not Go's WTF-8 conversion, as oracle.
// Its URI decoder supplies exactly the escaped byte sequence to the VFS.
func TestV15SQLiteMalformedURIPathCompatibility(t *testing.T) {
	for _, vector := range []struct {
		name  string
		bytes []byte
	}{
		{"valid-unicode", []byte("café-😀")}, {"invalid-byte", []byte{0xff}},
		{"overlong", []byte{0xc0, 0xaf}}, {"unpaired-surrogate", []byte{0xed, 0xa0, 0x80}},
		{"incomplete", []byte{0xe2, 0x82}}, {"out-of-range", []byte{0xf4, 0x90, 0x80, 0x80}},
	} {
		t.Run(vector.name, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			dsn := "file:" + url.PathEscape("probe-"+string(vector.bytes)+".db")
			control, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			control.SetMaxOpenConns(1)
			openErr := control.Ping()
			var selected string
			if openErr == nil {
				if _, err := control.Exec("create table kept(v);insert into kept values('prior-commit')"); err != nil {
					control.Close()
					t.Fatal(err)
				}
				var sequence int
				var schema string
				if err := control.QueryRow("pragma database_list").Scan(&sequence, &schema, &selected); err != nil {
					control.Close()
					t.Fatal(err)
				}
			}
			if err := control.Close(); err != nil {
				t.Fatal(err)
			}
			before := nativeTargetFiles(t, root)
			t.Logf("before candidate: URI=%q raw=%x modernc-open=%v selected=%q saved-files=%x", dsn, vector.bytes, openErr, selected, before)
			candidate, candidateErr := sqlite3.OpenTopologyContext(sqlite3.WithMaxMemory(t.Context(), 8<<20), dsn)
			if candidate != nil {
				defer func() {
					if err := candidate.Retire(); err != nil {
						t.Error("retire malformed-URI candidate", err)
					}
				}()
			}
			if openErr != nil {
				if candidate != nil {
					if err := candidate.Retire(); err != nil {
						t.Fatal(err)
					}
				}
				after := nativeTargetFiles(t, root)
				if candidateErr == nil || !maps.Equal(before, after) {
					t.Fatal("candidate changed prior refusal/files", candidateErr, before, after)
				}
				return
			}
			if candidateErr != nil {
				t.Fatal("candidate refused prior accepted malformed URI", candidateErr)
			}
			stmt, _, err := candidate.Prepare("select v from kept")
			if err != nil {
				t.Fatalf("candidate selected another file: %v files=%x", err, nativeTargetFiles(t, root))
			}
			value := "<no row>"
			if stmt.Step() {
				value = stmt.ColumnText(0)
			}
			queryErr, closeErr := stmt.Err(), stmt.Close()
			if value != "prior-commit" || queryErr != nil || closeErr != nil {
				t.Fatal("saved malformed-URI target changed", value, queryErr, closeErr)
			}
			if err := candidate.Exec("update kept set v='candidate-commit'"); err != nil {
				t.Fatal(err)
			}
			if err := candidate.Retire(); err != nil {
				t.Fatal(err)
			}
			assertGUIDDatabase(t, dsn, "candidate-commit")
		})
	}
}

func TestV15SQLiteWholeFileSymlinkWALCompatibility(t *testing.T) {
	root := t.TempDir()
	target, alias := filepath.Join(root, "target.db"), filepath.Join(root, "alias.db")
	// A valid empty file is enough for SQLite to initialize through the alias.
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, alias); err != nil {
		if errors.Is(err, syscall.Errno(1314)) && os.Getenv("GOCLUSTER_SQLITE_SYMLINK_REQUIRED") != "1" {
			t.Skip("actual Windows file symlink unavailable; required qualification sets GOCLUSTER_SQLITE_SYMLINK_REQUIRED=1")
		}
		t.Fatal("actual Windows file symlink fixture unavailable", err)
	}
	// All three processes use the inherited same alias, so their sidecars must
	// be alias-derived. This deliberately does not assert cross-alias WAL safety.
	testWALProcessLocksAndCoherence(t, alias, alias)
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(alias + suffix); err != nil {
			t.Fatal("missing inherited alias sidecar", suffix, err)
		}
		if _, err := os.Stat(target + suffix); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("candidate introduced target-named sidecar", suffix, err)
		}
	}
}
