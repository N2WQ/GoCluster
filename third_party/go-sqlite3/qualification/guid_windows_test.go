//go:build sqlite3_qualification && windows

package v15probe

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	sqlite3 "github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/internal/qualificationfixture"
)

// This is a differential baseline, not permission to change path semantics.
// The unchanged modernc driver determines the accepted saved-file target.
// Raw suffix concatenation is intentional: filepath.Join would erase the
// lexical '..' input before either implementation saw the adversarial case.
func TestV15SQLiteGUIDPathCompatibility(t *testing.T) {
	testV15SQLiteReparsePathCompatibility(t, qualificationfixture.GUIDJunction)
}

func TestV15SQLiteDrivePathCompatibility(t *testing.T) {
	testV15SQLiteReparsePathCompatibility(t, qualificationfixture.DriveJunction)
}

func testV15SQLiteReparsePathCompatibility(t *testing.T, createJunction func(*testing.T, string, string, string) string) {
	t.Helper()
	originalDebug := os.Getenv("GODEBUG")
	for _, mode := range []string{"1", "0"} {
		for _, scenario := range []string{"ordinary", "lexical-parent", "final-dot", "final-space", "extended-target"} {
			t.Run("winreadlinkvolume="+mode+"/"+scenario, func(t *testing.T) {
				t.Setenv("GODEBUG", originalDebug+",winsymlink=1,winreadlinkvolume="+mode)
				root := t.TempDir()
				target := filepath.Join(root, "real", "sub")
				if err := os.MkdirAll(target, 0o755); err != nil {
					t.Fatal(err)
				}
				physicalTarget := target
				if scenario == "extended-target" {
					target = root + `\literal. `
					physicalTarget = `\\?\` + target
					if err := os.Mkdir(physicalTarget, 0o755); err != nil {
						t.Fatal("create actual extended literal target", err)
					}
					rel, err := filepath.Rel(root, target)
					if err != nil || !filepath.IsAbs(target) || !filepath.IsLocal(rel) || rel == "." {
						t.Fatal("literal cleanup target escaped its owned root", target, err)
					}
					t.Cleanup(func() {
						// This exact, newly created literal directory is inside
						// the fixture root. The junction entry is removed first.
						if err := os.RemoveAll(physicalTarget); err != nil {
							t.Error("remove owned literal target", err)
						}
					})
				}
				link := filepath.Join(root, "junction")
				createJunction(t, root, link, target)
				direct := physicalTarget + `\kept.db`
				alias := link + `\kept.db`
				if scenario == "lexical-parent" {
					alias = link + `\..\kept.db`
					direct = filepath.Join(root, "kept.db")
					seedGUIDDatabase(t, filepath.Join(root, "real", "kept.db"), "wrong-resolved-parent")
				}
				if scenario == "final-dot" {
					alias += "."
				}
				if scenario == "final-space" {
					alias += " "
				}
				seedPath := direct
				if scenario == "extended-target" {
					// A literal \\?\ path contains the driver's DSN query
					// delimiter. Seed through the accepted ordinary alias,
					// then prove its physical identity with native file IDs.
					seedPath = alias
				}
				seedGUIDDatabase(t, seedPath, "committed-original")
				if scenario == "extended-target" {
					physicalInfo, physicalErr := os.Stat(direct)
					aliasInfo, aliasErr := os.Stat(alias)
					if physicalErr != nil || aliasErr != nil || !os.SameFile(physicalInfo, aliasInfo) {
						t.Fatal("extended fixture did not seed the literal native target", physicalErr, aliasErr)
					}
				}
				control, err := sql.Open("sqlite", alias)
				if err != nil {
					t.Fatal(err)
				}
				control.SetMaxOpenConns(1)
				defer control.Close()
				var expected string
				controlErr := control.QueryRow("select v from kept").Scan(&expected)
				candidate, candidateErr := sqlite3.OpenTopologyContext(sqlite3.WithMaxMemory(t.Context(), 8<<20), alias)
				if candidate != nil {
					defer func() {
						if err := candidate.Retire(); err != nil {
							t.Error("retire candidate", err)
						}
					}()
				}
				t.Logf("configured=%q modernc=%q/%v candidate-open=%v", alias, expected, controlErr, candidateErr)
				if controlErr != nil || expected != "committed-original" {
					t.Fatalf("modernc baseline did not establish the intended existing target: %q/%v", expected, controlErr)
				}
				if candidateErr != nil {
					t.Fatal("candidate refused a modernc-accepted saved file", candidateErr)
				}
				stmt, _, err := candidate.Prepare("select v from kept")
				if err != nil {
					t.Fatal(err)
				}
				value := "<no row>"
				if stmt.Step() {
					value = stmt.ColumnText(0)
				}
				queryErr, closeErr := stmt.Err(), stmt.Close()
				if value != expected || queryErr != nil || closeErr != nil {
					t.Fatalf("candidate selected a different saved target: got=%q expected=%q query=%v close=%v", value, expected, queryErr, closeErr)
				}
				if err := candidate.Exec("update kept set v='candidate-commit'"); err != nil {
					t.Fatal(err)
				}
				if err := control.QueryRow("select v from kept").Scan(&value); err != nil || value != "candidate-commit" {
					t.Fatal("independent modernc observer missed candidate commit", value, err)
				}
				if scenario == "lexical-parent" {
					assertGUIDDatabase(t, filepath.Join(root, "real", "kept.db"), "wrong-resolved-parent")
				}
				assertGUIDDatabase(t, seedPath, "candidate-commit")
			})
		}
	}
}

func seedGUIDDatabase(t *testing.T, path, value string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("create table kept(v text); insert into kept values(?)", value); err != nil {
		t.Fatal("seed independently owned saved file", path, err)
	}
}

func assertGUIDDatabase(t *testing.T, path, expected string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value, integrity string
	if err := db.QueryRow("select v from kept").Scan(&value); err != nil || value != expected {
		t.Fatal("saved contents differ", path, value, expected, err)
	}
	if err := db.QueryRow("pragma integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal("saved integrity differs", path, integrity, err)
	}
}

func TestV15SQLiteWALGUIDJunctionAlias(t *testing.T) {
	originalDebug := os.Getenv("GODEBUG")
	for _, mode := range []string{"1", "0"} {
		t.Run("winreadlinkvolume="+mode, func(t *testing.T) {
			t.Setenv("GODEBUG", originalDebug+",winsymlink=1,winreadlinkvolume="+mode)
			root := t.TempDir()
			target, link := filepath.Join(root, "real"), filepath.Join(root, "junction")
			if err := os.Mkdir(target, 0o755); err != nil {
				t.Fatal(err)
			}
			qualificationfixture.GUIDJunction(t, root, link, target)
			testWALProcessLocksAndCoherence(t, filepath.Join(target, "wal.db"), filepath.Join(link, "wal.db"))
		})
	}
}
