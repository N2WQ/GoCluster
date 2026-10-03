//go:build sqlite3_qualification && windows

package v15probe

import (
	"crypto/sha256"
	"database/sql"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"

	sqlite3 "github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/internal/qualificationfixture"
)

// Establish native target dot-component behavior before changing the shared
// walker. Literal raw substitute bytes are read back by the native fixture.
// Modernc's actual open and distinct committed sentinels are the oracle; the
// candidate's normalization never supplies expected target values.
func TestV15SQLiteNativeTargetDotCompatibility(t *testing.T) {
	originalDebug := os.Getenv("GODEBUG")
	for _, kind := range []string{"GUID", "DRIVE"} {
		for _, mode := range []string{"1", "0"} {
			for _, component := range []string{"dot", "parent"} {
				t.Run(kind+"/winreadlinkvolume="+mode+"/"+component, func(t *testing.T) {
					t.Setenv("GODEBUG", originalDebug+",winsymlink=1,winreadlinkvolume="+mode)
					root := t.TempDir()
					saved := map[string]string{
						filepath.Join(root, "a", "b", "kept.db"): "a-b",
						filepath.Join(root, "a", "c", "kept.db"): "a-c",
						filepath.Join(root, "b", "kept.db"):      "root-b",
						filepath.Join(root, "c", "kept.db"):      "root-c",
					}
					for path, value := range saved {
						if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
							t.Fatal(err)
						}
						seedGUIDDatabase(t, path, value)
					}
					target := root + `\a\.\b`
					if component == "parent" {
						target = root + `\a\b\..\c`
					}
					link := filepath.Join(root, "junction")
					if kind == "GUID" {
						qualificationfixture.GUIDJunction(t, root, link, target)
					} else {
						qualificationfixture.DriveJunction(t, root, link, target)
					}
					alias := link + `\kept.db`
					control, err := sql.Open("sqlite", alias)
					if err != nil {
						t.Fatal(err)
					}
					control.SetMaxOpenConns(1)
					openErr := control.Ping()
					var expected string
					if openErr == nil {
						if err := control.QueryRow("select v from kept").Scan(&expected); err != nil {
							control.Close()
							t.Fatal("modernc baseline opened without a known saved sentinel", err)
						}
					}
					if err := control.Close(); err != nil {
						t.Fatal(err)
					}
					selected := ""
					for path, value := range saved {
						if value == expected {
							selected = path
						}
					}
					if openErr == nil && selected == "" {
						t.Fatal("modernc selected no independently seeded target", expected)
					}
					before := nativeTargetFiles(t, root)
					// This artifact is emitted before any candidate open, including
					// refusal cases. It records all sentinels and their exact bytes.
					t.Logf("before candidate: modernc-open=%v selected=%q sentinel=%q saved=%v files=%x", openErr, selected, expected, saved, before)
					candidate, candidateErr := sqlite3.OpenTopologyContext(sqlite3.WithMaxMemory(t.Context(), 8<<20), alias)
					if candidate != nil {
						defer func() {
							if err := candidate.Retire(); err != nil {
								t.Error("retire native-dot candidate", err)
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
							t.Fatalf("candidate changed baseline refusal or saved state: candidate=%v before=%x after=%x", candidateErr, before, after)
						}
						return
					}
					if candidateErr != nil {
						t.Fatal("candidate refused native target accepted by modernc", candidateErr)
					}
					stmt, _, err := candidate.Prepare("select v from kept")
					if err != nil {
						t.Fatal(err)
					}
					actual := "<no row>"
					if stmt.Step() {
						actual = stmt.ColumnText(0)
					}
					queryErr, closeErr := stmt.Err(), stmt.Close()
					if actual != expected || queryErr != nil || closeErr != nil {
						t.Fatal("native target selected different saved contents", actual, expected, queryErr, closeErr)
					}
					if err := candidate.Exec("update kept set v='candidate-commit'"); err != nil {
						t.Fatal(err)
					}
					if err := candidate.Retire(); err != nil {
						t.Fatal(err)
					}
					for path, value := range saved {
						if path == selected {
							value = "candidate-commit"
						}
						assertGUIDDatabase(t, path, value)
					}
				})
			}
		}
	}
}

func nativeTargetFiles(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	files := make(map[string][32]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// WalkDir does not follow the junction; never hash through its target.
		if !entry.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsLocal(rel) {
			t.Fatal("native target inventory escaped owned root", path, err)
		}
		files[rel] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
