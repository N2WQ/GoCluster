//go:build sqlite3_qualification && windows

package v15probe

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

type dataDirectoryResult struct {
	Driver      string
	PragmaError string
	LaterValue  string
	LaterError  string
}

// Process isolation is necessary: the prior pragma changes modernc GLOBAL
// state consumed by later unrelated modernc opens, as dashboard/ULS do.
func TestV15SQLiteDataDirectoryCompatibility(t *testing.T) {
	var results [2]dataDirectoryResult
	for i, driver := range []string{"modernc", "candidate"} {
		root := t.TempDir()
		result := filepath.Join(root, "result.json")
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15SQLiteDataDirectoryProcessHelper$", "-test.v", "-test.count=1")
		command.Env = append(os.Environ(), "V15_SQLITE_DATADIR_DRIVER="+driver, "V15_SQLITE_DATADIR_ROOT="+root, "V15_SQLITE_DATADIR_RESULT="+result)
		output, err := command.CombinedOutput()
		cancel()
		t.Logf("%s isolated output:\n%s", driver, output)
		if err != nil {
			t.Fatal("isolated data-directory fixture failed", driver, err)
		}
		data, err := os.ReadFile(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &results[i]); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("isolated prior/candidate data-directory results: %+v", results)
	if results[0].PragmaError != "" || results[0].LaterError != "" || results[0].LaterValue != "redirected" {
		t.Fatal("prior-driver fixture did not establish the configured redirect", results[0])
	}
	if results[1].PragmaError != results[0].PragmaError || results[1].LaterError != results[0].LaterError || results[1].LaterValue != results[0].LaterValue {
		t.Fatal("configured topology pragma no longer preserves later modernc file target", results)
	}
}

func TestV15SQLiteDataDirectoryProcessHelper(t *testing.T) {
	driver := os.Getenv("V15_SQLITE_DATADIR_DRIVER")
	if driver == "" {
		return
	}
	root := os.Getenv("V15_SQLITE_DATADIR_ROOT")
	output := os.Getenv("V15_SQLITE_DATADIR_RESULT")
	if !filepath.IsAbs(root) || filepath.Dir(output) != root {
		t.Fatal("fixture result escaped its owned root")
	}
	t.Chdir(root)
	if err := os.Mkdir("redirect", 0o755); err != nil {
		t.Fatal(err)
	}
	base, redirected := filepath.Join(root, "later.db"), filepath.Join(root, "redirect", "later.db")
	seedGUIDDatabase(t, base, "base")
	seedGUIDDatabase(t, redirected, "redirected")
	const pragma = "data_store_directory('redirect')"
	result := dataDirectoryResult{Driver: driver}
	if driver == "modernc" {
		db, err := sql.Open("sqlite", "topology.db?_pragma="+url.QueryEscape(pragma))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Ping(); err != nil {
			result.PragmaError = err.Error()
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	} else if driver == "candidate" {
		// The production adapter removes private DSN parameters, opens the
		// main file, then streams exactly these pragmas onto the connection.
		db, err := sqlite3.OpenTopologyContext(sqlite3.WithMaxMemory(t.Context(), 8<<20), "topology.db")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("pragma " + pragma); err != nil {
			result.PragmaError = err.Error()
		}
		if err := db.Retire(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("unknown fixture driver", driver)
	}
	later, err := sql.Open("sqlite", "later.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := later.QueryRow("select v from kept").Scan(&result.LaterValue); err != nil {
		result.LaterError = err.Error()
	}
	if err := later.Close(); err != nil {
		t.Fatal(err)
	}
	// Absolute independent observers are unaffected by the global redirect.
	assertGUIDDatabase(t, base, "base")
	assertGUIDDatabase(t, redirected, "redirected")
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
