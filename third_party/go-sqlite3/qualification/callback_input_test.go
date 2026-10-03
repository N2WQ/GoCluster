//go:build sqlite3_qualification

package v15probe

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestV15SQLiteModerncGeneratedCallbackInput(t *testing.T) {
	kind := os.Getenv("V15_SQLITE_PRIOR_CALLBACK_CHILD")
	if kind == "" {
		for _, kind := range []string{"key", "value", "psow", "vfs", "pragma"} {
			t.Run(kind, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15SQLiteModerncGeneratedCallbackInput$", "-test.v", "-test.count=1")
				cmd.Env = append(os.Environ(), "V15_SQLITE_PRIOR_CALLBACK_CHILD="+kind)
				out, err := cmd.CombinedOutput()
				t.Logf("prior modernc %s:\n%s", kind, out)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
		return
	}
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "main.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := db.Exec("CREATE TABLE kept(v); INSERT INTO kept VALUES('saved')"); err != nil {
		t.Fatal(err)
	}
	uri := "'file:" + strings.ReplaceAll(filepath.ToSlash(filepath.Join(root, "attached.db")), "'", "''")
	const value = "printf('%1100000s','x')"
	var statement string
	switch kind {
	case "key":
		statement = uri + "?' || " + value + " || '=ignored'"
	case "value":
		statement = uri + "?unrelated=' || " + value
	case "psow":
		statement = uri + "?psow=1' || " + value
	case "vfs":
		statement = uri + "?vfs=' || " + value
	case "pragma":
		statement = "SELECT * FROM pragma_table_info(" + value + ")"
	default:
		t.Fatal("unknown fixture")
	}
	if kind != "pragma" {
		statement = "ATTACH " + statement + " AS attached; CREATE TABLE attached.kept(v); INSERT INTO attached.kept VALUES('attached'); DETACH attached"
	}
	_, err = db.Exec(statement)
	t.Logf("short SQL bytes=%d; generated1100000; result=%.80v", len(statement), err)
	if kind == "vfs" {
		if err == nil || !strings.Contains(err.Error(), "no such vfs") {
			t.Fatal("prior unknown VFS refusal not established", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	var kept string
	if err := db.QueryRow("SELECT v FROM kept").Scan(&kept); err != nil || kept != "saved" {
		t.Fatal("saved main data changed", kept, err)
	}
}
