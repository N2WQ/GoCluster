package sqlite3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// Run a real SQL-generated callback value in a bounded child. Input SQL stays
// short; the8MiB engine creates600,000/1,100,000-byte values. This distinguishes
// callback conversion from the unrelated64KiB configured-DSN input bound.
func TestV15SQLiteGeneratedCallbackBacking(t *testing.T) {
	if kind := os.Getenv("V15_SQLITE_CALLBACK_CHILD"); kind != "" {
		topologyCallbackChild(t, kind)
		return
	}
	for _, kind := range []string{"unknown-uri-value", "unknown-uri-key", "psow", "pragma", "unknown-vfs", "large-unknown-uri-value", "large-unknown-uri-key", "large-psow", "large-pragma", "large-unknown-vfs"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15SQLiteGeneratedCallbackBacking$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), "V15_SQLITE_CALLBACK_CHILD="+kind)
			output, err := cmd.CombinedOutput()
			t.Logf("bounded child %s:\n%s", kind, output)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func topologyCallbackChild(t *testing.T, kind string) {
	if profile := os.Getenv("V15_SQLITE_CALLBACK_PROFILE"); profile != "" {
		runtime.MemProfileRate = 1
		defer func() {
			f, err := os.Create(profile + "-" + kind + ".pprof")
			if err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
				t.Error(err)
			}
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	root := t.TempDir()
	length := 600000
	if strings.HasPrefix(kind, "large-") {
		length = 1100000
		kind = strings.TrimPrefix(kind, "large-")
	}
	generated := fmt.Sprintf("printf('%%%ds','x')", length)
	c, err := OpenTopologyContext(WithMaxMemory(t.Context(), 8<<20), filepath.Join(root, "main.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Retire(); err != nil {
			t.Error(err)
		}
	}()
	if err := c.Exec("CREATE TABLE kept(v); INSERT INTO kept VALUES('saved')"); err != nil {
		t.Fatal(err)
	}
	uri := "file:" + filepath.ToSlash(filepath.Join(root, "attached.db"))
	quoted := "'" + strings.ReplaceAll(uri, "'", "''")
	var sql string
	switch kind {
	case "unknown-uri-value":
		sql = quoted + "?unrelated=' || " + generated + " || '&psow=1suffix'"
	case "unknown-uri-key":
		sql = quoted + "?' || " + generated + " || '=ignored&psow=0suffix'"
	case "psow":
		sql = quoted + "?psow=1' || " + generated
	case "pragma":
		sql = "SELECT * FROM pragma_table_info(" + generated + ")"
	case "unknown-vfs":
		sql = quoted + "?vfs=' || " + generated
	default:
		t.Fatal("unknown bounded fixture", kind)
	}
	if kind != "pragma" {
		sql = "ATTACH " + sql + " AS attached; CREATE TABLE attached.kept(v); INSERT INTO attached.kept VALUES('attached')"
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = c.Exec(sql)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("SQL bytes=%d; generated value=%d; Go allocated bytes=%d; result=%.80v", len(sql), length, allocated, err)
	if kind == "unknown-vfs" {
		if !errors.Is(err, ERROR) || !strings.Contains(err.Error(), "no such vfs") {
			t.Fatal("unknown VFS changed ordinary refusal", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if kind != "pragma" && kind != "unknown-vfs" {
		got, err := c.FileControl("attached", FCNTL_POWERSAFE_OVERWRITE)
		if err != nil || got != (kind != "unknown-uri-key") {
			t.Fatal("psow callback result changed", got, err)
		}
		if err := c.Exec("DETACH attached"); err != nil {
			t.Fatal(err)
		}
	}
	statement, _, err := c.Prepare("SELECT v FROM kept")
	if err != nil {
		t.Fatal(err)
	}
	if !statement.Step() || statement.ColumnText(0) != "saved" {
		t.Fatal("main committed sentinel changed", statement.Err())
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	// Allocated bytes deliberately overcounts unreachable garbage. A passing
	// ceiling falsifies these large copies; it is not the full host proof.
	if allocated > 96<<10 {
		t.Fatal(fmt.Sprintf("generated callback allocated %d Go bytes; fixed host conversion must stay below96KiB", allocated))
	}
}

func TestV15SQLiteChecksumCallbackState(t *testing.T) {
	c, err := OpenTopologyContext(WithMaxMemory(t.Context(), 8<<20), filepath.Join(t.TempDir(), "checksums.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Retire(); err != nil {
			t.Error(err)
		}
	}()
	if err := c.EnableChecksums(""); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec("CREATE TABLE kept(v); INSERT INTO kept VALUES('saved')"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"0suffix", 0}, {"1suffix", 1}, {"FALSE", 0}, {"TRUE", 1},
		{strings.Repeat("x", 21), 1}, {"off", 0}, {"\xff", 0}, {"9" + strings.Repeat("x", 600000), 1},
	} {
		stmt, _, err := c.Prepare("PRAGMA checksum_verification='" + tc.value + "'")
		if err != nil {
			t.Fatal(err)
		}
		if !stmt.Step() || stmt.ColumnInt(0) != tc.want {
			t.Fatal("real checksum result", len(tc.value), tc.want, stmt.Err())
		}
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Exec("PRAGMA page_size=8192"); err != nil {
		t.Fatal(err)
	}
	stmt, _, err := c.Prepare("PRAGMA page_size")
	if err != nil {
		t.Fatal(err)
	}
	if !stmt.Step() || stmt.ColumnInt(0) != 4096 {
		t.Fatal("checksum page_size refusal changed", stmt.Err())
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
}
