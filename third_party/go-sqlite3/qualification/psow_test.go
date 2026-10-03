//go:build sqlite3_qualification

package v15probe

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	bounded "github.com/ncruces/go-sqlite3"
	"modernc.org/libc"
	prior "modernc.org/sqlite/lib"
)

type psowObservation struct {
	Query string
	Value bool
}

func TestV15SQLitePSOWCompatibility(t *testing.T) {
	queries := []string{"", "psow=", "psow=0", "psow=1", "psow=01", "psow=256", "psow=257", "psow=512", "psow=0x100", "psow=0x101", "psow=2147483648", "psow=9223372036854775808", "psow=1suffix", "psow=0suffix", "psow=%2B1", "psow=%201", "psow=TRUE", "psow=false", "psow=off", "psow=yes", "psow=%FF", "psow=0&psow=1", "psow=1&psow=0"}
	driver := os.Getenv("V15_SQLITE_PSOW_CHILD")
	if driver == "" {
		var observations [2][]psowObservation
		for i, driver := range []string{"modernc", "candidate"} {
			outPath := filepath.Join(t.TempDir(), "result.json")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15SQLitePSOWCompatibility$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), "V15_SQLITE_PSOW_CHILD="+driver, "V15_SQLITE_PSOW_RESULT="+outPath)
			out, err := cmd.CombinedOutput()
			cancel()
			t.Logf("%s isolated results:\n%s", driver, out)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &observations[i]); err != nil {
				t.Fatal(err)
			}
			if len(observations[i]) != len(queries) {
				t.Fatal("incomplete independent observations", driver, len(observations[i]), len(queries))
			}
			for row, observation := range observations[i] {
				if observation.Query != queries[row] {
					t.Fatal("independent input identity changed", driver, row, observation.Query)
				}
			}
		}
		for i, before := range observations[0] {
			if !reflect.DeepEqual(before, observations[1][i]) {
				t.Errorf("PSOW behavior differs: prior=%+v candidate=%+v", before, observations[1][i])
			}
		}
		return
	}
	var rows []psowObservation
	for _, query := range queries {
		path := filepath.Join(t.TempDir(), "kept.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE kept(v); INSERT INTO kept VALUES('saved')"); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		uri := "file:" + filepath.ToSlash(path)
		if query != "" {
			uri += "?" + query
		}
		var value bool
		if driver == "modernc" {
			value = priorPSOW(t, uri)
		} else if driver == "candidate" {
			conn, err := bounded.OpenTopologyContext(bounded.WithMaxMemory(t.Context(), 8<<20), uri)
			if err != nil {
				t.Fatal(err)
			}
			got, err := conn.FileControl("main", bounded.FCNTL_POWERSAFE_OVERWRITE)
			if err != nil {
				t.Fatal(err)
			}
			value = got.(bool)
			if err := conn.Retire(); err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatal("unknown fixture driver")
		}
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		var saved string
		if err := db.QueryRow("SELECT v FROM kept").Scan(&saved); err != nil || saved != "saved" {
			t.Fatal("saved data changed", saved, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("query=%q PSOW=%v", query, value)
		rows = append(rows, psowObservation{query, value})
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("V15_SQLITE_PSOW_RESULT"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

// The prior driver's exact generated engine/open flags, without reflection into
// its private driver conn. All pointer arguments belong to its libc allocator.
func priorPSOW(t *testing.T, uri string) bool {
	tls := libc.NewTLS()
	defer tls.Close()
	name, err := libc.CString(uri)
	if err != nil {
		t.Fatal(err)
	}
	defer libc.Xfree(tls, name)
	args := libc.Xcalloc(tls, 1, 16)
	if args == 0 {
		t.Fatal("fixture allocation failed")
	}
	defer libc.Xfree(tls, args)
	rc := prior.Xsqlite3_open_v2(tls, name, args, prior.SQLITE_OPEN_READWRITE|prior.SQLITE_OPEN_CREATE|prior.SQLITE_OPEN_FULLMUTEX|prior.SQLITE_OPEN_URI, 0)
	// The generated library's supported accessor borrows this libc-owned
	// block; avoid private-driver reflection or a Go-pointer/uintptr roundtrip.
	block := libc.GoBytes(args, 16)
	var db uintptr
	if unsafe.Sizeof(uintptr(0)) == 8 {
		db = uintptr(binary.NativeEndian.Uint64(block))
	} else {
		db = uintptr(binary.NativeEndian.Uint32(block))
	}
	if db != 0 {
		defer func() {
			if rc := prior.Xsqlite3_close_v2(tls, db); rc != 0 {
				t.Error("prior close", rc)
			}
		}()
	}
	if rc != 0 {
		t.Fatal("prior open", rc)
	}
	binary.NativeEndian.PutUint32(block[8:], ^uint32(0))
	if rc := prior.Xsqlite3_file_control(tls, db, 0, prior.SQLITE_FCNTL_POWERSAFE_OVERWRITE, args+8); rc != 0 {
		t.Fatal("prior file control", rc)
	}
	return binary.NativeEndian.Uint32(block[8:]) != 0
}
