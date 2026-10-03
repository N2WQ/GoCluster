package peer

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

func TestV15TopologyDSNCompatibility(t *testing.T) {
	for _, query := range []string{"", "?_pragma=cache_size(9)&_pragma=cache_size(7)", "?_pragma=busy_timeout(123)&_pragma=foreign_keys(1)&_pragma=trusted_schema(0)", "?_txlock=&_txlock=exclusive&_time_format=&_time_format=invalid", "?_pragma=cache_size%28", "?x=%ZZ", "?_txlock=invalid", "?_time_format=invalid"} {
		t.Run(query, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "candidate.db") + query
			control, err := sql.Open("sqlite", filepath.Join(dir, "control.db")+query)
			if err != nil {
				t.Fatal(err)
			}
			defer control.Close()
			control.SetMaxOpenConns(1)
			controlErr := control.PingContext(t.Context())
			store, err := openTopologyStore(path, time.Hour)
			if (err == nil) != (controlErr == nil) {
				t.Fatalf("candidate=%v modernc=%v", err, controlErr)
			}
			if err != nil {
				return
			}
			defer store.Close()
			for _, pragma := range []string{"cache_size", "busy_timeout", "foreign_keys", "trusted_schema"} {
				var want, got int64
				if err = control.QueryRowContext(t.Context(), "pragma "+pragma).Scan(&want); err != nil {
					t.Fatal(err)
				}
				err = store.db.run(t.Context(), func(c *sqlite3.Conn) error {
					stmt, _, err := c.Prepare("pragma " + pragma)
					if err != nil {
						return err
					}
					defer stmt.Close()
					if !stmt.Step() {
						return stmt.Err()
					}
					got = stmt.ColumnInt64(0)
					return nil
				})
				if err != nil || got != want {
					t.Fatalf("%s candidate=%d modernc=%d err=%v", pragma, got, want, err)
				}
			}
		})
	}
}

func TestV15TopologyConfiguredMmapReadWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapped.db")
	store, err := openTopologyStore(path+"?_pragma=mmap_size(1048576)", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	err = store.db.transaction(t.Context(), func(tx *topologyTransaction) error {
		if err := tx.Exec("create table kept(id integer primary key, value text)"); err != nil {
			return err
		}
		return tx.Exec("insert into kept values(1,?)", "committed")
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := topologyTestDB(t, store)
	var value string
	if err = observer.QueryRowContext(t.Context(), "select value from kept where id=1").Scan(&value); err != nil || value != "committed" {
		t.Fatalf("independent commit observer: %q %v", value, err)
	}
	if _, err = observer.ExecContext(t.Context(), "update kept set value='external' where id=1"); err != nil {
		t.Fatal(err)
	}
	err = store.db.run(t.Context(), func(c *sqlite3.Conn) error {
		s, _, err := c.Prepare("select value from kept where id=1")
		if err != nil {
			return err
		}
		defer s.Close()
		if !s.Step() {
			return errors.New("configured mmap read produced no row")
		}
		value = s.ColumnText(0)
		return s.Err()
	})
	if err != nil || value != "external" {
		t.Fatalf("external commit visible: %q %v", value, err)
	}
	if runtime.GOOS == "windows" {
		err = store.db.run(t.Context(), func(c *sqlite3.Conn) error {
			s, _, err := c.Prepare("pragma mmap_size")
			if err != nil {
				return err
			}
			defer s.Close()
			if s.Step() {
				return errors.New("Windows no-mapper fixture unexpectedly exposes mmap pragma row")
			}
			return s.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestV15TopologyDSNBoundedParse(t *testing.T) {
	program, err := parseTopologyDSN("file:test.db?mode=rwc&_pragma=cache_size%289%29&_pragma=cache_size%287%29&_txlock=immediate")
	if err != nil || program.filename != "file:test.db?mode=rwc" || program.begin != "begin immediate" || program.count != 2 || program.pragmas[0].sql != "cache_size(7)" {
		t.Fatalf("URI parse: %+v %v", program, err)
	}
	if _, err = parseTopologyDSN("test.db?" + strings.Repeat("_pragma=cache_size(7)&", 129)); !errors.Is(err, errTopologyBudget) {
		t.Fatalf("pragma table overflow not refused: %v", err)
	}
	if _, err = parseTopologyDSN(strings.Repeat("x", topologyDSNBytes+1)); !errors.Is(err, errTopologyBudget) {
		t.Fatalf("DSN byte overflow not refused: %v", err)
	}
}

func TestV15TopologyDSNLowercaseCompatibility(t *testing.T) {
	values := []string{"", "BUSY_TIMEOUT(3)", "busy_timeout(1)", " busy_timeout(2) ", "cache_size(7)", "CACHE_SIZE(9)", "İ", "i", "Ⱥ", "ⱥ", "ſ", "s", "\xff", "\xfe\xff", "\ufffd", "\u2003Ⱥ \xff\u2003", "\x00A", "EXCLUSIVE", "excluſive"}
	for _, a := range values {
		for _, b := range values {
			if got, want := topologyLowerCompare(a, b), strings.Compare(strings.ToLower(a), strings.ToLower(b)); got != want {
				t.Fatalf("compare %q/%q got=%d want=%d", a, b, got, want)
			}
		}
		if topologyHasBusyTimeoutPrefix(strings.TrimSpace(a)) != strings.HasPrefix(strings.TrimSpace(strings.ToLower(a)), "busy_timeout") {
			t.Fatalf("busy_timeout prefix differs for %q", a)
		}
	}
	// Trimming before lowercasing must commute for every Unicode scalar. This
	// justifies the borrowed key, separately from the frozen ordering vectors.
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if unicode.IsSpace(r) != unicode.IsSpace(unicode.ToLower(r)) {
			t.Fatalf("case mapping changes whitespace: %U", r)
		}
	}
	want := append([]string(nil), values...)
	sort.Slice(want, func(i, j int) bool {
		x, y := strings.TrimSpace(strings.ToLower(want[i])), strings.TrimSpace(strings.ToLower(want[j]))
		if strings.HasPrefix(x, "busy_timeout") {
			return true
		}
		if strings.HasPrefix(y, "busy_timeout") {
			return false
		}
		return x < y
	})
	query := "test.db?"
	for _, value := range values {
		query += "_pragma=" + value + "&"
	}
	program, err := parseTopologyDSN(query)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if program.pragmas[i].sql != want[i] {
			t.Fatalf("sorted row %d got=%q want=%q", i, program.pragmas[i].sql, want[i])
		}
	}
	for _, value := range []string{"EXCLUSIVE", "excluſive", "İMMEDİATE", "\xff"} {
		_, err := parseTopologyDSN("test.db?_txlock=" + value)
		lower := strings.ToLower(value)
		wantAccepted := lower == "exclusive" || lower == "immediate" || lower == "deferred"
		if (err == nil) != wantAccepted {
			t.Fatalf("txlock %q accepted=%t want=%t", value, err == nil, wantAccepted)
		}
	}
	malformed := strings.Repeat("\xff", topologyDSNBytes-64)
	loweredMalformed := strings.ToLower(malformed)
	if allocations := testing.AllocsPerRun(20, func() {
		if topologyLowerCompare(malformed, loweredMalformed) != 0 || topologyHasBusyTimeoutPrefix(malformed) {
			panic("malformed comparison changed")
		}
	}); allocations != 0 {
		t.Fatalf("malformed lowercase stream allocated: %g", allocations)
	}
	if _, err := parseTopologyDSN("file:test.db?_pragma=" + malformed); err != nil {
		t.Fatal("bounded malformed SQL changed parser admission", err)
	}
}

func TestV15TopologyDirectoryBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ordinary", "nested", "saved.db")
	store, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal("normal directory creation failed", err)
	}
	observer := topologyTestDB(t, store)
	if _, err = observer.ExecContext(t.Context(), "create table kept(v);insert into kept values('unchanged')"); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{strings.Repeat("x", 2048), strings.Repeat("x"+string(filepath.Separator), 600)} {
		first := filepath.Join(dir, "must-not-create")
		candidate, err := openTopologyStore(filepath.Join(first, suffix, "refused.db"), time.Hour)
		if candidate != nil || !errors.Is(err, errTopologyBudget) {
			t.Fatalf("oversized directory admitted: %v %v", candidate, err)
		}
		if _, err = os.Stat(first); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refusal modified directory tree: %v", err)
		}
		if topologyReservation.Load() != nil {
			t.Fatal("directory refusal retained reservation")
		}
	}
	var value string
	if err = observer.QueryRowContext(t.Context(), "select v from kept").Scan(&value); err != nil || value != "unchanged" {
		t.Fatalf("saved value changed: %q %v", value, err)
	}
	if err = observer.QueryRowContext(t.Context(), "pragma integrity_check").Scan(&value); err != nil || value != "ok" {
		t.Fatalf("saved file failed integrity check: %q %v", value, err)
	}
}

func TestV15TopologyModeofBudgetAndParity(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	reference := filepath.Join(dir, "reference")
	if err := os.WriteFile(reference, []byte("mode source"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Preserve the adapter's historical directory interpretation of URI DSNs:
	// a relative file URI needs no synthetic "file:D:" parent directory.
	path := "candidate.db"
	dsn := func(path, modeof string) string {
		return "file:" + filepath.ToSlash(path) + "?modeof=" + url.QueryEscape(modeof)
	}
	store, err := openTopologyStore(dsn(path, reference), time.Hour)
	if err != nil {
		t.Fatal("normal modeof rejected", err)
	}
	defer store.Close()
	observer := topologyTestDB(t, store)
	if _, err = observer.ExecContext(t.Context(), "create table kept(v);insert into kept values('committed')"); err != nil {
		t.Fatal(err)
	}
	controlPath := "control.db"
	control, err := sql.Open("sqlite", dsn(controlPath, reference))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if _, err = control.ExecContext(t.Context(), "create table kept(v);insert into kept values('committed')"); err != nil {
		t.Fatal("modernc normal modeof control failed", err)
	}
	actualInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	controlInfo, err := os.Stat(controlPath)
	if err != nil || actualInfo.Mode().Perm() != controlInfo.Mode().Perm() {
		t.Fatalf("normal modeof permission parity: candidate=%v control=%v err=%v", actualInfo, controlInfo, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{1024, 1025} {
		// Use an absolute reference to isolate its raw byte boundary from
		// the separately admitted relative-to-CWD composition.
		prefix := filepath.VolumeName(dir) + string(os.PathSeparator)
		modeof := prefix + strings.Repeat("x", length-len(prefix))
		candidate, err := openTopologyStore(dsn(path, modeof), time.Hour)
		if candidate != nil || err == nil || errors.Is(err, errTopologyBudget) != (length > 1024) {
			t.Fatalf("modeof length%d: store=%v err=%v", length, candidate, err)
		}
		if topologyReservation.Load() != nil {
			t.Fatal("modeof refusal lost partial-file cleanup")
		}
		var value string
		if err = observer.QueryRowContext(t.Context(), "select v from kept").Scan(&value); err != nil || value != "committed" {
			t.Fatalf("modeof refusal changed data: %q %v", value, err)
		}
		if err = observer.QueryRowContext(t.Context(), "pragma integrity_check").Scan(&value); err != nil || value != "ok" {
			t.Fatalf("modeof refusal changed integrity: %q %v", value, err)
		}
	}
}

func TestV15TopologyStartupResourceRefusalPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-schema.db")
	control, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = control.ExecContext(t.Context(), "create table preserved(v);insert into preserved values('K1KEEP');create table huge(v default('"+strings.Repeat("x", 3<<20)+"'))")
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	if err = control.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := openTopologyStore(path, time.Hour)
	if store != nil {
		store.Close()
	}
	if !errors.Is(err, errTopologyBudget) {
		t.Fatalf("large schema did not reach real engine budget: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("original database file missing", err)
	}
	control, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var value string
	if err = control.QueryRowContext(context.Background(), "select v from preserved").Scan(&value); err != nil || value != "K1KEEP" {
		t.Fatalf("committed data lost: %q %v", value, err)
	}
	if err = control.QueryRowContext(t.Context(), "pragma integrity_check").Scan(&value); err != nil || value != "ok" {
		t.Fatalf("refused database integrity: %q %v", value, err)
	}
	if topologyReservation.Load() != nil {
		t.Fatal("cleanly refused startup retained reservation")
	}
}

func TestV15TopologyTemporaryPathResourceRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporary.db")
	store, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.db.run(t.Context(), func(c *sqlite3.Conn) error {
		return c.Exec("create table kept(v);insert into kept values('preserved')")
	}); err != nil {
		t.Fatal(err)
	}
	spill := func(c *sqlite3.Conn) error {
		return c.Exec("pragma temp_store=FILE;pragma temp.cache_size=2;create temp table scratch(v);with recursive n(x) as(values(1) union all select x+1 from n where x<2000) insert into scratch select zeroblob(1024) from n")
	}
	t.Setenv("SQLITE_TMPDIR", strings.Repeat("x", 2048))
	err = store.db.run(t.Context(), spill)
	if !errors.Is(err, errTopologyBudget) {
		t.Fatalf("oversized temporary path was not an ordinary resource refusal: %v", err)
	}
	t.Setenv("SQLITE_TMPDIR", t.TempDir())
	if err = store.db.run(t.Context(), spill); err != nil {
		t.Fatal("control temporary spill did not succeed", err)
	}
	var value string
	if err = topologyTestDB(t, store).QueryRowContext(t.Context(), "select v from kept").Scan(&value); err != nil || value != "preserved" {
		t.Fatalf("temporary refusal changed committed data: %q %v", value, err)
	}
}
