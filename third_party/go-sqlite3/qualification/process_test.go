//go:build sqlite3_qualification

package v15probe

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
	_ "modernc.org/sqlite"
)

type command struct {
	SQL   string
	Query bool
}
type response struct {
	Rows                  [][]string
	Error                 string
	Retired               bool
	EngineBytes, WALSlots int64
}

func TestV15SQLiteProcessHelper(t *testing.T) {
	role := os.Getenv("V15_SQLITE_ROLE")
	if role == "" {
		return
	}
	path := os.Getenv("V15_SQLITE_PATH")
	var db *sql.DB
	var conn *sqlite3.Conn
	var err error
	if role == "modernc" {
		db, err = sql.Open("sqlite", path)
		if err == nil {
			db.SetMaxOpenConns(1)
			err = db.Ping()
		}
	} else {
		ctx := sqlite3.WithMaxMemory(context.Background(), 8<<20)
		if role == "fallback" {
			ctx = fallbackContext(ctx)
		}
		conn, err = sqlite3.OpenContext(ctx, path)
	}
	w := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(w)
	ready := response{}
	if err != nil {
		ready.Error = err.Error()
	}
	enc.Encode(ready)
	w.Flush()
	if err != nil {
		return
	}
	if db != nil {
		defer db.Close()
	} else {
		defer func() {
			if conn != nil {
				conn.Retire()
			}
		}()
	}
	dec := json.NewDecoder(os.Stdin)
	for {
		var c command
		if err = dec.Decode(&c); err == io.EOF {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		r := response{}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		if db != nil {
			if c.Query {
				var rows *sql.Rows
				rows, err = db.QueryContext(ctx, c.SQL)
				if err == nil {
					cols, _ := rows.Columns()
					for rows.Next() {
						values := make([]any, len(cols))
						pointers := make([]any, len(cols))
						for i := range values {
							pointers[i] = &values[i]
						}
						if err = rows.Scan(pointers...); err != nil {
							break
						}
						out := make([]string, len(cols))
						for i, v := range values {
							out[i] = fmt.Sprint(v)
						}
						r.Rows = append(r.Rows, out)
					}
					if err == nil {
						err = rows.Err()
					}
					rows.Close()
				}
			} else {
				_, err = db.ExecContext(ctx, c.SQL)
			}
		} else {
			err = executeCandidate(ctx, &conn, c, &r)
		}
		cancel()
		if err != nil {
			r.Error = err.Error()
		}
		if err = enc.Encode(r); err != nil {
			t.Fatal(err)
		}
		if err = w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

// This fixture boundary exposes the fork's exact OOM sentinel as a response;
// production boundary behavior is checked separately in peer adapter tests.
func executeCandidate(ctx context.Context, conn **sqlite3.Conn, c command, r *response) (err error) {
	if *conn == nil {
		return errors.New("fixture connection retired")
	}
	owner := *conn
	usage := owner.Ownership()
	defer func() {
		if failure := recover(); failure != nil {
			if !sqlite3.IsOutOfMemoryPanic(failure) {
				panic(failure)
			}
			err = sqlite3.NOMEM
			if cleanupErr := owner.Retire(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			} else {
				*conn = nil
				r.Retired = true
			}
		}
		if *conn != nil {
			owner.SetInterrupt(context.Background())
		}
		if usage != nil {
			r.EngineBytes = usage.EngineBacking.Load()
			r.WALSlots = usage.WALSlots.Load()
		}
	}()
	owner.SetInterrupt(ctx)
	if !c.Query {
		return owner.Exec(c.SQL)
	}
	stmt, _, err := owner.Prepare(c.SQL)
	if err != nil {
		return err
	}
	for stmt.Step() {
		out := make([]string, stmt.ColumnCount())
		for i := range out {
			out[i] = stmt.ColumnText(i)
		}
		r.Rows = append(r.Rows, out)
	}
	err = stmt.Err()
	return errors.Join(err, stmt.Close())
}

type process struct {
	cmd     *exec.Cmd
	in      io.WriteCloser
	enc     *json.Encoder
	dec     *json.Decoder
	stderr  strings.Builder
	killed  bool
	waited  bool
	waitErr error
}

func startProcess(t *testing.T, role, path string) *process {
	t.Helper()
	p := &process{cmd: exec.Command(os.Args[0], "-test.run=^TestV15SQLiteProcessHelper$", "-test.timeout=180s")}
	p.cmd.Env = append(os.Environ(), "V15_SQLITE_ROLE="+role, "V15_SQLITE_PATH="+path)
	p.cmd.Stderr = &p.stderr
	var err error
	p.in, err = p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.enc = json.NewEncoder(p.in)
	p.dec = json.NewDecoder(out)
	if err = p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.in.Close()
		if err := p.wait(); err != nil && !p.killed {
			t.Errorf("%s helper: %v %s", role, err, p.stderr.String())
		}
	})
	var r response
	if err = p.dec.Decode(&r); err != nil || r.Error != "" {
		t.Fatalf("%s startup: %v %s %s", role, err, r.Error, p.stderr.String())
	}
	return p
}

// wait is owned by the test goroutine and its cleanup. Crash recovery must
// join the exact writer before probing its files; successful WAL reads do not
// establish termination. Cleanup reuses the result instead of waiting twice.
func (p *process) wait() error {
	if !p.waited {
		p.waitErr = p.cmd.Wait()
		p.waited = true
	}
	return p.waitErr
}

func (p *process) crash(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	p.killed = true
	err := p.wait()
	state := p.cmd.ProcessState
	t.Logf("writer pid=%d kill=success wait=%v state=%v joined_at=%s", p.cmd.Process.Pid, err, state, time.Now().UTC().Format(time.RFC3339Nano))
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || state == nil {
		t.Fatalf("writer termination was not witnessed: wait=%v state=%v stderr=%s", err, state, p.stderr.String())
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || (runtime.GOOS == "windows" && state.ExitCode() != 1) || (runtime.GOOS != "windows" && (!status.Signaled() || status.Signal() != syscall.SIGKILL)) {
		t.Fatalf("writer did not exit through forced termination: wait=%v state=%v stderr=%s", err, state, p.stderr.String())
	}
}
func (p *process) do(t *testing.T, q string, query bool) response {
	t.Helper()
	if err := p.enc.Encode(command{q, query}); err != nil {
		t.Fatal(err)
	}
	var r response
	if err := p.dec.Decode(&r); err != nil {
		t.Fatalf("helper response: %v %s", err, p.stderr.String())
	}
	return r
}
func (p *process) exec(t *testing.T, q string) {
	t.Helper()
	if r := p.do(t, q, false); r.Error != "" {
		t.Fatalf("%s: %s", q, r.Error)
	}
}
func (p *process) value(t *testing.T, q, want string) {
	t.Helper()
	r := p.do(t, q, true)
	if r.Error != "" || len(r.Rows) != 1 || len(r.Rows[0]) != 1 || r.Rows[0][0] != want {
		t.Fatalf("%s got%+v want%s", q, r, want)
	}
}

func TestV15SQLiteWALProcessLocksAndCoherence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.db")
	testWALProcessLocksAndCoherence(t, path, path)
}

func testWALProcessLocksAndCoherence(t *testing.T, direct, alias string) {
	t.Helper()
	m := startProcess(t, "modernc", direct)
	m.exec(t, "pragma page_size=512; pragma journal_mode=WAL; pragma wal_autocheckpoint=0; create table x(id integer primary key,v text); insert into x values(1,'old')")
	n := startProcess(t, "native", alias)
	t.Logf("secondary candidate mode=%s", secondaryMode())
	f := startProcess(t, secondaryMode(), alias)
	for _, p := range []*process{m, n, f} {
		p.value(t, "pragma journal_mode", "wal")
	}
	for i, pair := range [][2]*process{{m, f}, {f, n}, {n, m}, {f, m}, {n, f}} {
		writer, reader := pair[0], pair[1]
		old := fmt.Sprint(i)
		next := fmt.Sprint(i + 1)
		writer.exec(t, "update x set v='"+old+"'")
		reader.exec(t, "begin")
		reader.value(t, "select v from x", old)
		writer.exec(t, "begin immediate; update x set v='"+next+"'; commit")
		reader.value(t, "select v from x", old)
		reader.exec(t, "rollback")
		reader.value(t, "select v from x", next)
		writer.exec(t, "begin immediate")
		if r := reader.do(t, "begin immediate", false); r.Error == "" {
			t.Fatal("conflicting cross-process writer admitted")
		}
		writer.exec(t, "rollback")
		reader.exec(t, "begin immediate; rollback")
	}
	f.exec(t, "begin")
	f.value(t, "select count(*) from x", "1")
	m.exec(t, "insert into x values(2,'new')")
	r := n.do(t, "pragma wal_checkpoint(TRUNCATE)", true)
	if r.Error != "" || len(r.Rows) != 1 || r.Rows[0][0] != "1" {
		t.Fatalf("pinned reader did not hold checkpoint: %+v", r)
	}
	f.exec(t, "rollback")
	r = n.do(t, "pragma wal_checkpoint(TRUNCATE)", true)
	if r.Error != "" || len(r.Rows) != 1 || r.Rows[0][0] != "0" {
		t.Fatalf("checkpoint did not recover: %+v", r)
	}
	for _, p := range []*process{m, n, f} {
		p.value(t, "pragma integrity_check", "ok")
		p.value(t, "select count(*) from x", "2")
	}
}

func TestV15SQLiteWALMultipleIndexPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.db")
	m := startProcess(t, "modernc", path)
	m.exec(t, "pragma page_size=512; pragma journal_mode=WAL; pragma wal_autocheckpoint=0; create table x(id integer primary key,v); insert into x values(0,0)")
	t.Logf("secondary candidate mode=%s", secondaryMode())
	f := startProcess(t, secondaryMode(), path)
	f.exec(t, "begin")
	f.value(t, "select count(*) from x", "1")
	m.exec(t, "with recursive n(x) as(values(1) union all select x+1 from n where x<16000) insert into x select x,zeroblob(512) from n")
	info, err := os.Stat(path + "-shm")
	if err != nil || info.Size() <= 65536 {
		t.Fatalf("fixture did not cross WAL-index 64 KiB boundary: %v %v", info, err)
	}
	f.value(t, "select count(*) from x", "1")
	f.exec(t, "rollback")
	f.value(t, "select count(*) from x", "16001")
	f.exec(t, "update x set v=zeroblob(700) where id between 1 and 6000")
	m.value(t, "select count(*) from x where length(v)=700", "6000")
	n := startProcess(t, "native", path)
	n.value(t, "select count(*) from x", "16001")
	for _, p := range []*process{m, n, f} {
		p.value(t, "pragma integrity_check", "ok")
	}
}

// These are the same read probes used by crash recovery. They must succeed
// while a live writer still owns the WAL write lock, proving that reads alone
// cannot replace the process-exit barrier before a recovery write.
func TestV15SQLiteWALLiveWriterReadProbes(t *testing.T) {
	roles := []string{"native"}
	if secondaryMode() == "fallback" {
		roles = append(roles, "fallback")
	}
	for _, role := range roles {
		t.Run(role, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "live.db")
			m := startProcess(t, "modernc", path)
			m.exec(t, "pragma journal_mode=WAL;create table x(id integer primary key,v);insert into x values(1,'old'),(2,'old')")
			writer := startProcess(t, role, path)
			writer.exec(t, "pragma synchronous=FULL;begin immediate;update x set v='new'")
			m.exec(t, "pragma busy_timeout=5000")
			m.value(t, "select min(v)||':'||max(v)||':'||count(*) from x", "old:old:2")
			m.value(t, "pragma integrity_check", "ok")
			reopened := startProcess(t, role, path)
			reopened.value(t, "select min(v)||':'||max(v)||':'||count(*) from x", "old:old:2")
			r := reopened.do(t, "update x set v='recovered'", false)
			if r.Error != "sqlite3: database is locked" {
				t.Fatalf("live writer must retain the WAL write lock after successful read probes: %+v", r)
			}
			writer.value(t, "select min(v)||':'||max(v)||':'||count(*) from x", "new:new:2")
			t.Logf("live writer pid=%d; read probes passed; conflicting update=%s", writer.cmd.Process.Pid, r.Error)
			writer.crash(t)
			// No sleep, retry, or changed timeout: the same observer must write
			// immediately after the owner's verified exit releases its locks.
			reopened.exec(t, "update x set v='recovered'")
			m.value(t, "select count(*) from x where v='recovered'", "2")
			m.value(t, "pragma integrity_check", "ok")
		})
	}
}

func TestV15SQLiteWALCrashAndReopen(t *testing.T) {
	roles := []string{"native"}
	if secondaryMode() == "fallback" {
		roles = append(roles, "fallback")
	}
	for _, role := range roles {
		for _, phase := range []string{"uncommitted", "unacknowledged", "acknowledged"} {
			t.Run(role+"/"+phase, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "crash.db")
				m := startProcess(t, "modernc", path)
				m.exec(t, "pragma journal_mode=WAL;create table x(id integer primary key,v);insert into x values(1,'old'),(2,'old')")
				writer := startProcess(t, role, path)
				writer.exec(t, "pragma synchronous=FULL;begin immediate;update x set v='new'")
				switch phase {
				case "unacknowledged":
					if err := writer.enc.Encode(command{"commit", false}); err != nil {
						t.Fatal(err)
					}
				case "acknowledged":
					writer.exec(t, "commit")
				}
				writer.crash(t)
				// crash joins kernel termination without collecting the COMMIT
				// response; the unacknowledged outcome remains old or new.
				m.exec(t, "pragma busy_timeout=5000")
				r := m.do(t, "select min(v),max(v),count(*) from x", true)
				if r.Error != "" || len(r.Rows) != 1 || len(r.Rows[0]) != 3 || r.Rows[0][2] != "2" || r.Rows[0][0] != r.Rows[0][1] {
					t.Fatalf("torn commit: %+v", r)
				}
				value := r.Rows[0][0]
				if (phase == "uncommitted" && value != "old") || (phase == "acknowledged" && value != "new") || (value != "old" && value != "new") {
					t.Fatalf("phase=%s value=%s", phase, value)
				}
				t.Logf("phase=%s committed_value=%s rows=2", phase, value)
				m.value(t, "pragma integrity_check", "ok")
				reopened := startProcess(t, role, path)
				reopened.value(t, "select min(v)||':'||max(v)||':'||count(*) from x", value+":"+value+":2")
				reopened.exec(t, "update x set v='recovered'")
				m.value(t, "select count(*) from x where v='recovered'", "2")
			})
		}
	}
}

func TestV15SQLiteWALExternalGrowthBudgetAndRecovery(t *testing.T) {
	if secondaryMode() != "fallback" {
		t.Skip("Windows fallback mapping-cap gate; native backing gate runs separately")
	}
	path := filepath.Join(t.TempDir(), "growth.db")
	m := startProcess(t, "modernc", path)
	m.exec(t, "pragma page_size=512;pragma journal_mode=WAL;pragma wal_autocheckpoint=0;create table x(id integer primary key,v);insert into x values(0,'kept')")
	f := startProcess(t, "fallback", path)
	f.exec(t, "begin")
	f.value(t, "select count(*) from x", "1")
	m.exec(t, "with recursive n(x) as(values(1) union all select x+1 from n where x<40000) insert into x select x,zeroblob(4096) from n")
	info, err := os.Stat(path + "-shm")
	if err != nil || info.Size() <= 64*32768 {
		t.Fatalf("fixture did not exceed mapping capacity: %v %v", info, err)
	}
	f.value(t, "select count(*) from x", "1")
	f.exec(t, "rollback")
	r := f.do(t, "select count(*) from x", true)
	if r.Error == "" {
		t.Fatalf("over-cap external WAL admitted: %+v", r)
	}
	if !r.Retired || r.EngineBytes != 0 || r.WALSlots != 0 {
		t.Fatalf("budget refusal retained unreported owner: %+v", r)
	}
	t.Logf("external WAL-index bytes=%d; candidate refusal=%s", info.Size(), r.Error)
	m.value(t, "select count(*) from x", "40001")
	m.value(t, "select v from x where id=0", "kept")
	m.exec(t, "pragma busy_timeout=5000")
	r = m.do(t, "pragma wal_checkpoint(TRUNCATE)", true)
	if r.Error != "" || len(r.Rows) != 1 || r.Rows[0][0] != "0" {
		t.Fatalf("recovery checkpoint: %+v", r)
	}
	reopened := startProcess(t, "fallback", path)
	reopened.value(t, "select count(*) from x", "40001")
	reopened.exec(t, "update x set v='recovered' where id=0")
	m.value(t, "select v from x where id=0", "recovered")
	m.value(t, "pragma integrity_check", "ok")
}

func TestV15SQLiteDefaultInventory(t *testing.T) {
	m := startProcess(t, "modernc", filepath.Join(t.TempDir(), "modernc.db"))
	n := startProcess(t, "native", filepath.Join(t.TempDir(), "native.db"))
	for _, pragma := range []string{"foreign_keys", "recursive_triggers", "secure_delete", "synchronous", "temp_store", "auto_vacuum", "cache_size", "mmap_size", "page_size", "locking_mode", "journal_mode", "busy_timeout", "trusted_schema", "defer_foreign_keys", "ignore_check_constraints", "automatic_index", "read_uncommitted", "reverse_unordered_selects", "legacy_alter_table", "query_only", "fullfsync", "checkpoint_fullfsync", "wal_autocheckpoint", "journal_size_limit"} {
		a, b := m.do(t, "pragma "+pragma, true), n.do(t, "pragma "+pragma, true)
		t.Logf("%s modernc=%+v candidate=%+v", pragma, a, b)
	}
}
