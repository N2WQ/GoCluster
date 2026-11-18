package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func diagnosticArtifactOwner(t *testing.T, limit, reserve int64, ops studyDiagnosticArtifactOps) *studyDiagnosticArtifacts {
	t.Helper()
	owner, err := newStudyDiagnosticArtifactsWithOps(t.TempDir(), limit, reserve, ops)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func diagnosticArtifactOpen(t *testing.T, a *studyDiagnosticArtifacts, path string) *studyDiagnosticArtifactFile {
	t.Helper()
	file, err := a.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600, false)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestDiagnosticArtifactProductionConstructor(t *testing.T) {
	root := t.TempDir()
	a, err := newStudyDiagnosticArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	accounting, err := a.accounting("constructed_before_writes")
	if err != nil || a.root != filepath.Clean(resolved) || a.Err() != nil || accounting.Stage != "constructed_before_writes" || accounting.ChargedBytes != 0 || accounting.LimitBytes != int64(4*1024*1024*1024) || accounting.ReservedCapacityBytes != 16*1024*1024 {
		t.Fatalf("production constructor policy/root state changed: %+v err=%v", accounting, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("construction created output evidence:", err)
	}
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(root, "missing")
	for _, input := range []string{"", " \t\r\n", "https://example.invalid/output", missing, file} {
		owner, err := newStudyDiagnosticArtifacts(input)
		if err == nil || owner != nil {
			t.Fatalf("invalid production root created an owner: %q %v", input, err)
		}
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("constructor created a missing caller-owned directory")
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "old" {
		t.Fatal("constructor changed a rejected existing file:", err)
	}
}

type diagnosticArtifactFaultSink struct {
	studyAuditSink
	writeErr, syncErr, closeErr error
	short                       bool
	writes, syncs, closes       int
}

func (s *diagnosticArtifactFaultSink) Write(data []byte) (int, error) {
	s.writes++
	if s.short {
		count := len(data) / 2
		n, err := s.studyAuditSink.Write(data[:count])
		return n, errors.Join(err, s.writeErr)
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.studyAuditSink.Write(data)
}
func (s *diagnosticArtifactFaultSink) Sync() error {
	s.syncs++
	return errors.Join(s.studyAuditSink.Sync(), s.syncErr)
}
func (s *diagnosticArtifactFaultSink) Close() error {
	s.closes++
	return errors.Join(s.studyAuditSink.Close(), s.closeErr)
}

func TestDiagnosticArtifactSharedQuota(t *testing.T) {
	a := diagnosticArtifactOwner(t, 10, 0, studyDiagnosticArtifactFileOps())
	first, second := diagnosticArtifactOpen(t, a, "one"), diagnosticArtifactOpen(t, a, "two")
	if _, err := first.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("123456")); err != nil {
		t.Fatal(err)
	}
	if n, err := second.Write([]byte("x")); n != 0 || err == nil || a.quota.used != 10 {
		t.Fatal("one-byte excess was written/refunded")
	}
	if err := errors.Join(first.Sync(), first.Close(), second.Sync(), second.Close()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(a.root, "two"))
	if err != nil || string(data) != "123456" {
		t.Fatal("quota rejection wrote bytes")
	}
	a = diagnosticArtifactOwner(t, 256, 128, studyDiagnosticArtifactFileOps())
	file := diagnosticArtifactOpen(t, a, "data")
	if _, err := file.Write(make([]byte, 128)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("x")); err == nil {
		t.Fatal("data consumed reserved metadata bytes")
	}
	original := a.Err()
	if original == nil {
		t.Fatal("quota refusal did not latch its cause")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.atomicJSON("COMPLETE.json", true); !errors.Is(err, original) {
		t.Fatal("normal metadata continued after latch")
	}
	if err := a.writeFailed(struct {
		Failed bool `json:"failed"`
	}{true}); err != nil {
		t.Fatal("reserved explicit failure output unavailable:", err)
	}
	if !errors.Is(a.Err(), original) {
		t.Fatal("FAILED cleared/replaced first error")
	}
	if _, err := os.Stat(filepath.Join(a.root, "COMPLETE.json")); !os.IsNotExist(err) {
		t.Fatal("latched owner published COMPLETE")
	}
}

func TestDiagnosticArtifactShortWrites(t *testing.T) {
	for _, withError := range []bool{false, true} {
		t.Run(map[bool]string{false: "short_nil", true: "short_error"}[withError], func(t *testing.T) {
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			var sink *diagnosticArtifactFaultSink
			sentinel := errors.New("underlying write")
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				sink = &diagnosticArtifactFaultSink{studyAuditSink: file, short: true}
				if withError {
					sink.writeErr = sentinel
				}
				return sink, err
			}
			a := diagnosticArtifactOwner(t, 100, 0, ops)
			file := diagnosticArtifactOpen(t, a, "short")
			data := []byte("abcde")
			before := bytes.Clone(data)
			n, err := file.Write(data)
			if n != 2 || !errors.Is(err, io.ErrShortWrite) || withError && !errors.Is(err, sentinel) || a.quota.used != 5 || !bytes.Equal(data, before) {
				t.Fatalf("short write contract: n=%d err=%v charged=%d", n, err, a.quota.used)
			}
			if n, err := file.Write([]byte("later")); n != 0 || err == nil || sink.writes != 1 {
				t.Fatal("normal writer continued after latch")
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil || sink.closes != 1 {
				t.Fatal("Close was repeated/reported twice")
			}
		})
	}
}

type diagnosticArtifactSource struct {
	file             *os.File
	statErr, readErr error
	maxRead          int
}

func (s *diagnosticArtifactSource) Stat() (os.FileInfo, error) {
	if s.statErr != nil {
		return nil, s.statErr
	}
	return s.file.Stat()
}
func (s *diagnosticArtifactSource) Read(data []byte) (int, error) {
	s.maxRead = max(s.maxRead, len(data))
	if s.readErr != nil {
		return 0, s.readErr
	}
	return s.file.Read(data)
}
func (s *diagnosticArtifactSource) WriteTo(io.Writer) (int64, error) {
	panic("copy bypassed guarded writer using WriteTo")
}

func TestDiagnosticArtifactFailureLatch(t *testing.T) {
	readErr, syncErr, closeErr := errors.New("source read"), errors.New("output sync"), errors.New("output close")
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
	ops := studyDiagnosticArtifactFileOps()
	open := ops.open
	var output *diagnosticArtifactFaultSink
	ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
		file, err := open(path, flags, mode)
		output = &diagnosticArtifactFaultSink{studyAuditSink: file, syncErr: syncErr, closeErr: closeErr}
		return output, err
	}
	a := diagnosticArtifactOwner(t, 1024, 128, ops)
	_, err = a.copyFile("copy", &diagnosticArtifactSource{file: source, readErr: readErr})
	if !errors.Is(err, readErr) || !errors.Is(err, syncErr) || !errors.Is(err, closeErr) || !errors.Is(a.Err(), readErr) || errors.Is(a.Err(), syncErr) || errors.Is(a.Err(), closeErr) || output.syncs != 1 || output.closes != 1 {
		t.Fatalf("first failure/finalization lost: %v", err)
	}
	if _, err := source.Stat(); err != nil {
		t.Fatal("copy closed caller-owned source:", err)
	}
	if _, err := a.openFile("later", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600, true); !errors.Is(err, readErr) {
		t.Fatal("metadata open ignored first latch")
	}
	for _, kind := range []string{"open", "stat", "json_preflight", "json_marshal"} {
		t.Run(kind, func(t *testing.T) {
			ops := studyDiagnosticArtifactFileOps()
			sentinel := errors.New(kind)
			if kind == "open" {
				ops.open = func(string, int, os.FileMode) (studyAuditSink, error) { return nil, sentinel }
			}
			a := diagnosticArtifactOwner(t, 1024, 128, ops)
			var err error
			switch kind {
			case "open":
				_, err = a.openFile("file", os.O_WRONLY|os.O_CREATE, 0o600, false)
			case "stat":
				_, err = a.copyFile("file", &diagnosticArtifactSource{file: source, statErr: sentinel})
			case "json_preflight":
				err = a.atomicJSON("report", strings.Repeat("x", 1025))
			case "json_marshal":
				err = a.atomicJSON("report", math.NaN())
			}
			if err == nil || a.Err() == nil {
				t.Fatal("primitive failure was returned without latching")
			}
			if err := a.atomicJSON("COMPLETE.json", true); err == nil {
				t.Fatal("latched primitive permitted completion")
			}
		})
	}
}

func TestDiagnosticArtifactOwnedPaths(t *testing.T) {
	opsLink := studyDiagnosticArtifactFileOps()
	lstat := opsLink.lstat
	opsLink.lstat = func(path string) (os.FileInfo, error) {
		if filepath.Base(path) == "link" {
			return diagnosticScratchInfo{mode: os.ModeSymlink}, nil
		}
		return lstat(path)
	}
	linked := diagnosticArtifactOwner(t, 1024, 0, opsLink)
	if _, err := linked.openFile("link", os.O_WRONLY|os.O_CREATE, 0o600, false); err == nil || linked.Err() == nil {
		t.Fatal("link destination was followed")
	}
	for _, kind := range []string{"parent_escape", "absolute_escape", "resolved_parent_escape"} {
		t.Run(kind, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			ops := studyDiagnosticArtifactFileOps()
			resolve := ops.resolve
			if kind == "resolved_parent_escape" {
				if err := os.Mkdir(filepath.Join(root, "alias"), 0o700); err != nil {
					t.Fatal(err)
				}
				ops.resolve = func(path string) (string, error) {
					if filepath.Base(path) == "alias" {
						return outside, nil
					}
					return resolve(path)
				}
			}
			a, err := newStudyDiagnosticArtifactsWithOps(root, 1024, 0, ops)
			if err != nil {
				t.Fatal(err)
			}
			name := "../escape"
			if kind == "absolute_escape" {
				name = filepath.Join(outside, "escape")
			}
			if kind == "resolved_parent_escape" {
				name = filepath.Join("alias", "escape")
			}
			if _, err := a.openFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600, false); err == nil || a.Err() == nil {
				t.Fatal("destination containment failure was ignored")
			}
			if _, err := os.Stat(filepath.Join(outside, "escape")); !os.IsNotExist(err) {
				t.Fatal("escape wrote outside owned root")
			}
		})
	}
	ops := studyDiagnosticArtifactFileOps()
	open := ops.open
	var flags []int
	var modes []os.FileMode
	ops.open = func(path string, flag int, mode os.FileMode) (studyAuditSink, error) {
		flags = append(flags, flag)
		modes = append(modes, mode)
		return open(path, flag, mode)
	}
	a := diagnosticArtifactOwner(t, 1024, 0, ops)
	for _, fixture := range []struct {
		flags int
		mode  os.FileMode
		text  string
	}{{os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0o644, "abc"}, {os.O_WRONLY | os.O_APPEND, 0o644, "de"}, {os.O_WRONLY | os.O_TRUNC, 0o600, "f"}} {
		file, err := a.openFile("owned", fixture.flags, fixture.mode, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(fixture.text)); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(flags, []int{os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.O_WRONLY | os.O_APPEND, os.O_WRONLY | os.O_TRUNC}) || !reflect.DeepEqual(modes, []os.FileMode{0o644, 0o644, 0o600}) || a.quota.used != 6 {
		t.Fatal("open flags, permissions or cumulative overwrite accounting changed")
	}
}

type diagnosticArtifactCustomJSON struct{ calls *int }

func (v diagnosticArtifactCustomJSON) MarshalJSON() ([]byte, error) {
	(*v.calls)++
	panic("unbounded diagnostic custom marshaler must never run")
}

func TestDiagnosticArtifactJSONBounds(t *testing.T) {
	for _, over := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact_newline_limit", true: "one_byte_over"}[over], func(t *testing.T) {
			a := diagnosticArtifactOwner(t, 1<<20, 0, studyDiagnosticArtifactFileOps())
			file := diagnosticArtifactOpen(t, a, "rows")
			values := make([]int, 32767)
			if over {
				values[len(values)-1] = 10
			}
			err := a.writeRow(file, values)
			if (err != nil) != over {
				t.Fatalf("row/newline byte bound failed: err=%v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(a.root, "rows"))
			if err != nil {
				t.Fatal(err)
			}
			want := int64(studyAuditRowBytes)
			if over {
				want = 0
			}
			if info.Size() != want {
				t.Fatalf("row wrote %d bytes want %d", info.Size(), want)
			}
		})
	}
	for _, value := range []string{strings.Repeat("é", 512), "<&>\u2028\n\""} {
		a := diagnosticArtifactOwner(t, 10000, 0, studyDiagnosticArtifactFileOps())
		file := diagnosticArtifactOpen(t, a, "row")
		if err := a.writeRow(file, value); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(a.root, "row"))
		if err != nil || string(data) != string(encoded)+"\n" || a.quota.used != int64(len(data)) {
			t.Fatal("UTF-8/escaping accounting did not count actual bytes")
		}
	}
	for _, value := range []any{strings.Repeat("é", 513), string([]byte{0xff}), json.RawMessage(`{"x":"raw JSON is unsupported"}`)} {
		a := diagnosticArtifactOwner(t, 10000, 0, studyDiagnosticArtifactFileOps())
		file := diagnosticArtifactOpen(t, a, "row")
		if err := a.writeRow(file, value); err == nil || a.quota.used != 0 {
			t.Fatal("invalid label encoded/written before refusal")
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	a := diagnosticArtifactOwner(t, 10000, 0, studyDiagnosticArtifactFileOps())
	calls := 0
	if err := a.atomicJSON("custom", diagnosticArtifactCustomJSON{calls: &calls}); err == nil || calls != 0 {
		t.Fatal("unbounded custom marshaler executed before preflight")
	}
}

func TestDiagnosticArtifactAtomicFailures(t *testing.T) {
	for _, fault := range []string{"short", "sync", "close", "rename", "existing_final", "existing_tmp"} {
		t.Run(fault, func(t *testing.T) {
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			sentinel := errors.New(fault)
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				sink := &diagnosticArtifactFaultSink{studyAuditSink: file}
				switch fault {
				case "short":
					sink.short = true
				case "sync":
					sink.syncErr = sentinel
				case "close":
					sink.closeErr = sentinel
				}
				return sink, err
			}
			if fault == "rename" {
				ops.rename = func(string, string) error { return sentinel }
			}
			a := diagnosticArtifactOwner(t, 10000, 128, ops)
			if strings.HasPrefix(fault, "existing_") {
				name := "COMPLETE.json"
				if fault == "existing_tmp" {
					name += ".tmp"
				}
				if err := os.WriteFile(filepath.Join(a.root, name), []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.atomicJSON("COMPLETE.json", struct {
				Complete bool `json:"complete"`
			}{true}); err == nil || a.Err() == nil {
				t.Fatal("atomic failure did not latch")
			}
			if fault == "existing_final" {
				data, err := os.ReadFile(filepath.Join(a.root, "COMPLETE.json"))
				if err != nil || string(data) != "old" {
					t.Fatal("existing evidence overwritten")
				}
			} else if _, err := os.Stat(filepath.Join(a.root, "COMPLETE.json")); !os.IsNotExist(err) {
				t.Fatal("failed atomic operation published final")
			}
		})
	}
}

func TestDiagnosticArtifactBoundedCopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	data := bytes.Repeat([]byte("copy-data"), 131072)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	a := diagnosticArtifactOwner(t, 2<<20, 128, studyDiagnosticArtifactFileOps())
	source := &diagnosticArtifactSource{file: file}
	receipt, err := a.copyFile("copied", source)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if receipt.BytesCopied != int64(len(data)) || receipt.SHA256 != hex.EncodeToString(sum[:]) || source.maxRead > 32<<10 || a.quota.used != int64(len(data)) {
		t.Fatal("streaming byte/hash/buffer contract changed")
	}
	destination := filepath.Join(a.root, "copied")
	copied, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(copied, data) || receipt.Path != destination {
		t.Fatal("copied artifact content or owned receipt path changed:", err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatal("source ownership was stolen")
	}
	for _, grow := range []int{5, 6} {
		t.Run(map[int]string{5: "grow_to_exact", 6: "grow_one_over"}[grow], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "growing")
			if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			source, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := source.Close(); err != nil {
					t.Error(err)
				}
			}()
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			ops.open = func(destination string, flags int, mode os.FileMode) (studyAuditSink, error) {
				appendFile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
				if err != nil {
					return nil, err
				}
				_, writeErr := appendFile.Write(bytes.Repeat([]byte("x"), grow))
				if err := errors.Join(writeErr, appendFile.Close()); err != nil {
					return nil, err
				}
				return open(destination, flags, mode)
			}
			a := diagnosticArtifactOwner(t, 8, 0, ops)
			receipt, err := a.copyFile("copy", source)
			if grow == 5 {
				if err != nil || receipt.SourceSizeAtStat != 3 || receipt.BytesCopied != 8 {
					t.Fatal("copy trusted stale Stat size")
				}
			} else if err == nil || a.Err() == nil || a.quota.used > 8 {
				t.Fatal("growing source bypassed cumulative quota")
			}
		})
	}
}

func TestDiagnosticArtifactAccountingStages(t *testing.T) {
	a := diagnosticArtifactOwner(t, 10000, 128, studyDiagnosticArtifactFileOps())
	file := diagnosticArtifactOpen(t, a, "deleted")
	if _, err := file.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(a.root, "deleted")); err != nil {
		t.Fatal(err)
	}
	before, err := a.accounting("before_report_write")
	if err != nil || before.ChargedBytes != 8 || before.ReservedCapacityBytes != 128 {
		t.Fatal("cleanup refunded charged bytes")
	}
	if err := a.atomicJSON("report.json", before); err != nil {
		t.Fatal(err)
	}
	after, err := a.accounting("after_report_write")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(a.root, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if after.ChargedBytes != 8+info.Size() || after.Stage == before.Stage {
		t.Fatal("rename charged twice or accounting stage lost")
	}
}
