// File role: Own cumulative diagnostic artifact bytes and first-failure state.
// One synchronous run exclusively owns this object and its directory. It has no
// concurrent writers, background work, refund, or implicit success publication.
// Original sources and the separately measured scratch database are not opened
// through this owner. Ordinary replay and the E0 owner remain unchanged.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"
)

type studyDiagnosticArtifactOps struct {
	open    func(string, int, os.FileMode) (studyAuditSink, error)
	rename  func(string, string) error
	lstat   func(string) (os.FileInfo, error)
	resolve func(string) (string, error)
}

func studyDiagnosticArtifactFileOps() studyDiagnosticArtifactOps {
	return studyDiagnosticArtifactOps{open: func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
		return os.OpenFile(path, flags, mode)
	}, rename: os.Rename, lstat: os.Lstat, resolve: filepath.EvalSymlinks}
}

type studyDiagnosticArtifacts struct {
	root  string
	quota studyAuditQuota
	first error
	ops   studyDiagnosticArtifactOps
}

func newStudyDiagnosticArtifacts(root string) (*studyDiagnosticArtifacts, error) {
	return newStudyDiagnosticArtifactsWithOps(root, studyAuditOutputBytes, studyAuditReserveBytes, studyDiagnosticArtifactFileOps())
}

func newStudyDiagnosticArtifactsWithOps(root string, limit, reserve int64, ops studyDiagnosticArtifactOps) (*studyDiagnosticArtifacts, error) {
	if limit <= 0 || limit > studyAuditOutputBytes || reserve < 0 || reserve > limit || ops.open == nil || ops.rename == nil || ops.lstat == nil || ops.resolve == nil {
		return nil, errors.New("invalid diagnostic artifact owner construction")
	}
	if err := studyAuditLocalPath(root); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := ops.lstat(abs)
	if err != nil {
		return nil, err
	}
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("diagnostic artifact root must be an exclusively owned directory")
	}
	resolved, err := ops.resolve(abs)
	if err != nil {
		return nil, err
	}
	if len(resolved) > studyAuditLabelBytes || !utf8.ValidString(resolved) {
		return nil, errors.New("diagnostic artifact root exceeds UTF-8 path bound")
	}
	return &studyDiagnosticArtifacts{root: strings.Clone(filepath.Clean(resolved)), quota: studyAuditQuota{limit: limit, reserve: reserve}, ops: ops}, nil
}

func (a *studyDiagnosticArtifacts) latch(err error) error {
	if err == nil {
		return nil
	}
	if a.first == nil {
		a.first = err
		return err
	}
	return errors.Join(a.first, err)
}

func (a *studyDiagnosticArtifacts) Err() error { return a.first }

type studyDiagnosticArtifactAccounting struct {
	Stage                 string `json:"stage"`
	ChargedBytes          int64  `json:"charged_bytes"`
	LimitBytes            int64  `json:"limit_bytes"`
	ReservedCapacityBytes int64  `json:"reserved_capacity_bytes"`
	Policy                string `json:"policy"`
}

func (a *studyDiagnosticArtifacts) accounting(stage string) (studyDiagnosticArtifactAccounting, error) {
	if stage == "" || len(stage) > studyAuditLabelBytes || !utf8.ValidString(stage) {
		return studyDiagnosticArtifactAccounting{}, a.latch(errors.New("diagnostic accounting requires a bounded UTF-8 stage"))
	}
	return studyDiagnosticArtifactAccounting{Stage: strings.Clone(stage), ChargedBytes: a.quota.used, LimitBytes: a.quota.limit, ReservedCapacityBytes: a.quota.reserve,
		Policy: "cumulative requested writes; no cleanup refunds; rename charged once"}, nil
}

// Existing regular files are permitted only through explicit caller-owned
// flags (e.g. history APPEND). Atomic/new helpers additionally require absence.
// Parent resolution rejects links before any open/rename; the run's exclusive
// filesystem ownership excludes concurrent replacement of checked components.
func (a *studyDiagnosticArtifacts) destination(path string) (string, error) {
	if err := studyAuditLocalPath(path); err != nil {
		return "", a.latch(err)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(a.root, path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", a.latch(err)
	}
	rel, err := filepath.Rel(a.root, abs)
	if err != nil || rel == "." || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", a.latch(errors.Join(err, errors.New("diagnostic artifact escapes its owned root")))
	}
	if studyDiagnosticReservedScratchChild(rel) {
		return "", a.latch(errors.New("first-party artifacts cannot use the reserved scratch root child"))
	}
	parent, err := a.ops.resolve(filepath.Dir(abs))
	if err != nil {
		return "", a.latch(err)
	}
	resolved := filepath.Join(parent, filepath.Base(abs))
	resolvedRel, err := filepath.Rel(a.root, resolved)
	if err != nil || filepath.IsAbs(resolvedRel) || resolvedRel == ".." || strings.HasPrefix(resolvedRel, ".."+string(filepath.Separator)) {
		return "", a.latch(errors.Join(err, errors.New("diagnostic artifact parent escapes its owned root")))
	}
	if studyDiagnosticReservedScratchChild(resolvedRel) {
		return "", a.latch(errors.New("resolved first-party artifact cannot use the reserved scratch root child"))
	}
	if len(resolved) > studyAuditLabelBytes {
		return "", a.latch(errors.New("diagnostic artifact path exceeds 1024 bytes"))
	}
	if info, statErr := a.ops.lstat(resolved); statErr == nil {
		if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return "", a.latch(errors.New("diagnostic destination is not a regular owned file"))
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", a.latch(statErr)
	}
	return resolved, nil
}

const studyDiagnosticScratchChild = "custom_scp_runtime"

func studyDiagnosticReservedScratchChild(relative string) bool {
	if index := strings.IndexAny(relative, "/\\"); index >= 0 {
		relative = relative[:index]
	}
	return strings.EqualFold(relative, studyDiagnosticScratchChild)
}

type studyDiagnosticArtifactFile struct {
	owner                     *studyDiagnosticArtifacts
	sink                      studyAuditSink
	path                      string
	metadata, failure, closed bool
}

// openFile transfers Sync/Close responsibility to the synchronous caller,
// including after a latch. atomic and copyFile own and finalize their sinks.
func (a *studyDiagnosticArtifacts) openFile(path string, flags int, mode os.FileMode, metadata bool) (*studyDiagnosticArtifactFile, error) {
	return a.open(path, flags, mode, metadata, false)
}

func (a *studyDiagnosticArtifacts) open(path string, flags int, mode os.FileMode, metadata, failure bool) (*studyDiagnosticArtifactFile, error) {
	if a.first != nil && !failure {
		return nil, a.first
	}
	resolved, err := a.destination(path)
	if err != nil {
		return nil, err
	}
	if failure && resolved != filepath.Join(a.root, "FAILED.json.tmp") {
		return nil, a.latch(errors.New("only explicit FAILED output may bypass a diagnostic failure"))
	}
	sink, err := a.ops.open(resolved, flags, mode)
	if err != nil {
		return nil, a.latch(err)
	}
	if studyDiagnosticNil(sink) {
		return nil, a.latch(errors.New("diagnostic opener returned a nil sink"))
	}
	return &studyDiagnosticArtifactFile{owner: a, sink: sink, path: strings.Clone(resolved), metadata: metadata, failure: failure}, nil
}

func (f *studyDiagnosticArtifactFile) Write(data []byte) (int, error) {
	if f.closed {
		return 0, f.owner.latch(os.ErrClosed)
	}
	if f.owner.first != nil && !f.failure {
		return 0, f.owner.first
	}
	if err := f.owner.quota.take(len(data), f.metadata); err != nil {
		return 0, f.owner.latch(err)
	}
	n, err := f.sink.Write(data)
	if n < 0 || n > len(data) {
		return 0, f.owner.latch(errors.Join(err, errors.New("invalid diagnostic writer count")))
	}
	if n != len(data) {
		err = errors.Join(err, io.ErrShortWrite)
	}
	if err != nil {
		return n, f.owner.latch(err)
	}
	return n, nil
}

func (f *studyDiagnosticArtifactFile) Sync() error {
	if f.closed {
		return f.owner.latch(os.ErrClosed)
	}
	return f.owner.latch(f.sink.Sync())
}

func (f *studyDiagnosticArtifactFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	return f.owner.latch(f.sink.Close())
}

func (a *studyDiagnosticArtifacts) jsonBytes(value any, limit int) ([]byte, error) {
	if err := studyDiagnosticJSONPreflight(reflect.ValueOf(value), limit); err != nil {
		return nil, a.latch(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, a.latch(err)
	}
	if len(data) > limit {
		return nil, a.latch(errors.New("diagnostic encoded JSON exceeds limit"))
	}
	return data, nil
}

func (a *studyDiagnosticArtifacts) writeRow(output *studyDiagnosticArtifactFile, value any) error {
	if a.first != nil {
		return a.first
	}
	if output == nil || output.owner != a {
		return a.latch(errors.New("diagnostic row requires this run's owned writer"))
	}
	data, err := a.jsonBytes(value, studyAuditRowBytes-1)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = output.Write(data)
	return err
}

func (a *studyDiagnosticArtifacts) atomicJSON(path string, value any) error {
	return a.atomic(path, value, false)
}

// This is the sole output path permitted after a latch. Metadata/COMPLETE
// helpers cannot use the failure reservation to continue a failed study.
func (a *studyDiagnosticArtifacts) writeFailed(value any) error {
	return a.atomic("FAILED.json", value, true)
}

func (a *studyDiagnosticArtifacts) atomic(path string, value any, failure bool) error {
	if a.first != nil && !failure {
		return a.first
	}
	final, err := a.destination(path)
	if err != nil {
		return err
	}
	tmp, err := a.destination(path + ".tmp")
	if err != nil {
		return err
	}
	if failure && final != filepath.Join(a.root, "FAILED.json") {
		return a.latch(errors.New("invalid diagnostic FAILED destination"))
	}
	for _, target := range []string{final, tmp} {
		if _, err := a.ops.lstat(target); !errors.Is(err, os.ErrNotExist) {
			return a.latch(errors.Join(err, errors.New("diagnostic atomic destination already exists")))
		}
	}
	data, err := a.jsonBytes(value, studyAuditRecordBytes-1)
	if err != nil {
		return err
	}
	file, err := a.open(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, true, failure)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	if _, err := a.destination(final); err != nil {
		return err
	}
	if resolved, err := a.destination(tmp); err != nil || resolved != tmp {
		return a.latch(errors.Join(err, errors.New("diagnostic atomic temporary path changed")))
	}
	if _, err := a.ops.lstat(final); !errors.Is(err, os.ErrNotExist) {
		return a.latch(errors.Join(err, errors.New("diagnostic atomic final destination appeared")))
	}
	return a.latch(a.ops.rename(tmp, final))
}

type studyDiagnosticCopySource interface {
	io.Reader
	Stat() (os.FileInfo, error)
}
type studyDiagnosticCopyReceipt struct {
	Path, SHA256                  string
	BytesCopied, SourceSizeAtStat int64
}

func (a *studyDiagnosticArtifacts) copyFile(path string, source studyDiagnosticCopySource) (studyDiagnosticCopyReceipt, error) {
	var receipt studyDiagnosticCopyReceipt
	if a.first != nil {
		return receipt, a.first
	}
	if studyDiagnosticNil(source) {
		return receipt, a.latch(errors.New("diagnostic copy requires a caller-owned source"))
	}
	info, err := source.Stat()
	if err != nil {
		return receipt, a.latch(err)
	}
	if info == nil || !info.Mode().IsRegular() || info.Size() < 0 {
		return receipt, a.latch(errors.New("diagnostic copy source must be a regular file with nonnegative size"))
	}
	output, err := a.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600, false)
	if err != nil {
		return receipt, err
	}
	hash := sha256.New()
	var count int64
	var buffer [32 << 10]byte
	var copyErr error
	for {
		n, readErr := source.Read(buffer[:])
		if n < 0 || n > len(buffer) {
			copyErr = a.latch(errors.New("invalid diagnostic copy reader count"))
			break
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			copyErr = a.latch(readErr)
			break
		}
		if n > 0 {
			written, writeErr := output.Write(buffer[:n])
			count += int64(written)
			if _, hashErr := hash.Write(buffer[:written]); hashErr != nil {
				copyErr = a.latch(hashErr)
				break
			}
			if writeErr != nil {
				copyErr = writeErr
				break
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if n == 0 {
			copyErr = a.latch(io.ErrNoProgress)
			break
		}
	}
	if err := errors.Join(copyErr, output.Sync(), output.Close()); err != nil {
		return receipt, err
	}
	resolved, err := a.destination(path)
	if err != nil {
		return receipt, err
	}
	return studyDiagnosticCopyReceipt{Path: resolved, SHA256: hex.EncodeToString(hash.Sum(nil)), BytesCopied: count, SourceSizeAtStat: info.Size()}, nil
}

func studyDiagnosticNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	return (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil()
}
