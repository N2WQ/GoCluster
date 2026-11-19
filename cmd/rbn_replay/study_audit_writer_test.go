package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type studyAuditFaultSink struct {
	studyAuditSink
	fault string
}

func (s *studyAuditFaultSink) Write(data []byte) (int, error) {
	if s.fault == "short" {
		n := len(data) / 2
		_, err := s.studyAuditSink.Write(data[:n])
		return n, err
	}
	if s.fault == "write" {
		return 0, errors.New("injected write failure")
	}
	return s.studyAuditSink.Write(data)
}

func (s *studyAuditFaultSink) Sync() error {
	err := s.studyAuditSink.Sync()
	if s.fault == "sync" {
		err = errors.Join(err, errors.New("injected sync failure"))
	}
	return err
}

func (s *studyAuditFaultSink) Close() error {
	err := s.studyAuditSink.Close()
	if s.fault == "close" {
		err = errors.Join(err, errors.New("injected close failure"))
	}
	return err
}

func TestStudyAuditWriterFailuresNeverComplete(t *testing.T) {
	for _, fault := range []string{"short", "write", "sync", "close", "aggregate_short", "complete_short", "rename", "aggregate_rename"} {
		t.Run(fault, func(t *testing.T) {
			f := studyAuditTestInputs(t)
			out := filepath.Join(t.TempDir(), "audit")
			ops := studyAuditDefaultFileOps()
			open, rename := ops.open, ops.rename
			ops.open = func(path string) (studyAuditSink, error) {
				file, err := open(path)
				if err != nil {
					return nil, err
				}
				if strings.HasSuffix(path, "paired_union.jsonl") && (fault == "short" || fault == "write" || fault == "sync" || fault == "close") {
					return &studyAuditFaultSink{studyAuditSink: file, fault: fault}, nil
				}
				if fault == "aggregate_short" && strings.HasSuffix(path, "aggregates.json.tmp") || fault == "complete_short" && strings.HasSuffix(path, "COMPLETE.json.tmp") {
					return &studyAuditFaultSink{studyAuditSink: file, fault: "short"}, nil
				}
				return file, nil
			}
			ops.rename = func(from, to string) error {
				if fault == "rename" && strings.HasSuffix(to, "COMPLETE.json") || fault == "aggregate_rename" && strings.HasSuffix(to, "aggregates.json") {
					return errors.New("injected rename failure")
				}
				return rename(from, to)
			}
			if err := studyAuditRun(f.Audit.Path, f.Audit.SHA256, out, ops); err == nil {
				t.Fatal("fault did not fail audit")
			}
			if _, err := os.Stat(filepath.Join(out, "COMPLETE.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failure published COMPLETE: %v", err)
			}
			if _, err := os.Stat(filepath.Join(out, "FAILED.json")); err != nil {
				t.Fatalf("writable output lost best-effort FAILED marker: %v", err)
			}
			if _, err := os.Stat(filepath.Join(out, "paired_union.jsonl")); err != nil {
				t.Fatalf("partial evidence was erased: %v", err)
			}
		})
	}
}

func TestStudyAuditWriterLiteralQuotaAndShortFlush(t *testing.T) {
	q := &studyAuditQuota{limit: 100, reserve: 20}
	if err := q.take(80, false); err != nil {
		t.Fatal(err)
	}
	if err := q.take(1, false); err == nil {
		t.Fatal("data consumed metadata reserve")
	}
	if err := q.take(19, true); err != nil {
		t.Fatal(err)
	}
	if err := q.take(2, true); err == nil {
		t.Fatal("metadata exceeded combined quota")
	}
	if q.used != 99 {
		t.Fatal("refused bytes consumed quota")
	}
	q = &studyAuditQuota{limit: 1024, reserve: 100}
	ops := studyAuditDefaultFileOps()
	original := ops.open
	ops.open = func(path string) (studyAuditSink, error) {
		file, err := original(path)
		if err != nil {
			return nil, err
		}
		return &studyAuditFaultSink{studyAuditSink: file, fault: "short"}, nil
	}
	w, err := studyAuditOpenWriter(filepath.Join(t.TempDir(), "rows.jsonl"), q, ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.record(struct {
		Value string `json:"value"`
	}{"bounded"}); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short underlying write at Flush was not detected: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatal("close should be idempotent")
	}
}

func TestStudyAuditAtomicNoOverwriteAndFailedOpen(t *testing.T) {
	out := t.TempDir()
	quota := &studyAuditQuota{limit: 10000, reserve: 2000}
	ops := studyAuditDefaultFileOps()
	path := filepath.Join(out, "COMPLETE.json")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := studyAuditAtomicJSON(out, "COMPLETE.json", struct {
		Complete bool `json:"complete"`
	}{true}, quota, ops); err == nil {
		t.Fatal("existing evidence overwritten")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "existing" {
		t.Fatal("existing evidence changed")
	}
	f := studyAuditTestInputs(t)
	output := filepath.Join(t.TempDir(), "failure")
	storageErr, markerErr := errors.New("storage unavailable"), errors.New("marker unavailable")
	openCalls := 0
	ops.open = func(string) (studyAuditSink, error) {
		openCalls++
		if openCalls == 1 {
			return nil, storageErr
		}
		return nil, markerErr
	}
	if err := studyAuditRun(f.Audit.Path, f.Audit.SHA256, output, ops); !errors.Is(err, storageErr) || !errors.Is(err, markerErr) {
		t.Fatalf("open/marker failure lost original cause: %v", err)
	}
	if _, err := os.Stat(filepath.Join(output, "COMPLETE.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("open failure published complete")
	}
	// FAILED is intentionally not guaranteed when even its storage is unavailable.
}

func TestStudyAuditEncodedNullPreflight(t *testing.T) {
	for _, value := range []any{map[string]int(nil), []int(nil), []byte(nil)} {
		if err := studyAuditEncodedBound(reflect.ValueOf(value), 3); err == nil {
			t.Fatal("JSON null undercounted before allocation")
		}
		if err := studyAuditEncodedBound(reflect.ValueOf(value), 4); err != nil {
			t.Fatal("literal null bound:", err)
		}
	}
	for _, value := range []any{map[string]int{}, []int{}} {
		if err := studyAuditEncodedBound(reflect.ValueOf(value), 2); err != nil {
			t.Fatal("literal empty composite bound:", err)
		}
	}
}

func TestStudyAuditFailureReasonUTF8ByteLimit(t *testing.T) {
	out := t.TempDir()
	quota := &studyAuditQuota{limit: 10000, reserve: 2000}
	if err := studyAuditFailure(out, errors.New(strings.Repeat("€", 500)), quota, studyAuditDefaultFileOps()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "FAILED.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatal(err)
	}
	if len(marker.Reason) > studyAuditLabelBytes || !strings.HasSuffix(marker.Reason, "?") {
		t.Fatalf("truncated UTF-8 reason exceeds decoded byte cap: bytes=%d", len(marker.Reason))
	}
}
