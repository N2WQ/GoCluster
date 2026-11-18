// File role: Own the audit's new files and literal combined output quota.
// Data cannot consume the reserved metadata/failure allowance. Files are
// flushed, synced, closed and hashed before COMPLETE.json is renamed last.
// Partial output is retained on failure; FAILED.json is best effort only.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type studyAuditSink interface {
	io.Writer
	Sync() error
	Close() error
}

type studyAuditQuota struct {
	used    int64
	limit   int64
	reserve int64
}

func (q *studyAuditQuota) take(n int, metadata bool) error {
	limit := q.limit
	if !metadata {
		limit -= q.reserve
	}
	if n < 0 || int64(n) > limit-q.used {
		return errors.New("audit combined output byte quota exhausted")
	}
	q.used += int64(n)
	return nil
}

// The file hooks are local to one writer owner, solely to exercise actual
// short-write/flush/sync/close/rename failures without global process state.
type studyAuditFileOps struct {
	open   func(string) (studyAuditSink, error)
	rename func(string, string) error
}

func studyAuditDefaultFileOps() studyAuditFileOps {
	return studyAuditFileOps{
		open: func(path string) (studyAuditSink, error) {
			return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		},
		rename: os.Rename,
	}
}

type studyAuditWriter struct {
	file   studyAuditSink
	buffer *bufio.Writer
	quota  *studyAuditQuota
	path   string
	closed bool
}

func studyAuditOpenWriter(path string, quota *studyAuditQuota, ops studyAuditFileOps) (*studyAuditWriter, error) {
	file, err := ops.open(path)
	if err != nil {
		return nil, err
	}
	return &studyAuditWriter{file: file, buffer: bufio.NewWriterSize(file, 64<<10), quota: quota, path: path}, nil
}

func (w *studyAuditWriter) record(value any) error {
	// Each caller supplies one flat, prebounded receipt, never the day map.
	if err := studyAuditLabels(reflect.ValueOf(value)); err != nil {
		return err
	}
	if err := studyAuditEncodedBound(reflect.ValueOf(value), studyAuditRowBytes); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) >= studyAuditRecordBytes {
		return errors.New("audit JSONL record exceeds 8 MiB")
	}
	if err := w.quota.take(len(data)+1, false); err != nil {
		return err
	}
	if n, err := w.buffer.Write(data); err != nil || n != len(data) {
		return errors.Join(err, io.ErrShortWrite)
	}
	return w.buffer.WriteByte('\n')
}

func (w *studyAuditWriter) close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	return errors.Join(w.buffer.Flush(), w.file.Sync(), w.file.Close())
}

func (w *studyAuditWriter) binding() (studyFile, error) {
	if !w.closed {
		return studyFile{}, errors.New("audit cannot hash an open output")
	}
	sum, err := studyHashFile(w.path)
	return studyFile{Path: w.path, SHA256: sum}, err
}

func studyAuditAtomicJSON(output, name string, value any, quota *studyAuditQuota, ops studyAuditFileOps) error {
	if err := studyAuditLabels(reflect.ValueOf(value)); err != nil {
		return err
	}
	if err := studyAuditEncodedBound(reflect.ValueOf(value), studyAuditRecordBytes); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > studyAuditRecordBytes {
		return errors.New("audit metadata exceeds 8 MiB")
	}
	if err := quota.take(len(data)+1, true); err != nil {
		return err
	}
	path, tmp := filepath.Join(output, name), filepath.Join(output, name+".tmp")
	file, err := ops.open(tmp)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(err, fmt.Errorf("audit refuses existing %s", name))
	}
	return ops.rename(tmp, path)
}

// Upper-bound JSON encoding before allocation. Our callers pass only concrete
// audit structs, maps, scalars, RawMessages and time.Time. Six bytes per string
// byte covers all JSON/HTML escapes; integer/float scalars fit 32 bytes. This
// conservative check can refuse an unusually large row rather than allocate it.
func studyAuditEncodedBound(value reflect.Value, limit int) error {
	left := int64(limit)
	var visit func(reflect.Value) bool
	take := func(n int64) bool {
		if n < 0 || n > left {
			return false
		}
		left -= n
		return true
	}
	visit = func(v reflect.Value) bool {
		if !v.IsValid() {
			return take(4)
		}
		if v.Type() == reflect.TypeFor[time.Time]() {
			return take(40)
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return take(4)
			}
			return visit(v.Elem())
		case reflect.String:
			return take(int64(v.Len())*6 + 2)
		case reflect.Bool:
			return take(5)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			return take(32)
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice && v.IsNil() {
				return take(4)
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return take(int64(v.Len())*6 + 2)
			}
			if !take(2 + int64(v.Len())) {
				return false
			}
			for i := 0; i < v.Len(); i++ {
				if !visit(v.Index(i)) {
					return false
				}
			}
			return true
		case reflect.Struct:
			if !take(2) {
				return false
			}
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				if field.PkgPath != "" || field.Tag.Get("json") == "-" {
					continue
				}
				if !take(int64(len(field.Name)+len(field.Tag.Get("json")))*6+4) || !visit(v.Field(i)) {
					return false
				}
			}
			return true
		case reflect.Map:
			if v.IsNil() {
				return take(4)
			}
			if !take(2 + int64(v.Len())) {
				return false
			}
			iter := v.MapRange()
			for iter.Next() {
				if !visit(iter.Key()) || !take(1) || !visit(iter.Value()) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !visit(value) {
		return errors.New("audit encoded row/record exceeds preallocation byte bound")
	}
	return nil
}

func studyAuditFailure(output string, cause error, quota *studyAuditQuota, ops studyAuditFileOps) error {
	reason := cause.Error()
	if len(reason) > studyAuditLabelBytes {
		reason = reason[:studyAuditLabelBytes]
	}
	// Truncation may cut a UTF-8 sequence. A one-byte replacement preserves
	// the decoded label bound rather than letting JSON expand it to U+FFFD.
	reason = strings.ToValidUTF8(reason, "?")
	if err := studyAuditAtomicJSON(output, "FAILED.json", struct {
		Complete bool   `json:"complete"`
		Reason   string `json:"reason"`
	}{false, reason}, quota, ops); err != nil {
		return fmt.Errorf("write best-effort audit failure marker: %w", err)
	}
	return nil
}
