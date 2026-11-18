// File role: Observe only the finite quota-owned writer inventory for one run.
// Tracking never owns a second handle or changes flags. Active counts allow
// repeated APPEND opens; final hashing requires every actual close and sync.
package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type studyDiagnosticFileState struct {
	Path          string
	Active        int
	UnsyncedClose bool
}

type studyDiagnosticCopyRecord struct {
	Receipt               studyDiagnosticCopyReceipt `json:"receipt"`
	VerifiedBeforeCleanup bool                       `json:"verified_before_cleanup"`
	DeletedAfterCleanup   bool                       `json:"deleted_after_cleanup"`
}

type studyDiagnosticOutputFile struct {
	File  studyFile `json:"file"`
	Bytes int64     `json:"bytes"`
}

type studyDiagnosticTrackedSink struct {
	studyAuditSink
	state         *studyDiagnosticFileState
	dirty, closed bool
}

func (f *studyDiagnosticTrackedSink) Write(data []byte) (int, error) {
	n, err := f.studyAuditSink.Write(data)
	if n > 0 {
		f.dirty = true
	}
	return n, err
}
func (f *studyDiagnosticTrackedSink) Sync() error {
	err := f.studyAuditSink.Sync()
	if err == nil {
		f.dirty = false
	}
	return err
}
func (f *studyDiagnosticTrackedSink) Close() error {
	err := f.studyAuditSink.Close()
	if !f.closed {
		f.closed = true
		f.state.Active--
		f.state.UnsyncedClose = f.state.UnsyncedClose || f.dirty
	}
	return err
}

func studyDiagnosticPathKey(path string) string { return strings.ToLower(filepath.Clean(path)) }

func (d *studyDiagnosticRun) trackFiles() {
	open, rename := d.owner.ops.open, d.owner.ops.rename
	d.owner.ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
		key := studyDiagnosticPathKey(path)
		state := d.files[key]
		if state == nil && len(d.files) >= 256 {
			return nil, errors.New("diagnostic file inventory exceeds256 paths")
		}
		file, err := open(path, flags, mode)
		if err != nil || studyDiagnosticNil(file) {
			return file, err
		}
		if state == nil {
			state = &studyDiagnosticFileState{Path: strings.Clone(path)}
			d.files[key] = state
		}
		state.Active++
		return &studyDiagnosticTrackedSink{studyAuditSink: file, state: state}, nil
	}
	d.owner.ops.rename = func(source, destination string) error {
		sourceKey, destinationKey := studyDiagnosticPathKey(source), studyDiagnosticPathKey(destination)
		state := d.files[sourceKey]
		if state == nil || state.Active != 0 || d.files[destinationKey] != nil {
			return errors.New("diagnostic rename requires a closed tracked source and new destination")
		}
		if err := rename(source, destination); err != nil {
			return err
		}
		delete(d.files, sourceKey)
		state.Path = strings.Clone(destination)
		d.files[destinationKey] = state
		return nil
	}
}

func (d *studyDiagnosticRun) noteCopy(receipt studyDiagnosticCopyReceipt) error {
	if len(d.copies) >= 128 {
		return d.owner.latch(errors.New("diagnostic frozen-copy receipts exceed128"))
	}
	if !studyValidSHA(receipt.SHA256) || receipt.BytesCopied < 0 {
		return d.owner.latch(errors.New("invalid diagnostic frozen-copy receipt"))
	}
	path, err := d.owner.destination(receipt.Path)
	if err != nil {
		return err
	}
	for i := range d.copies {
		if studyDiagnosticPathKey(d.copies[i].Receipt.Path) == studyDiagnosticPathKey(path) {
			return d.owner.latch(errors.New("duplicate diagnostic frozen-copy destination"))
		}
	}
	receipt.Path = strings.Clone(path)
	d.copies = append(d.copies, studyDiagnosticCopyRecord{Receipt: receipt})
	return nil
}

func (d *studyDiagnosticRun) renameAsset(source, destination string) error {
	if d.owner.Err() != nil {
		return d.owner.Err()
	}
	source, err := d.owner.destination(source)
	if err != nil {
		return err
	}
	destination, err = d.owner.destination(destination)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return d.owner.latch(errors.Join(err, errors.New("diagnostic asset rename destination already exists")))
	}
	index := -1
	for i := range d.copies {
		if studyDiagnosticPathKey(d.copies[i].Receipt.Path) == studyDiagnosticPathKey(source) {
			index = i
			break
		}
	}
	if index < 0 {
		return d.owner.latch(errors.New("diagnostic asset rename has no copy receipt"))
	}
	if err := d.owner.ops.rename(source, destination); err != nil {
		return d.owner.latch(err)
	}
	d.copies[index].Receipt.Path = strings.Clone(destination)
	return nil
}

func (d *studyDiagnosticRun) verifyCopies(afterCleanup bool) error {
	if afterCleanup && d.licenseRoot != "" {
		if _, err := os.Lstat(d.licenseRoot); !os.IsNotExist(err) {
			return d.owner.latch(errors.Join(err, errors.New("actual license copy root was not deleted by successful existing cleanup")))
		}
	}
	for i := range d.copies {
		copyRecord := &d.copies[i]
		receipt := &copyRecord.Receipt
		_, err := os.Lstat(receipt.Path)
		if afterCleanup && copyRecord.VerifiedBeforeCleanup && d.licenseRoot != "" && strings.EqualFold(filepath.Dir(receipt.Path), d.licenseRoot) && os.IsNotExist(err) {
			copyRecord.DeletedAfterCleanup = true
			continue
		}
		if err != nil {
			return d.owner.latch(err)
		}
		if err := studyCheckFile(studyFile{Path: receipt.Path, SHA256: receipt.SHA256}); err != nil {
			return d.owner.latch(err)
		}
		copyRecord.VerifiedBeforeCleanup = true
	}
	return nil
}

func (d *studyDiagnosticRun) closedOutputs() ([]studyDiagnosticOutputFile, int64, error) {
	deleted := make(map[string]int64, len(d.copies))
	var deletedBytes int64
	for i := range d.copies {
		if d.copies[i].DeletedAfterCleanup {
			deleted[studyDiagnosticPathKey(d.copies[i].Receipt.Path)] = d.copies[i].Receipt.BytesCopied
			deletedBytes += d.copies[i].Receipt.BytesCopied
		}
	}
	keys := make([]string, 0, len(d.files))
	for key := range d.files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	outputs := make([]studyDiagnosticOutputFile, 0, len(keys))
	for _, key := range keys {
		state := d.files[key]
		if state.Active != 0 || state.UnsyncedClose {
			return nil, 0, d.owner.latch(errors.New("diagnostic output still open or closed with unsynced writes"))
		}
		if _, removed := deleted[key]; removed {
			continue
		}
		path, err := d.owner.destination(state.Path)
		if err != nil {
			return nil, 0, err
		}
		info, err := os.Lstat(path)
		if err != nil || info == nil || !info.Mode().IsRegular() {
			return nil, 0, d.owner.latch(errors.Join(err, errors.New("diagnostic closed output missing or nonregular")))
		}
		digest, err := studyHashFile(path)
		if err != nil {
			return nil, 0, d.owner.latch(err)
		}
		outputs = append(outputs, studyDiagnosticOutputFile{File: studyFile{Path: path, SHA256: digest}, Bytes: info.Size()})
	}
	return outputs, deletedBytes, nil
}
