// File role: Captures exact user-record bytes for a later atomic replacement.
// Preparation owns validation/metadata work; commit owns only the captured
// path and bytes. Callers retain their existing full-callsign transaction lease.
package filter

import (
	"errors"
	"path/filepath"
	"strings"
)

// PreparedUserRecord owns an immutable target path and final YAML bytes. It
// retains no caller record, maps, lists, baseline or writer closure. Preparation
// does not grant transaction authority: the caller must retain its existing
// full-callsign ownership through Commit. The zero value cannot be committed.
type PreparedUserRecord struct {
	path string
	data []byte
}

// ByteLen reports the exact final encoded length, or zero for a nil payload.
func (prepared *PreparedUserRecord) ByteLen() int {
	if prepared == nil {
		return 0
	}
	return len(prepared.data)
}

// Commit atomically writes the captured bytes without loading, validating,
// normalizing or encoding again. It uses the same writer as existing saves.
func (prepared *PreparedUserRecord) Commit() error {
	return prepared.commitWith(writeAtomicUserFile)
}

func (prepared *PreparedUserRecord) commitWith(write func(string, []byte) error) error {
	if err := prepared.validate(); err != nil {
		return err
	}
	return write(prepared.path, prepared.data)
}

func (prepared *PreparedUserRecord) validate() error {
	if prepared == nil || prepared.path == "" || len(prepared.data) == 0 {
		return errors.New("unprepared user record")
	}
	if len(prepared.data) > MaxUserRecordBytes {
		return storageAdmissionf("user record exceeds %d bytes", MaxUserRecordBytes)
	}
	return nil
}

// PrepareUserRecord validates and encodes the supplied record without durable
// effects. Callers must synchronize or detach its maps/lists for the entire
// preparation. Configuration, baseline and exact final record bytes must fit
// their independent bounds before a replacement payload can be returned.
func PrepareUserRecord(callsign string, record *UserRecord) (*PreparedUserRecord, error) {
	if record == nil {
		return nil, errors.New("nil user record")
	}
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return nil, errors.New("empty callsign")
	}
	data, err := encodeUserRecord(record)
	if err != nil {
		return nil, err
	}
	// Capture relative UserDataDir against this working directory. Later changes
	// to either setting cannot redirect a prepared replacement. No symlinks or
	// filesystem objects are resolved or created during preparation.
	path, err := filepath.Abs(userRecordPath(callsign))
	if err != nil {
		return nil, err
	}
	return &PreparedUserRecord{path: path, data: data}, nil
}
