// File role: Exact storage admission and bounded physical record reads.
// Resource refusals never grant replacement authority; current-record load
// failures retain their cause so callers can distinguish temporary human edits.
package filter

import (
	"errors"
	"fmt"
	"io"
)

// MaxUserRecordBytes bounds raw, normalized and prospective user records.
const MaxUserRecordBytes = 1 << 20

const (
	maxUserRecordWork       = 2_097_152
	maxPresetCollectionWork = 16_777_216
)

// ErrStorageAdmission identifies a byte or decoding-work resource refusal.
var ErrStorageAdmission = errors.New("stored configuration resource admission refused")

// ErrCurrentUserRecord identifies failure to load the authoritative destination,
// independently of admission of the newly combined prospective record.
var ErrCurrentUserRecord = errors.New("current user record could not be loaded")

// AdmitUserRecord checks detached session data without acquiring path or write
// authority. Temporary admission never authorizes replacing a refused record.
func AdmitUserRecord(record *UserRecord) error {
	if record == nil {
		return errors.New("nil user record")
	}
	_, err := encodeUserRecord(record)
	return err
}

type storageAdmissionError struct {
	message string
	cause   error
}

func (err *storageAdmissionError) Error() string { return err.message }
func (err *storageAdmissionError) Unwrap() error { return ErrStorageAdmission }
func (err *storageAdmissionError) Is(target error) bool {
	return err.cause != nil && errors.Is(err.cause, target)
}

func storageAdmissionf(format string, args ...any) error {
	return &storageAdmissionError{message: fmt.Sprintf(format, args...)}
}

type currentUserRecordError struct{ cause error }

func (err *currentUserRecordError) Error() string {
	return fmt.Sprintf("load current user record: %v", err.cause)
}

func (err *currentUserRecordError) Unwrap() error        { return err.cause }
func (err *currentUserRecordError) Is(target error) bool { return target == ErrCurrentUserRecord }

// Read at most one distinguishing byte beyond the bound. A stat or a read-all
// length check cannot bound bytes consumed when the file changes during reading.
func readUserRecordBytes(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxUserRecordBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxUserRecordBytes {
		return nil, storageAdmissionf("user record exceeds %d bytes", MaxUserRecordBytes)
	}
	return data, nil
}

func recordConfiguration(record *UserRecord) Configuration {
	return ConfigurationFromFilter(&record.Filter, SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass,
		DedupePolicy: record.DedupePolicy, PathMinObservationCount: record.PathMinObservationCount,
		SolarSummaryMinutes: record.SolarSummaryMinutes,
	})
}

// encodeUserRecord is pure: both canonical loading and prospective preparation
// use the same v6 encoding, without path resolution or changes to caller data.
// Configuration and baseline use the independent standalone SavedPreset oracle.
func encodeUserRecord(record *UserRecord) ([]byte, error) {
	stored, err := V6UserRecordFromRuntime(record)
	if err != nil {
		return nil, err
	}
	return EncodeV6UserRecord(stored)
}
