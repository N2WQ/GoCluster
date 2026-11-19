// File role: Journal actual Observe attempts and join only requested event IDs.
// One run goroutine owns the journal. The raw day remains caller-owned through
// Join; no complete daily ID map is retained. Sync/Close precede read ownership.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"dxcluster/internal/correctionflow"
)

type studyDiagnosticAttempt struct {
	ID         uint64                       `json:"id"`
	Binding    studyDiagnosticRawBinding    `json:"binding"`
	Key        correctionflow.DiagnosticKey `json:"key"`
	ObservedAt time.Time                    `json:"observed_at"`
	Accepted   bool                         `json:"accepted"`
	Reason     string                       `json:"reason"`
}

type studyDiagnosticWantedEvent struct {
	Key        correctionflow.DiagnosticKey
	ObservedAt time.Time
}

type studyDiagnosticWanted map[uint64]studyDiagnosticWantedEvent

func (wanted *studyDiagnosticWanted) addEvaluate(capture *correctionflow.TemporalEvaluateDiagnostics) error {
	if capture == nil || len(capture.Window) > 32 {
		return errors.New("invalid diagnostic Evaluate event window")
	}
	if len(capture.Window) == 0 {
		return nil
	}
	if err := studyDiagnosticCheckKey(capture.Key); err != nil {
		return err
	}
	for i := range capture.Window {
		event := &capture.Window[i]
		if event.ID == 0 || event.ObservedAt.IsZero() {
			return errors.New("diagnostic wanted event requires actual ID/time")
		}
		expected := studyDiagnosticWantedEvent{Key: capture.Key, ObservedAt: event.ObservedAt}
		if current, exists := (*wanted)[event.ID]; exists {
			if current.Key != expected.Key || !current.ObservedAt.Equal(expected.ObservedAt) {
				return errors.New("conflicting diagnostic wanted key/time")
			}
			continue
		}
		if len(*wanted) >= 16384 {
			return errors.New("diagnostic wanted event limit exceeds 16384")
		}
		if *wanted == nil {
			*wanted = make(studyDiagnosticWanted)
		}
		expected.Key.Band, expected.Key.Mode = strings.Clone(expected.Key.Band), strings.Clone(expected.Key.Mode)
		(*wanted)[event.ID] = expected
	}
	return nil
}

func studyDiagnosticCheckKey(key correctionflow.DiagnosticKey) error {
	if len(key.Band) > 1024 || len(key.Mode) > 1024 || !utf8.ValidString(key.Band) || !utf8.ValidString(key.Mode) || strings.TrimSpace(key.Band) == "" || strings.TrimSpace(key.Mode) == "" {
		return errors.New("diagnostic event requires a supplied bounded key")
	}
	return nil
}

type studyDiagnosticJournal struct {
	owner             *studyDiagnosticArtifacts
	file              *studyDiagnosticArtifactFile
	day               *studyRawDay
	path              string
	start, count      uint64
	previousOrdinal   int64
	finalized, closed bool
}

// startNextID is the actual next counter BEFORE attempts: the first recorded
// ID equals startNextID, and the final next counter equals startNextID+count.
func newStudyDiagnosticJournal(owner *studyDiagnosticArtifacts, path string, day *studyRawDay, startNextID uint64) (*studyDiagnosticJournal, error) {
	if owner == nil {
		return nil, errors.New("diagnostic journal requires an artifact owner")
	}
	if day == nil || len(day.Rows) > studyMaxRows || !studyValidSHA(day.InputSHA256) || startNextID == 0 || startNextID > math.MaxUint64-studyMaxRows {
		return nil, owner.latch(errors.New("invalid diagnostic journal day or starting ID"))
	}
	file, err := owner.openFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
	if err != nil {
		return nil, err
	}
	return &studyDiagnosticJournal{owner: owner, file: file, day: day, path: strings.Clone(path), start: startNextID}, nil
}

func (j *studyDiagnosticJournal) Record(attempt studyDiagnosticAttempt) error {
	if j.owner.Err() != nil {
		return j.owner.Err()
	}
	if j.finalized || j.count >= uint64(len(j.day.Rows)) || attempt.ID != j.start+j.count || attempt.Binding.Ordinal <= j.previousOrdinal {
		return j.owner.latch(errors.New("diagnostic journal requires contiguous IDs, increasing raw ordinals and daily row bound"))
	}
	if err := studyDiagnosticValidateAttempt(j.day, attempt); err != nil {
		return j.owner.latch(err)
	}
	if err := j.owner.writeRow(j.file, attempt); err != nil {
		return err
	}
	j.count++
	j.previousOrdinal = attempt.Binding.Ordinal
	return nil
}

func studyDiagnosticValidateAttempt(day *studyRawDay, attempt studyDiagnosticAttempt) error {
	if err := studyDiagnosticCheckRawBinding(day, attempt.Binding); err != nil {
		return err
	}
	if err := studyDiagnosticCheckKey(attempt.Key); err != nil {
		return err
	}
	if attempt.ID == 0 || attempt.ObservedAt.IsZero() || len(attempt.Reason) > 1024 || !utf8.ValidString(attempt.Reason) {
		return errors.New("invalid actual diagnostic attempt ID/time/reason")
	}
	return nil
}

func (j *studyDiagnosticJournal) Close() error {
	if j.finalized {
		return j.owner.Err()
	}
	j.finalized = true
	err := errors.Join(j.file.Sync(), j.file.Close())
	j.closed = err == nil
	return err
}

// Join returns at most 16384 accepted actual attempts. It verifies the entire
// journal, including rejected/unselected rows, before returning any result.
func (j *studyDiagnosticJournal) Join(wanted studyDiagnosticWanted, endNextID, actualAttempts uint64) (out []studyDiagnosticAttempt, err error) {
	if j.owner.Err() != nil {
		return nil, j.owner.Err()
	}
	if !j.finalized || !j.closed || actualAttempts != j.count {
		return nil, j.owner.latch(errors.New("diagnostic journal must be closed and reconciled before join"))
	}
	path, err := j.owner.destination(j.path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, j.owner.latch(err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			out = nil
			err = j.owner.latch(err)
		}
	}()
	return studyDiagnosticReadJournal(file, j.day, wanted, j.start, endNextID, actualAttempts)
}

func studyDiagnosticReadJournal(reader io.Reader, day *studyRawDay, wanted studyDiagnosticWanted, startNextID, endNextID, actualAttempts uint64) ([]studyDiagnosticAttempt, error) {
	if day == nil || len(wanted) > 16384 || actualAttempts > uint64(len(day.Rows)) || actualAttempts > studyMaxRows || startNextID == 0 || startNextID > math.MaxUint64-actualAttempts || endNextID != startNextID+actualAttempts {
		return nil, errors.New("diagnostic journal counter/size reconciliation failed")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), studyAuditRowBytes)
	scanner.Split(studyDiagnosticJournalSplit)
	matched := make(map[uint64]bool, len(wanted))
	out := make([]studyDiagnosticAttempt, 0, len(wanted))
	var count uint64
	var previousOrdinal int64
	for scanner.Scan() {
		if count >= actualAttempts || len(scanner.Bytes())+1 > studyAuditRowBytes {
			return nil, errors.New("diagnostic journal has excess rows or oversized framing")
		}
		var attempt studyDiagnosticAttempt
		if err := studyDiagnosticDecode(scanner.Bytes(), diagnosticWireAttempt, &attempt); err != nil {
			return nil, err
		}
		if attempt.ID != startNextID+count {
			return nil, errors.New("diagnostic journal has a missing/duplicate/reordered actual attempt ID")
		}
		if attempt.Binding.Ordinal <= previousOrdinal {
			return nil, errors.New("diagnostic journal raw ordinals must strictly increase")
		}
		if err := studyDiagnosticValidateAttempt(day, attempt); err != nil {
			return nil, err
		}
		count++
		previousOrdinal = attempt.Binding.Ordinal
		if expected, needed := wanted[attempt.ID]; needed {
			if !attempt.Accepted || expected.Key != attempt.Key || !expected.ObservedAt.Equal(attempt.ObservedAt) {
				return nil, errors.New("diagnostic wanted event has rejected/conflicting actual binding")
			}
			matched[attempt.ID] = true
			out = append(out, attempt)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if count != actualAttempts || len(matched) != len(wanted) {
		return nil, errors.New("diagnostic journal missing complete attempt count or wanted IDs")
	}
	return out, nil
}

func studyDiagnosticJournalSplit(data []byte, atEOF bool) (int, []byte, error) {
	if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
		return newline + 1, data[:newline], nil
	}
	if atEOF && len(data) != 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return 0, nil, nil
}
