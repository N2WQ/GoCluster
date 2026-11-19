package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"dxcluster/internal/correctionflow"
	rbnfeed "dxcluster/rbn"
)

func diagnosticAttemptFixture() (*studyRawDay, []studyDiagnosticAttempt, studyDiagnosticWanted) {
	day, bindings := diagnosticFoundationDay()
	key := correctionflow.DiagnosticKey{Band: "20m", Mode: "CW", Bucket: 14020, ToleranceHz: 100}
	at := day.Rows[0].Row.Time.Add(5 * time.Second)
	attempts := []studyDiagnosticAttempt{
		{ID: 7, Binding: bindings[0], Key: key, ObservedAt: at, Accepted: true, Reason: "held"},
		{ID: 8, Binding: bindings[1], Key: key, ObservedAt: at, Accepted: false, Reason: "request_cap"},
		{ID: 9, Binding: bindings[2], Key: key, ObservedAt: at.Add(time.Second), Accepted: true, Reason: "held"},
	}
	wanted := studyDiagnosticWanted{7: {Key: key, ObservedAt: at}, 9: {Key: key, ObservedAt: at.Add(time.Second)}}
	return day, attempts, wanted
}

func diagnosticJournalBytes(t *testing.T, attempts []studyDiagnosticAttempt) []byte {
	t.Helper()
	var result bytes.Buffer
	for _, attempt := range attempts {
		data, err := json.Marshal(attempt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := result.WriteByte('\n'); err != nil {
			t.Fatal(err)
		}
	}
	return result.Bytes()
}

func TestDiagnosticMappingActualAttemptJoin(t *testing.T) {
	day, attempts, wanted := diagnosticAttemptFixture()
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, 7)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range attempts {
		if err := journal.Record(attempt); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	joined, err := journal.Join(wanted, 10, 3)
	if err != nil || len(joined) != 2 || joined[0].Binding.RawID != attempts[0].Binding.RawID || joined[1].Binding.RawID != attempts[2].Binding.RawID {
		t.Fatal("actual accepted unselected contributing event was not joined:", err)
	}
	if joined[0].ObservedAt.Equal(day.Rows[0].Row.Time) || attempts[0].Binding.RawID == attempts[1].Binding.RawID || attempts[0].Binding.RawSHA256 != attempts[1].Binding.RawSHA256 {
		t.Fatal("literal equal-call/time raw IDs or supplied decision clock fixture changed")
	}
	if err := journal.Close(); err != nil {
		t.Fatal("journal closed twice:", err)
	}
}

func TestDiagnosticMappingEmptyAndHighIDCounters(t *testing.T) {
	for _, count := range []int{0, 3} {
		day, attempts, _ := diagnosticAttemptFixture()
		owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
		start := uint64(1)
		if count != 0 {
			start = (1 << 53) + 17
		}
		journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, start)
		if err != nil {
			t.Fatal(err)
		}
		wanted := studyDiagnosticWanted{}
		for i := 0; i < count; i++ {
			attempts[i].ID = start + uint64(i)
			if err := journal.Record(attempts[i]); err != nil {
				t.Fatal(err)
			}
			if attempts[i].Accepted {
				wanted[attempts[i].ID] = studyDiagnosticWantedEvent{Key: attempts[i].Key, ObservedAt: attempts[i].ObservedAt}
			}
		}
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
		joined, err := journal.Join(wanted, start+uint64(count), uint64(count))
		if err != nil || len(joined) != len(wanted) {
			t.Fatal("empty/high-ID journal counter contract failed:", err)
		}
		if count != 0 && joined[0].ID != (1<<53)+17 {
			t.Fatal("actual uint64 attempt ID lost precision")
		}
	}
	day, _, _ := diagnosticAttemptFixture()
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	if journal, err := newStudyDiagnosticJournal(owner, "invalid.jsonl", day, 0); err == nil || journal != nil || owner.Err() == nil {
		t.Fatal("zero actual starting ID constructed a journal")
	}
	if _, err := os.Stat(filepath.Join(owner.root, "invalid.jsonl")); !os.IsNotExist(err) {
		t.Fatal("invalid journal construction created evidence")
	}
}

func TestDiagnosticMappingCompleteJournalRefusals(t *testing.T) {
	day, attempts, wanted := diagnosticAttemptFixture()
	for _, variant := range []string{"duplicate", "missing_unwanted", "missing_last", "reorder", "excess", "wrong_end", "wrong_count", "raw_sha", "raw_ordinal", "rejected", "key", "time", "missing_wanted", "newline", "unknown"} {
		t.Run(variant, func(t *testing.T) {
			rows := append([]studyDiagnosticAttempt(nil), attempts...)
			need := make(studyDiagnosticWanted, len(wanted))
			for id, value := range wanted {
				need[id] = value
			}
			end, count := uint64(10), uint64(3)
			switch variant {
			case "duplicate":
				rows[1].ID = rows[0].ID
			case "missing_unwanted":
				rows = append(rows[:1], rows[2:]...)
			case "missing_last":
				rows = rows[:2]
				need = studyDiagnosticWanted{}
			case "reorder":
				rows[0], rows[2] = rows[2], rows[0]
			case "excess":
				rows = append(rows, rows[2])
			case "wrong_end":
				end++
			case "wrong_count":
				count = 2
			case "raw_sha":
				rows[0].Binding.RawSHA256 = strings.Repeat("0", 64)
			case "raw_ordinal":
				rows[0].Binding.Ordinal = 2
			case "rejected":
				need[8] = studyDiagnosticWantedEvent{Key: rows[1].Key, ObservedAt: rows[1].ObservedAt}
			case "key":
				entry := need[7]
				entry.Key.Bucket++
				need[7] = entry
			case "time":
				entry := need[7]
				entry.ObservedAt = entry.ObservedAt.Add(time.Nanosecond)
				need[7] = entry
			case "missing_wanted":
				need[11] = need[7]
			}
			data := diagnosticJournalBytes(t, rows)
			if variant == "newline" {
				data = data[:len(data)-1]
			}
			if variant == "unknown" {
				data = append([]byte(`{"unknown":true,`), data[1:]...)
			}
			if joined, err := studyDiagnosticReadJournal(bytes.NewReader(data), day, need, 7, end, count); err == nil || joined != nil {
				t.Fatal("incomplete/forged journal returned completed bindings")
			}
		})
	}
	reader := io.MultiReader(bytes.NewReader(diagnosticJournalBytes(t, attempts[:1])), iotest.ErrReader(io.ErrUnexpectedEOF))
	if joined, err := studyDiagnosticReadJournal(reader, day, wanted, 7, 10, 3); !errors.Is(err, io.ErrUnexpectedEOF) || joined != nil {
		t.Fatal("stream read failure lost")
	}
}

func TestDiagnosticMappingCloseBeforeJoinAndFinalization(t *testing.T) {
	for _, variant := range []string{"unclosed", "sync_close", "missing_file_row", "record_gap", "record_after_close"} {
		t.Run(variant, func(t *testing.T) {
			day, attempts, wanted := diagnosticAttemptFixture()
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			syncErr, closeErr := errors.New("journal sync"), errors.New("journal close")
			if variant == "sync_close" {
				ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
					file, err := open(path, flags, mode)
					return &diagnosticArtifactFaultSink{studyAuditSink: file, syncErr: syncErr, closeErr: closeErr}, err
				}
			}
			owner := diagnosticArtifactOwner(t, 1<<20, 128, ops)
			journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, 7)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if !journal.finalized {
					if err := journal.Close(); err != nil && variant != "sync_close" {
						t.Error(err)
					}
				}
			})
			limit := len(attempts)
			if variant == "record_gap" {
				limit = 1
			}
			for _, attempt := range attempts[:limit] {
				if err := journal.Record(attempt); err != nil {
					t.Fatal(err)
				}
			}
			switch variant {
			case "unclosed":
				if _, err := journal.Join(wanted, 10, 3); err == nil {
					t.Fatal("join opened a live journal")
				}
			case "sync_close":
				if err := journal.Close(); !errors.Is(err, syncErr) || !errors.Is(err, closeErr) {
					t.Fatal("journal finalization errors lost:", err)
				}
				if _, err := journal.Join(wanted, 10, 3); err == nil {
					t.Fatal("failed journal finalized into join")
				}
			case "missing_file_row":
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(owner.root, "attempts.jsonl"), diagnosticJournalBytes(t, attempts[:2]), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := journal.Join(studyDiagnosticWanted{}, 10, 3); err == nil {
					t.Fatal("empty wanted map masked incomplete full journal")
				}
			case "record_gap":
				attempt := attempts[0]
				attempt.ID = 11
				if err := journal.Record(attempt); err == nil {
					t.Fatal("actual attempt count/gap accepted")
				}
			case "record_after_close":
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
				if err := journal.Record(attempts[0]); err == nil {
					t.Fatal("record after Close accepted")
				}
			}
			if owner.Err() == nil {
				t.Fatal("mapping failure did not latch")
			}
			if err := owner.atomicJSON("COMPLETE.json", true); err == nil {
				t.Fatal("mapping failure permitted completion")
			}
		})
	}
}

func TestDiagnosticMappingWantedBoundsAndDecoderKey(t *testing.T) {
	var wanted studyDiagnosticWanted
	if err := wanted.addEvaluate(&correctionflow.TemporalEvaluateDiagnostics{}); err != nil {
		t.Fatal("missing-request empty window invented a key:", err)
	}
	key := correctionflow.DiagnosticKey{Band: "20m", Mode: "CW", Bucket: 14020, ToleranceHz: 100}
	at := time.Date(2026, 11, 15, 0, 0, 0, 123456789, time.UTC)
	for base := uint64(1); base <= 16384; base += 32 {
		capture := &correctionflow.TemporalEvaluateDiagnostics{Key: key}
		for offset := uint64(0); offset < 32; offset++ {
			capture.Window = append(capture.Window, correctionflow.DiagnosticTemporalEvent{ID: base + offset, ObservedAt: at.Add(time.Duration(base + offset)), Selection: correctionflow.DiagnosticSelection{Snapshot: correctionflow.DiagnosticSnapshot{Key: correctionflow.DiagnosticKey{Band: "foreign", Mode: "missing"}}}})
		}
		if err := wanted.addEvaluate(capture); err != nil {
			t.Fatal("exact wanted bound refused:", err)
		}
	}
	if len(wanted) != 16384 || wanted[1].Key != key {
		t.Fatal("wanted join used snapshot provenance instead of actual Evaluate.Key")
	}
	if err := wanted.addEvaluate(&correctionflow.TemporalEvaluateDiagnostics{Key: key, Window: []correctionflow.DiagnosticTemporalEvent{{ID: 16385, ObservedAt: at}}}); err == nil || len(wanted) != 16384 {
		t.Fatal("16385th wanted ID accepted")
	}
	conflict := key
	conflict.Bucket++
	if err := wanted.addEvaluate(&correctionflow.TemporalEvaluateDiagnostics{Key: conflict, Window: []correctionflow.DiagnosticTemporalEvent{{ID: 1, ObservedAt: wanted[1].ObservedAt}}}); err == nil {
		t.Fatal("conflicting repeated wanted key accepted")
	}
}

func TestDiagnosticMappingFocalAndIncreasingOrdinals(t *testing.T) {
	for _, variant := range []string{"repeat", "regress", "B", "beacon", "CQ_beacon"} {
		t.Run(variant, func(t *testing.T) {
			day, attempts, _ := diagnosticAttemptFixture()
			rows := append([]studyDiagnosticAttempt(nil), attempts[:2]...)
			switch variant {
			case "repeat":
				rows[1].Binding = rows[0].Binding
			case "regress":
				rows[0].Binding, rows[1].Binding = rows[1].Binding, rows[0].Binding
			case "B":
				day.Rows[0].A = false
			case "beacon":
				day.Rows[0].Row.SpotClass = rbnfeed.SpotClassBeacon
				rows[0].Binding.RawSHA256 = hashAnyStable(day.Rows[0].Row)
			case "CQ_beacon":
				day.Rows[0].Row.DXCall = "K1ABC/B"
				rows[0].Binding.RawSHA256 = hashAnyStable(day.Rows[0].Row)
			}
			for _, attempt := range rows {
				if attempt.Binding.RawSHA256 != hashAnyStable(day.Rows[attempt.Binding.Ordinal-1].Row) {
					t.Fatal("focal/ordinal refusal fixture has an unrelated row hash mismatch")
				}
			}
			if joined, err := studyDiagnosticReadJournal(bytes.NewReader(diagnosticJournalBytes(t, rows)), day, studyDiagnosticWanted{}, 7, 9, 2); err == nil || joined != nil {
				t.Fatal("nonfocal or repeated/regressing ordinal accepted by full reader")
			}
			owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
			journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, 7)
			if err != nil {
				t.Fatal(err)
			}
			var recordErr error
			for _, attempt := range rows {
				recordErr = journal.Record(attempt)
				if recordErr != nil {
					break
				}
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			if recordErr == nil || owner.Err() == nil {
				t.Fatal("nonfocal or repeated/regressing ordinal was written as a valid attempt")
			}
			data, err := os.ReadFile(filepath.Join(owner.root, "attempts.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			written := 0
			if variant == "repeat" || variant == "regress" {
				written = 1
			}
			if bytes.Count(data, []byte{'\n'}) != written {
				t.Fatal("invalid focal/ordinal attempt reached the writer before refusal")
			}
		})
	}
	day, attempts, _ := diagnosticAttemptFixture()
	rows := []studyDiagnosticAttempt{attempts[0], attempts[2]}
	rows[1].ID = 8
	wanted := studyDiagnosticWanted{7: {Key: rows[0].Key, ObservedAt: rows[0].ObservedAt}, 8: {Key: rows[1].Key, ObservedAt: rows[1].ObservedAt}}
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, 7)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range rows {
		if err := journal.Record(attempt); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	joined, err := journal.Join(wanted, 9, 2)
	if err != nil || len(joined) != 2 || joined[0].Binding.Ordinal != 1 || joined[1].Binding.Ordinal != 3 {
		t.Fatal("valid strictly increasing raw ordinal gaps refused:", err)
	}
}

func TestDiagnosticMappingNearUint64CounterOverflow(t *testing.T) {
	maximum := ^uint64(0)
	for _, fixture := range []struct {
		start uint64
		valid bool
	}{{maximum - studyMaxRows, true}, {maximum - studyMaxRows + 1, false}, {maximum, false}} {
		day, attempts, _ := diagnosticAttemptFixture()
		owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
		journal, err := newStudyDiagnosticJournal(owner, "attempts.jsonl", day, fixture.start)
		if !fixture.valid {
			if err == nil || journal != nil || owner.Err() == nil {
				t.Fatal("next-ID potential overflow constructed a journal")
			}
			if _, err := os.Stat(filepath.Join(owner.root, "attempts.jsonl")); !os.IsNotExist(err) {
				t.Fatal("overflow refusal created a journal file")
			}
			continue
		}
		if err != nil {
			t.Fatal("exact protected next-ID boundary refused:", err)
		}
		attempt := attempts[0]
		attempt.ID = fixture.start
		if err := journal.Record(attempt); err != nil {
			t.Fatal(err)
		}
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
		wanted := studyDiagnosticWanted{attempt.ID: {Key: attempt.Key, ObservedAt: attempt.ObservedAt}}
		joined, err := journal.Join(wanted, fixture.start+1, 1)
		if err != nil || len(joined) != 1 || joined[0].ID != fixture.start {
			t.Fatal("near-MaxUint64 actual ID lost precision or refused:", err)
		}
	}
	day, attempts, _ := diagnosticAttemptFixture()
	// This complete row would satisfy all later checks if the overflow guard
	// were absent; an empty reader would fail from missing rows either way.
	wrapped := attempts[0]
	wrapped.ID = maximum
	if joined, err := studyDiagnosticReadJournal(bytes.NewReader(diagnosticJournalBytes(t, []studyDiagnosticAttempt{wrapped})), day, studyDiagnosticWanted{}, maximum, 0, 1); err == nil || joined != nil {
		t.Fatal("actual next-ID wrap accepted")
	}
	attempt := attempts[0]
	attempt.ID = maximum - 1
	if _, err := studyDiagnosticReadJournal(bytes.NewReader(diagnosticJournalBytes(t, []studyDiagnosticAttempt{attempt})), day, studyDiagnosticWanted{}, maximum-1, maximum, 1); err != nil {
		t.Fatal("nonwrapping final next-ID MaxUint64 refused:", err)
	}
}
