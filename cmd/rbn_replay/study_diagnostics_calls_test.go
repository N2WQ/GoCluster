package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
)

func diagnosticAttachReplay(t *testing.T, r *replayRunner, day *studyRawDay, ops *studyDiagnosticArtifactOps) *studyDiagnosticRun {
	t.Helper()
	out := t.TempDir()
	prepared := diagnosticSelectionForDay(t, day)
	prepared.artifactOps = ops
	d, err := newStudyDiagnosticRun(out, prepared)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var observerErr error
		if r.study != nil {
			observerErr = r.study.close()
		}
		if err := errors.Join(observerErr, r.closeWithError(), d.closeStreams()); err != nil {
			t.Error(err)
		}
	})
	r.outDir, r.archiveDir, r.diagnostics = d.owner.root, d.owner.root, d
	r.dayStart, r.dayEnd = day.Start, day.End
	r.nextSampleAt = day.End.Add(time.Hour)
	r.ctyDB = studyTestCTY(t, studyTestCTYPoints())
	if err := d.bindDay(studyInput{DateUTC: day.Start.Format("2006-01-02"), Archive: studyFile{Path: "synthetic-unused.zip", SHA256: day.InputSHA256}}, day, r.temporalNextID); err != nil {
		t.Fatal(err)
	}
	r.study, err = newStudyObserver(day, r, filepath.Join(d.owner.root, "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func diagnosticCallDay(at time.Time, count int) *studyRawDay {
	rows := make([]studyRaw, count)
	for i := range rows {
		rows[i] = studyTestRaw(at.Add(time.Duration(i)*100*time.Millisecond), []string{"N0BBB", "N0CCC", "N0DDD"}[i%3], []string{"N0BBB", "N0CCC", "N0DDD"}[i%3], "K1ABE", true)
	}
	return studyTestReferenceDay(rows)
}

func diagnosticExecutionRows(t *testing.T, d *studyDiagnosticRun) []studyDiagnosticExecutionRecord {
	t.Helper()
	if err := d.executionFile.Sync(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(d.owner.root, "diagnostic-execution.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []studyDiagnosticExecutionRecord
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row studyDiagnosticExecutionRecord
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestDiagnosticCallsTemporalApplyDeferAndEOF(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	for _, scenario := range []string{"lag_apply", "defer_max_wait", "forced_eof", "overflow"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := replayTemporalFreshConfig()
			if scenario == "defer_max_wait" || scenario == "forced_eof" {
				cfg.TemporalDecoder.MaxWaitSeconds = 3
				cfg.TemporalDecoder.MinScore = 1 << 30
			}
			count := 1
			if scenario == "overflow" {
				cfg.TemporalDecoder.MaxPending = 1
				count = 2
			}
			r, _ := replayTemporalFreshRunner(t, cfg, at.Add(-time.Second), time.Minute)
			day := diagnosticCallDay(at, count)
			d := diagnosticAttachReplay(t, r, day, nil)
			for _, raw := range day.Rows {
				if err := r.processCSVRow(raw.Row, raw.Valid); err != nil {
					t.Fatal(err)
				}
			}
			if d.attempts != uint64(count) {
				t.Fatal("actual Observe attempts including rejected overflow were omitted")
			}
			if scenario == "defer_max_wait" {
				if err := r.advanceReplayTime(at.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if len(r.temporalPending) != 1 || d.selected[d.prepared.Selection.Days[2].Selected[0].RawID].Calls.Evaluate != 1 {
					t.Fatal("actual lag Evaluate did not defer and remain pending")
				}
			}
			if scenario == "forced_eof" {
				r.drainTemporal(at.Add(time.Second), true)
			} else {
				if err := r.advanceReplayTime(at.Add(4 * time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.diagnosticBoundary(); err != nil {
				t.Fatal(err)
			}
			if len(r.temporalPending) != 0 || r.temporalDecoder.PendingCount() != 0 || len(r.study.pending) != 0 || r.study.terminal != count {
				t.Fatal("actual temporal terminal ownership did not retire")
			}
			rows := diagnosticExecutionRows(t, d)
			if len(rows) != count {
				t.Fatal("terminal execution rows missing")
			}
			for _, row := range rows {
				if row.Binding.Ordinal < 1 || row.Calls.InitialSelector != 1 || row.Calls.Release != 1 || !row.Execution.FinalLicenseChecked || !row.Execution.OuterLearningHelperReached {
					t.Fatal("actual release/execution facts missing:", row)
				}
			}
			if scenario == "lag_apply" && !rows[0].Execution.ConfirmChecked {
				t.Fatal("literal supported different-winner fixture never reached actual ConfirmCorrection")
			}
			if scenario == "defer_max_wait" && rows[0].Calls.Evaluate != 2 {
				t.Fatal("defer then max-wait call capture omitted")
			}
			if scenario == "overflow" {
				for _, row := range rows {
					if row.Binding.Ordinal == 2 && row.Calls.Evaluate != 0 {
						t.Fatal("rejected Observe overflow invented an Evaluate call")
					}
				}
			}
		})
	}
}

func TestDiagnosticCallsFailureFinishesStepAndDrainBatch(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	for _, scenario := range []string{"immediate", "held", "drain_batch"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := replayTemporalFreshConfig()
			count := 1
			if scenario == "immediate" {
				cfg.TemporalDecoder.Enabled = false
			}
			if scenario == "drain_batch" {
				count = 2
			}
			r, _ := replayTemporalFreshRunner(t, cfg, at.Add(-time.Second), time.Minute)
			if scenario == "immediate" {
				r.temporalDecoder = nil
			}
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			var traceSink *diagnosticArtifactFaultSink
			sentinel := errors.New("capture output failure")
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				if filepath.Base(path) == "diagnostic-trace.jsonl" {
					traceSink = &diagnosticArtifactFaultSink{studyAuditSink: file}
					return traceSink, err
				}
				return file, err
			}
			day := diagnosticCallDay(at, count)
			if scenario == "drain_batch" {
				day.Rows[1].Row.Time = at
			}
			d := diagnosticAttachReplay(t, r, day, &ops)
			before := r.resolver.MetricsSnapshot()
			if scenario != "drain_batch" {
				traceSink.writeErr = sentinel
			}
			for _, raw := range day.Rows {
				err := r.processCSVRow(raw.Row, raw.Valid)
				if scenario == "drain_batch" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, sentinel) {
					t.Fatal("capture failure did not reach the safe boundary:", err)
				}
			}
			after := r.resolver.MetricsSnapshot()
			if after.Accepted != before.Accepted+uint64(count) {
				t.Fatal("capture failure changed ordinary evidence admission")
			}
			if after.Processed != before.Processed+uint64(count) || after.QueueDepth != 0 {
				t.Fatal("capture failure bypassed existing post-observation Step")
			}
			if scenario == "held" {
				if len(r.temporalPending) != 1 || r.temporalDecoder.PendingCount() != 1 {
					t.Fatal("failed capture changed actual Observe/hold ownership")
				}
				return
			}
			if scenario == "drain_batch" {
				traceSink.writeErr = sentinel
				if err := r.advanceReplayTime(at.Add(2 * time.Second)); !errors.Is(err, sentinel) {
					t.Fatal("batch failure not returned after drain:", err)
				}
			}
			if len(r.temporalPending) != 0 || len(r.study.pending) != 0 || r.study.terminal != count {
				t.Fatal("capture error interrupted ordinary terminal retirement")
			}
			if err := d.owner.atomicJSON("complete.json", true); err == nil {
				t.Fatal("failed actual capture allowed COMPLETE")
			}
		})
	}
}

func TestDiagnosticCallsSampleAppendAndDetailed64Bound(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	r, _ := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at.Add(-time.Second), time.Minute)
	day := diagnosticCallDay(at, 1)
	d := diagnosticAttachReplay(t, r, day, nil)
	s := newReplaySpot(day.Rows[0].Row)
	if err := r.study.begin(s, 1); err != nil {
		t.Fatal(err)
	}
	pending := r.study.pending[s]
	binding := d.binding(pending)
	for range 63 {
		d.capture(binding, "pipeline_selection", &correctionflow.ResolverSelectionDiagnostics{}, nil)
	}
	if err := d.owner.Err(); err != nil {
		t.Fatal(err)
	}
	pending.Status, pending.FinalObserved, pending.FinalIdentity = "output", s.DXCall, s.DXCall
	d.terminal(pending, s, replayResolverApplyOutcome{}, &studyDiagnosticExecution{})
	if state := d.selected[binding.RawID]; state.TraceCaptures+state.Terminal != 64 {
		t.Fatal("63 traces plus terminal did not retain exact64 detailed records")
	}
	if rows := diagnosticExecutionRows(t, d); len(rows) != 1 || rows[0].Sequence != 64 {
		t.Fatal("terminal did not use reserved64th record")
	}
	d.terminal(pending, s, replayResolverApplyOutcome{}, &studyDiagnosticExecution{})
	if d.owner.Err() == nil {
		t.Fatal("duplicate terminal detailed logical record accepted")
	}
	// A separate actual sample writes through the guarded runbook, then appends
	// its ordinary metrics even when those ignored Fprintf writes have failed.
	r2, _ := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at.Add(-time.Second), time.Minute)
	day2 := diagnosticCallDay(at, 1)
	d2 := diagnosticAttachReplay(t, r2, day2, nil)
	file, err := r2.openReplayArtifact(filepath.Join(r2.outDir, "sample.log"), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	r2.runbookSamplesFile = file
	sentinel := errors.New("sample write")
	d2.latch(sentinel)
	before := len(r2.samples)
	if err := r2.emitSample(at); !errors.Is(err, sentinel) || len(r2.samples) != before+1 {
		t.Fatal("sample failure returned before actual append:", err)
	}
}

func TestDiagnosticCallsCollectorReservesTerminal64(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	r, _ := replayTemporalFreshRunner(t, replayTemporalFreshConfig(), at.Add(-time.Second), time.Minute)
	day := diagnosticCallDay(at, 1)
	d := diagnosticAttachReplay(t, r, day, nil)
	s := newReplaySpot(day.Rows[0].Row)
	if err := r.study.begin(s, 1); err != nil {
		t.Fatal(err)
	}
	binding := d.binding(r.study.pending[s])
	for range 63 {
		d.capture(binding, "pipeline_selection", &correctionflow.ResolverSelectionDiagnostics{}, nil)
	}
	if err := d.owner.Err(); err != nil {
		t.Fatal(err)
	}
	if d.trace.counts[binding.RawID] != 63 || d.selected[binding.RawID].TraceCaptures != 63 {
		t.Fatal("63 preterminal trace captures were not admitted")
	}
	d.capture(binding, "pipeline_selection", &correctionflow.ResolverSelectionDiagnostics{}, nil)
	if d.owner.Err() == nil || d.trace.counts[binding.RawID] != 63 || d.selected[binding.RawID].TraceCaptures != 63 || d.selected[binding.RawID].Terminal != 0 {
		t.Fatal("64th preterminal trace reached the independent trace writer instead of reserving the terminal record")
	}
}
