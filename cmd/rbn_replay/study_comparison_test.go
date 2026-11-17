package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func studyTestComparisonReports(t *testing.T, p studyPlan, digest string, badSecondCell, spillover bool) []string {
	t.Helper()
	return studyTestWriteReports(t, p, digest, func(in studyInput, arm studyArm) []studyDecision {
		date, _, err := parseUTCDate(in.DateUTC)
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]studyDecision, 0, 100)
		for i := 1; i <= 100; i++ {
			rawID := fmt.Sprintf("%s:%d", in.Archive.SHA256, i)
			d := studyTestDecision(rawID, rawID, arm.Name == "repaired_on", "K1ABC")
			d.Raw.Time, d.DecisionAtUTC = date.Add(time.Duration(300+i*196)*time.Second), date.Add(time.Duration(300+i*196)*time.Second)
			d.Ordinal, d.ReceiverFamily = int64(i), "N0BBB"
			d.RawSHA256 = hashAnyStable(d.Raw)
			if spillover && i == 1 {
				d.R1.Identity = "K9BAD"
				if arm.Name == "repaired_on" {
					d.ConfiguredDistance = 3
				}
			}
			if badSecondCell && i == 1 && arm.Name == "repaired_on" {
				d.ConfiguredDistance = 2
			}
			rows = append(rows, *d)
		}
		return rows
	})
}

func TestCCV2StudyWholeCombinationQualification(t *testing.T) {
	for _, variant := range []string{"one_cell_pass", "second_cell_fails", "outside_cell_harm"} {
		t.Run(variant, func(t *testing.T) {
			p := studyTestPlan(t)
			p.Candidates[0].ModeDistances = map[string][]int{"CW": {1}}
			if variant == "second_cell_fails" {
				p.Candidates[0].ModeDistances["CW"] = []int{1, 2}
			}
			for i := range p.Inputs {
				p.Inputs[i].MaxRows = 100
			}
			const digest = "hand-literal-comparison-plan"
			paths := studyTestComparisonReports(t, p, digest, variant == "second_cell_fails", variant == "outside_cell_harm")
			out := filepath.Join(t.TempDir(), "comparison")
			err := compareCCV2Study(p, digest, studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout", Output: out})
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(out, "comparison.json"))
			if err != nil {
				t.Fatal(err)
			}
			var got studyComparison
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if got.RawUnion != 200 || got.AllActiveCellsPass != (variant == "one_cell_pass") {
				t.Fatalf("whole-combination gate changed: %+v", got)
			}
			if variant != "one_cell_pass" && got.Verdict != "whole_bayes_off" {
				t.Fatalf("selected winning cells instead of whole off: %+v", got)
			}
			if variant == "outside_cell_harm" && (len(got.Spillover) != 2 || got.NewStrongContradictionsR1 != 2) {
				t.Fatalf("outside-mask stateful harm was hidden: %+v", got)
			}
		})
	}
}

func TestCCV2StudyRealRunnerAndWithheldReceivers(t *testing.T) {
	archive := studyTestZIP(t, "N0AAA,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\nN0EEE,14020,20m,K1ABC,10,2026-11-15 00:10:01,CQ,CW\nN0KKK,14020,20m,K1ABC,20,2026-11-15 00:10:02,CQ,CW\nN0BBB,14020,20m,K1ABD,-7,2026-11-15 00:10:03,CQ,CW\n")
	in := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 4, MaxCSVBytes: 4096}
	day, err := loadStudyRawDay(in, nil, 300)
	if err != nil {
		t.Fatal(err)
	}
	at := day.Rows[3].Row.Time
	r, store := replayLegacyClockRunner(t, at)
	observer, err := newStudyObserver(day, r, filepath.Join(t.TempDir(), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	r.study = observer
	t.Cleanup(func() { _ = observer.close() })
	for _, raw := range day.Rows {
		if err := r.processCSVRow(raw.Row, raw.Valid); err != nil {
			t.Fatal(err)
		}
	}
	if err := observer.complete(); err != nil {
		t.Fatal(err)
	}
	if got := r.resolver.MetricsSnapshot().Accepted; got != 1 || observer.terminal != 1 || observer.expected != 1 {
		t.Fatalf("withheld receivers reached predictor: accepted=%d terminal=%d expected=%d", got, observer.terminal, observer.expected)
	}
	if store.HasRecentSupport("K1ABC", "20m", "CW", 1, at) {
		t.Fatal("withheld raw rows seeded prediction history")
	}
	// Exercise orchestration and cleanup with the same parser/runtime owners;
	// no downloader, fabricated license authority or alternate predictor exists.
	out := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
	p := studyTestPlan(t)
	arm := p.Arms[0]
	if err := studyRunDay(p, "test-plan", studyCLIRequest{Output: out, Phase: "tuning"}, arm, p.Candidates[0], in, cfg, day, false); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "complete.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report studyReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Complete || report.RawRows != 4 || report.TerminalRows != 1 || report.FocalRows != 1 || report.PendingHighWater > report.PendingBound {
		t.Fatalf("usable runner produced partial success: %+v", report)
	}
	if _, err := os.Stat(filepath.Join(out, "FAILED.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful fixture left failure marker: %v", err)
	}
}
