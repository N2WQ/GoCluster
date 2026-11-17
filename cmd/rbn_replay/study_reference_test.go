package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	rbnfeed "dxcluster/rbn"
	"dxcluster/stats"
)

func studyTestZIP(t *testing.T, body string) studyFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "20261115.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	z := zip.NewWriter(f)
	w, err := z.Create("20261115.csv")
	if err == nil {
		_, err = w.Write([]byte("callsign,freq,band,dx,db,date,mode,tx_mode\n" + body))
	}
	if err := errors.Join(err, z.Close(), f.Close()); err != nil {
		t.Fatal(err)
	}
	sum, err := studyHashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return studyFile{Path: path, SHA256: sum}
}

func studyTestRaw(at time.Time, receiver, family, call string, a bool) studyRaw {
	return studyRaw{Valid: true, Family: family, A: a,
		Row: rbnHistoryRow{Time: at, FreqKHz: 14020, Band: "20m", DXCall: call,
			Spotter: receiver, Mode: "CW", SpotClass: rbnfeed.SpotClassCQ, ReportDB: 12}}
}

func studyTestReferenceDay(rows []studyRaw) *studyRawDay {
	start := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	d := &studyRawDay{Rows: rows, Start: start, End: start.Add(24 * time.Hour), First: start,
		Last: start.Add(24*time.Hour - time.Second), WarmupSeconds: 300, Index: make(map[string][]int), InputSHA256: strings.Repeat("a", 64)}
	for i := range d.Rows {
		d.Rows[i].Ordinal = int64(i + 1)
		if studyReferenceRow(d.Rows[i].Row) {
			key := d.Rows[i].Row.Band + "\x00" + d.Rows[i].Row.Mode
			d.Index[key] = append(d.Index[key], i)
		}
	}
	return d
}

func TestCCV2StudyInputAndReferenceIsolation(t *testing.T) {
	t.Run("snr_binding", func(t *testing.T) {
		archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,-7,2026-11-15 00:10:00,CQ,CW\nN0CCC,14020,20m,K1ABD,32,2026-11-15 00:10:00,CQ,RTTY\n")
		input := studyInput{DateUTC: "2026-11-15", Archive: archive, MaxRows: 2, MaxCSVBytes: 4096}
		day, err := loadStudyRawDay(input, nil, 300)
		if err != nil {
			t.Fatal(err)
		}
		if len(day.Rows) != 2 || day.Rows[0].Row.ReportDB != -7 || day.Rows[1].Row.ReportDB != 32 ||
			day.Rows[0].Row.Mode != "CW" || day.Rows[1].Row.Mode != "RTTY" || day.Rows[0].Row.SpotClass != rbnfeed.SpotClassCQ {
			t.Fatalf("actual CSV tx_mode/SNR association changed: %+v", day.Rows)
		}
		r := newReplayRunner(replayRequest{})
		r.cfg, r.tracker = &config.Config{}, stats.NewTracker()
		observer, err := newStudyObserver(day, r, filepath.Join(t.TempDir(), "decisions.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = observer.close() })
		if err := observer.checkRaw(day.Rows[0].Row, true, 1); err != nil {
			t.Fatal(err)
		}
		wrong := day.Rows[1].Row
		wrong.ReportDB = -7
		if err := observer.checkRaw(wrong, true, 2); err == nil {
			t.Fatal("receiver SNR moved between equal-time rows without refusal")
		}
		if !studyCohortA("N0BBB") || studyCohortA("N0AAA") || studyCohortA("N0EEE") || studyCohortA("N0KKK") {
			t.Fatal("seeded cohort golden values changed (independent SHA256 literals)")
		}
		for _, alias := range []string{"N0BBB-2", "N0BBB/P", "N0BBB/MM"} {
			if day.family(alias) != "N0BBB" || !observer.predictor(alias) {
				t.Fatalf("receiver alias escaped conservative family: %s => %s", alias, day.family(alias))
			}
		}
	})
	t.Run("reference_cutoffs", func(t *testing.T) {
		at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
		focal := studyTestRaw(at, "N0BBB", "N0BBB", "K1ABD", true)
		rows := []studyRaw{
			studyTestRaw(at.Add(-31*time.Second), "N0MMM", "N0MMM", "K9BAD", false),
			studyTestRaw(at.Add(-30*time.Second), "N0AAA", "N0AAA", "K1ABC", false),
			studyTestRaw(at.Add(-time.Second), "N0EEE", "N0EEE", "K1ABC", false),
			studyTestRaw(at, "N0KKK", "N0KKK", "K1ABC", false),
			focal,
			studyTestRaw(at, "N0CCC", "N0CCC", "K9BAD", true),
			studyTestRaw(at.Add(80*time.Second), "N0MMM", "N0MMM", "K9BAD", false),
			studyTestRaw(at.Add(81*time.Second), "N0CCC", "N0CCC", "K1ABC", true),
			studyTestRaw(at.Add(90*time.Second), "N0BBB/P", "N0BBB", "K9BAD", true),
			studyTestRaw(at.Add(100*time.Second), "N0DDD", "N0DDD", "K1ABC", true),
			studyTestRaw(at.Add(195*time.Second), "N0FFF", "N0FFF", "K1ABC", true),
			studyTestRaw(at.Add(196*time.Second), "N0MMM", "N0MMM", "K9BAD", false),
		}
		rows[1].Row.FreqKHz = 14020.100 // Inclusive 100 Hz core boundary.
		day := studyTestReferenceDay(rows)
		for _, later := range []bool{false, true} {
			got := day.reference(focal, later)
			if !got.Strong || got.Identity != "K1ABC" || got.Families != 3 {
				t.Fatalf("later=%v reference=%+v, want literal K1ABC/3", later, got)
			}
		}
		// A-only raw evidence cannot leak into R1. Contradictions among future
		// families make R2 unknown without consulting a proposed model winner.
		day.Rows[9].Row.DXCall = "K9BAD"
		if got := day.reference(focal, true); got.Strong || got.Reason != "reference_conflict" {
			t.Fatalf("future disagreement became a strong winner: %+v", got)
		}
		if got := day.reference(focal, false); !got.Strong || got.Identity != "K1ABC" {
			t.Fatalf("changing future rows changed causal R1: %+v", got)
		}
		focal.Row.Time = day.Start.Add(299 * time.Second)
		if got := day.reference(focal, false); got.Strong || got.Reason != "unavailable_context" {
			t.Fatalf("incomplete warmup escaped denominator unknown: %+v", got)
		}
	})
}

func TestCCV2StudyReferenceUnknowns(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	focal := studyTestRaw(at, "N0BBB", "N0BBB", "K1ABD", true)
	base := []studyRaw{
		studyTestRaw(at.Add(-3*time.Second), "N0AAA", "N0AAA", "K1ABC", false),
		studyTestRaw(at.Add(-2*time.Second), "N0EEE", "N0EEE", "K1ABC", false),
		studyTestRaw(at.Add(-time.Second), "N0KKK", "N0KKK", "K1ABC", false),
	}
	tests := []struct {
		name, reason string
		extra        []studyRaw
	}{
		{"conflicting_family", "receiver_conflict", []studyRaw{studyTestRaw(at, "N0AAA-2", "N0AAA", "K9BAD", false)}},
		{"other_call", "reference_conflict", []studyRaw{studyTestRaw(at, "N0MMM", "N0MMM", "K9BAD", false)}},
		{"slash_is_not_bare", "reference_conflict", []studyRaw{studyTestRaw(at, "N0MMM", "N0MMM", "K1ABC/P", false)}},
		{"guard", "cochannel", []studyRaw{studyTestRaw(at, "N0MMM", "N0MMM", "K9BAD", false), studyTestRaw(at, "N0OOO", "N0OOO", "K9BAD", false)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			extra := append([]studyRaw(nil), tc.extra...)
			if tc.name == "guard" {
				for i := range extra {
					extra[i].Row.FreqKHz = 14020.500
				}
			}
			day := studyTestReferenceDay(append(append([]studyRaw(nil), base...), extra...))
			got := day.reference(focal, false)
			if got.Strong || got.Reason != tc.reason {
				t.Fatalf("reference %+v, want unknown/%s", got, tc.reason)
			}
		})
	}
	day := studyTestReferenceDay(base[:2])
	if got := day.reference(focal, false); got.Strong || got.Families != 2 || got.Reason != "insufficient_families" {
		t.Fatalf("two families qualified: %+v", got)
	}
	day = studyTestReferenceDay(base)
	day.Last = at.Add(194 * time.Second)
	if got := day.reference(focal, true); got.Strong || got.Reason != "boundary" {
		t.Fatalf("truncated future was treated as silence/correct: %+v", got)
	}
}

func TestCCV2StudyFrozenEpisodeAnchors(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	rows := []studyRaw{
		studyTestRaw(at, "N0BBB", "N0BBB", "K1ABC", true),
		studyTestRaw(at, "N0CCC", "N0CCC", "K9BAD", true),
		studyTestRaw(at.Add(195*time.Second), "N0DDD", "N0DDD", "K2XYZ", true),
		studyTestRaw(at.Add(196*time.Second), "N0FFF", "N0FFF", "K2XYZ", true),
	}
	rows[1].Row.FreqKHz = 14020.100
	rows[2].Row.FreqKHz = 14020.100
	rows[3].Row.FreqKHz = 14020.200
	day := studyTestReferenceDay(rows)
	day.assignEpisodes()
	if day.Rows[0].Episode != day.Rows[1].Episode || day.Rows[0].Episode != day.Rows[2].Episode || day.Rows[3].Episode == day.Rows[0].Episode {
		t.Fatalf("episode chained or lost inclusive bounds: %+v", day.Rows)
	}
	if !strings.HasSuffix(day.Rows[0].Episode, ":1") {
		t.Fatalf("equal-time anchor did not use original ordinal: %s", day.Rows[0].Episode)
	}
}

func studyTestParameters(t *testing.T) json.RawMessage {
	t.Helper()
	// This is schema scaffolding, not a model arithmetic oracle. Semantic
	// expectations in the reference and metric tests are hand-literal.
	value := studyTestBayesBaseline()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "EnabledDistancesByMode")
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func studyTestBayesBaseline() config.CallCorrectionBayesBonusConfig {
	return config.CallCorrectionBayesBonusConfig{
		Enabled: true, WeightDistance1Milli: 350, WeightDistance2Milli: 200,
		WeightedSmoothingMilli: 1000, RecentSmoothing: 2,
		ObsLogCapMilli: 350, PriorLogMinMilli: -200, PriorLogMaxMilli: 600,
		ReportThresholdDistance1Milli: 450, ReportThresholdDistance2Milli: 650,
		AdvantageThresholdDistance1Milli: 700, AdvantageThresholdDistance2Milli: 950,
		AdvantageMinWeightedDeltaDistance1Milli: 200, AdvantageMinWeightedDeltaDistance2Milli: 300,
		AdvantageExtraConfidenceDistance1: 3, AdvantageExtraConfidenceDistance2: 5,
		RequireCandidateValidated: true, RequireSubjectUnvalidatedDistance2: true,
	}
}
