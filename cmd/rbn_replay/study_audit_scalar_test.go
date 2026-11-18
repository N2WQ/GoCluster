package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	stabilitypkg "dxcluster/internal/stability"
	rbnfeed "dxcluster/rbn"
)

func TestStudyAuditIndependentScalarVectors(t *testing.T) {
	start := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	config := replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500}
	type sight struct {
		seconds int
		freq    float64
	}
	for _, vector := range []struct {
		name   string
		rows   []sight
		stable bool
	}{
		{"inclusive_start_and_end", []sight{{0, 14020}, {1, 14020}, {900, 14020}}, true},
		{"start_before_one_second", []sight{{-1, 14020}, {1, 14020}, {900, 14020}}, false},
		{"end_after_one_second", []sight{{0, 14020}, {1, 14020}, {901, 14020}}, false},
		{"inclusive_both_frequency_edges", []sight{{0, 14019.5}, {1, 14020}, {900, 14020.5}}, true},
		{"low_edge_one_hz_outside", []sight{{0, 14019.499}, {1, 14020}, {900, 14020.5}}, false},
		{"high_edge_one_hz_outside", []sight{{0, 14019.5}, {1, 14020}, {900, 14020.501}}, false},
		{"same_second_three_rows", []sight{{0, 14020}, {0, 14020}, {0, 14020}}, true},
		{"two_rows", []sight{{0, 14020}, {900, 14020}}, false},
		{"none", nil, false},
	} {
		t.Run(vector.name, func(t *testing.T) {
			d := studyTestDecision("unused", "unused", true, "K1ABC")
			d.Raw.Time, d.DecisionAtUTC = start, start
			spots := make(map[string][]stabilitypkg.Spot)
			for _, s := range vector.rows {
				spots["K1ABC"] = append(spots["K1ABC"], stabilitypkg.Spot{Ts: start.Add(time.Duration(s.seconds) * time.Second).Unix(), Freq: s.freq})
			}
			if got := studyAuditStable(d, spots, config); got != vector.stable {
				t.Fatalf("literal boundary result=%t want=%t", got, vector.stable)
			}
			// Independently enter the existing public collector path. The golden
			// answer is the literal table, not either helper's computed answer.
			collector := newReplayStabilityCollector(config)
			for _, s := range vector.rows {
				collector.ObserveRaw(rbnHistoryRow{Time: start.Add(time.Duration(s.seconds) * time.Second), FreqKHz: s.freq, DXCall: "K1ABC"})
			}
			collector.ObserveApplied(start.Unix(), "K1ABC", 14020, "20m")
			if got := collector.Evaluate(start.Unix()).StableApplied; got != map[bool]int{false: 0, true: 1}[vector.stable] {
				t.Fatalf("public collector=%d", got)
			}
		})
	}
}

func TestStudyAuditRawPopulationLiteralCohortsAndBeaconClasses(t *testing.T) {
	f := studyAuditTestInputs(t)
	spots, accepted, bad, skipped, focal := studyAuditRawPopulation(f.Day)
	if accepted != 8 || bad != 0 || skipped != 0 || focal != 4 {
		t.Fatalf("population=%d,%d,%d,%d", accepted, bad, skipped, focal)
	}
	if got := spots["K1ABC"]; len(got) != 3 || got[0].Ts != f.Day.Rows[1].Row.Time.Unix() || got[2].Ts != f.Day.Rows[8].Row.Time.Unix() {
		t.Fatalf("BEACON/NCDXF B rows omitted or B cohort leaked: %+v", got)
	}
	if len(spots["K1ABC/B"]) != 1 {
		t.Fatal("CQ /B raw sighting was filtered before ObserveRaw")
	}
	var ids []int64
	for _, raw := range f.Day.Rows {
		if studyFocalRow(raw) {
			ids = append(ids, raw.Ordinal)
		}
	}
	if fmt.Sprint(ids) != "[1 6 7 8]" {
		t.Fatalf("focal literal IDs=%v", ids)
	}
}

func TestStudyAuditAliasesSSIDClocksAndDayEnd(t *testing.T) {
	metadata := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "alias.json"), []studyReceiverGroup{{Aliases: []string{"N0BBB", "N9ZZZ"}}})
	aliases, err := loadStudyReceiverAliases(metadata)
	if err != nil {
		t.Fatal(err)
	}
	day := &studyRawDay{Aliases: aliases}
	if day.family("N9ZZZ-7") != "N0BBB" || !studyCohortA(day.family("N9ZZZ-7")) {
		t.Fatal("frozen alias/SSID family was not used")
	}
	start := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	for _, second := range []int{0, 1, 900} {
		raw := studyTestRaw(start.Add(time.Duration(second)*time.Second), "N9ZZZ-7", "N0BBB", "K1ABC-7", true)
		raw.Row.Mode, raw.Row.Band = "RTTY", "40m" // scorer intentionally has no extra mode/band filter.
		raw.Row.SpotClass = rbnfeed.SpotClassBeacon
		day.Rows = append(day.Rows, raw)
	}
	spots, _, _, _, _ := studyAuditRawPopulation(day)
	d := studyTestDecision("unused", "unused", true, "K1ABC")
	d.Raw.Time, d.DecisionAtUTC = start, start
	d.FinalObserved = " k1abc/p "
	cfg := replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500}
	if !studyAuditStable(d, spots, cfg) {
		t.Fatal("normalization/repeated receiver/mode/band semantics changed")
	}
	d.FinalObserved = "K1ABC-7"
	if studyAuditStable(d, spots, cfg) {
		t.Fatal("winner accidentally used DX numeric-SSID normalization")
	}
	d.FinalObserved = "K1ABC"
	d.DecisionAtUTC = start.Add(time.Second)
	if studyAuditStable(d, spots, cfg) {
		t.Fatal("scorer used raw clock instead of actual decision clock")
	}
	d.Status = "suppressed"
	if studyAuditStable(d, spots, cfg) || studyAuditApplied(d) {
		t.Fatal("suppressed applied mutation became an output receipt")
	}
	d.Status = "output"
	d.DecisionAtUTC = start.Add(23*time.Hour + 49*time.Minute + 59*time.Second)
	end := d.DecisionAtUTC.Unix()
	spots = map[string][]stabilitypkg.Spot{"K1ABC": {{Ts: end, Freq: 14020}, {Ts: end, Freq: 14020}, {Ts: end, Freq: 14020}}}
	if !studyAuditStable(d, spots, cfg) {
		t.Fatal("day end incorrectly required a complete future horizon")
	}
	spots["K1ABC"] = spots["K1ABC"][:2]
	if studyAuditStable(d, spots, cfg) {
		t.Fatal("day-end scorer invented next-day support")
	}
}

func TestStudyAuditExactFractionsAndPairClasses(t *testing.T) {
	if got := studyAuditCompareFractions(studyAuditFraction{Stable: 1, Applied: 3}, studyAuditFraction{Stable: 2, Applied: 6}); got != "equal" {
		t.Fatal(got)
	}
	if got := studyAuditCompareFractions(studyAuditFraction{Stable: 1000000, Applied: 3000001}, studyAuditFraction{Stable: 1000001, Applied: 3000004}); got != "on_higher" {
		t.Fatalf("rounded percentage tie hid exact ordering: %s", got)
	}
	if got := studyAuditCompareFractions(studyAuditFraction{}, studyAuditFraction{Applied: 1}); got != "undefined" {
		t.Fatal(got)
	}
	if got := studyAuditCompareFractions(studyAuditFraction{Stable: 256000000, Applied: 256000000}, studyAuditFraction{Stable: 255999999, Applied: 256000000}); got != "off_higher" {
		t.Fatal("safe int64 pooled boundary:", got)
	}
	off := studyTestDecision("same", "episode", true, "K1ABC")
	on := *off
	on.FinalObserved, on.FinalIdentity = "K9ABC", "K9ABC"
	on.DecisionAtUTC = on.DecisionAtUTC.Add(time.Second)
	arms := [3]studyAuditReceipt{studyAuditMakeReceipt(off, false), studyAuditMakeReceipt(off, false), studyAuditMakeReceipt(&on, false)}
	arms[2].CurrentStable = true
	counts := studyAuditNewCounts()
	studyAuditAccount(&counts, arms)
	if counts.Common != 1 || counts.CommonRetargeted != 1 || counts.CommonRetimed != 1 || counts.CommonRetargetedRetimed != 1 || counts.Stable01 != 1 {
		t.Fatalf("retarget/retime pair=%+v", counts)
	}
	if counts.CommonSemanticChanged != 1 || counts.CommonLiteralChanged != 1 || counts.CommonScorerKeyChanged != 1 {
		t.Fatalf("winner change decomposition=%+v", counts)
	}
	if !counts.Arms[1].Defined || !counts.Arms[2].Defined {
		t.Fatal("defined denominator lost")
	}
	if empty := studyAuditNewCounts(); empty.Arms[1].Defined || empty.ExactRateComparison != "undefined" {
		t.Fatal("zero N was defined")
	}
}

func TestStudyAuditLiteralWinnerChangesAndOnlyFractions(t *testing.T) {
	d := studyTestDecision("same", "episode", true, "K1ABC")
	base := studyAuditMakeReceipt(d, false)
	for _, variant := range []struct {
		name                   string
		semantic, literal, key bool
	}{
		{"literal_same_key", false, true, false}, {"semantic_only", true, false, false}, {"scorer_only", false, false, true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			on := base
			if variant.semantic {
				on.FinalIdentity = "OTHER"
			}
			if variant.literal {
				on.FinalObserved = "k1abc/p"
			}
			if variant.key {
				on.WinnerKey = "OTHER"
			}
			counts := studyAuditNewCounts()
			studyAuditAccount(&counts, [3]studyAuditReceipt{base, base, on})
			if counts.CommonSame != 0 || counts.CommonRetargeted != 1 || (counts.CommonSemanticChanged == 1) != variant.semantic ||
				(counts.CommonLiteralChanged == 1) != variant.literal || (counts.CommonScorerKeyChanged == 1) != variant.key {
				t.Fatalf("winner change hidden: %+v", counts)
			}
		})
	}
	none := base
	none.ActualApplied = false
	none.Applied = false
	stable := base
	stable.CurrentStable = true
	counts := studyAuditNewCounts()
	studyAuditAccount(&counts, [3]studyAuditReceipt{none, none, stable})
	studyAuditAccount(&counts, [3]studyAuditReceipt{none, none, base})
	studyAuditAccount(&counts, [3]studyAuditReceipt{none, stable, none})
	if counts.ONOnlyFraction != (studyAuditFraction{Stable: 1, Applied: 2, Defined: true}) || counts.OFFOnlyFraction != (studyAuditFraction{Stable: 1, Applied: 1, Defined: true}) {
		t.Fatalf("only fractions=%+v/%+v", counts.ONOnlyFraction, counts.OFFOnlyFraction)
	}
	pooled := studyAuditNewCounts()
	studyAuditMergeCounts(&pooled, counts)
	if pooled.ONOnlyFraction != counts.ONOnlyFraction || pooled.OFFOnlyFraction != counts.OFFOnlyFraction {
		t.Fatal("pooling lost only fractions")
	}
	if empty := studyAuditNewCounts(); empty.ONOnlyFraction.Defined || empty.OFFOnlyFraction.Defined {
		t.Fatal("zero-only N defined")
	}
}

func TestStudyAuditCompactRowsAndPreallocationBounds(t *testing.T) {
	f := studyAuditTestInputs(t)
	loaded, err := studyAuditLoadGroup(f.Plan.Groups[0])
	if err != nil {
		t.Fatal(err)
	}
	pairs, err := studyReadDayPairs(f.Study.Inputs[0], loaded.Reports["2026-11-15"])
	if err != nil {
		t.Fatal(err)
	}
	d := pairs[1].Arms[0]
	row := studyAuditPairRecord{Group: 0, DateUTC: "2026-11-15", Phase: "holdout", RawID: d.RawID, RawSHA256: d.RawSHA256, Mode: d.Raw.Mode, SubjectIdentity: d.SubjectIdentity, Class: "common_same"}
	for i := range row.Arms {
		row.Arms[i] = studyAuditMakeReceipt(d, false)
	}
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 1500 || bytesContainAuditRaw(data) {
		t.Fatalf("full decisions leaked into compact union: bytes=%d", len(data))
	}
	if err := studyAuditEncodedBound(reflect.ValueOf(row), studyAuditRowBytes); err != nil {
		t.Fatal(err)
	}
	if err := studyAuditEncodedBound(reflect.ValueOf(strings.Repeat("x", studyAuditRowBytes)), studyAuditRowBytes); err == nil {
		t.Fatal("large row was not refused before allocation")
	}
	if err := studyAuditLabels(reflect.ValueOf(strings.Repeat("x", 1025))); err == nil {
		t.Fatal("large label accepted")
	}
}

func bytesContainAuditRaw(data []byte) bool {
	return strings.Contains(string(data), "receiver_location") || strings.Contains(string(data), "raw\":{") || strings.Contains(string(data), "r2_frozen")
}
