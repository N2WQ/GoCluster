package main

import (
	"encoding/json"
	"io"
	"testing"
)

func studyTestSpilloverPair(id, episode string, offCorrection, onCorrection bool, distance int, reference string) *studyPair {
	off := studyTestDecision(id, episode, offCorrection, reference)
	on := studyTestDecision(id, episode, onCorrection, reference)
	if offCorrection {
		off.ConfiguredDistance = distance
	}
	if onCorrection {
		on.ConfiguredDistance = distance
	}
	return &studyPair{Arms: [3]*studyDecision{off, off, on}}
}

func TestCCV2StudySpilloverFullEpisodeSupport(t *testing.T) {
	for _, scenario := range []string{"shared_receipt_before", "shared_receipt_after", "already_delivered_contradiction", "incremental_unknown"} {
		t.Run(scenario, func(t *testing.T) {
			const date = "2026-11-15"
			shared := studyTestSpilloverPair("shared", "affected", true, true, 2, "K1ABC")
			changed := studyTestSpilloverPair("changed", "affected", false, true, 2, "K1ABC")
			wantSpillover := studyCellDate{DateUTC: date}
			switch scenario {
			case "already_delivered_contradiction":
				// OFF already delivered X on the shared row. Renaming another
				// raw row to X against its strong labels must still count harm.
				changed = studyTestSpilloverPair("changed", "affected", false, true, 2, "K9BAD")
				wantSpillover.NewWrongR1, wantSpillover.NewWrongR2 = 1, 1
			case "incremental_unknown":
				// A genuinely new Z target remains incremental; its unknown R2
				// cannot borrow coverage from the shared X receipt's strong R2.
				changed = studyTestSpilloverPair("changed", "affected", false, true, 2, "K1ABZ")
				changed.Arms[2].FinalIdentity, changed.Arms[2].FinalObserved = "K1ABZ", "K1ABZ"
				for _, d := range changed.Arms {
					d.R2, d.R2Frozen = studyReference{Reason: "boundary"}, studyReference{Reason: "boundary"}
				}
				wantSpillover.Incremental, wantSpillover.CoveredR1 = 1, 1
			}
			pairs := map[int64]*studyPair{1: shared, 2: changed,
				// OFF-only losses in unaffected episodes/cells must not widen
				// the population admitted by changed ON renames.
				3: studyTestSpilloverPair("unaffected", "unaffected", true, false, 2, "K1ABC"),
				4: studyTestSpilloverPair("other-cell", "affected", true, false, 3, "K1ABC"),
				5: studyTestSpilloverPair("active", "active", false, true, 1, "K1ABC")}
			if scenario == "shared_receipt_after" {
				pairs[1], pairs[2] = pairs[2], pairs[1]
			}
			candidate := studyCandidate{ModeDistances: map[string][]int{"CW": {1}}}
			result := studyComparison{BayesOutcomes: make(map[string]int64), RepairOutcomes: make(map[string]int64)}
			counts, err := studyAccountDay(studyInput{DateUTC: date}, pairs, candidate, &result, json.NewEncoder(io.Discard), 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Spillover) != 1 || result.Spillover[0].Cell != "CW/2" || result.Spillover[0].Counts != wantSpillover {
				t.Fatalf("full episode support must net 1→1=0 without changing admission/coverage/harm: got=%+v want=%+v", result.Spillover, wantSpillover)
			}
			wantActive := studyCellDate{DateUTC: date, Incremental: 1, Joint: 1, CoveredR1: 1, CoveredR2: 1, NetSupportedR1: 1, NetSupportedR2: 1}
			if len(counts) != 1 || counts["CW/1"] != wantActive || result.RawUnion != 5 || result.UnpairedRawIDs != 0 {
				t.Fatalf("spillover analysis changed active cell/full union: counts=%+v result=%+v", counts, result)
			}
			control := studyComparison{BayesOutcomes: make(map[string]int64), RepairOutcomes: make(map[string]int64)}
			controlCounts, err := studyAccountDay(studyInput{DateUTC: date}, map[int64]*studyPair{5: pairs[5]}, candidate, &control, json.NewEncoder(io.Discard), 3)
			if err != nil || controlCounts["CW/1"] != counts["CW/1"] || len(control.Spillover) != 0 {
				t.Fatalf("unaffected active accounting differs from isolated control: counts=%+v err=%v", controlCounts, err)
			}
		})
	}
}
