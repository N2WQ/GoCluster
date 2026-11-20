package main

import (
	"testing"
	"time"

	"dxcluster/internal/stability"
)

func TestStudyPairedFollowOnBoundaries(t *testing.T) {
	at := time.Unix(10000, 0).UTC()
	raw := rbnHistoryRow{Time: at, FreqKHz: 7000, DXCall: "K1ABC"}
	ordinary := studyDecision{Status: "output", Raw: raw, DecisionAtUTC: at, FinalIdentity: "K1ABC", FinalObserved: "K1ABC"}
	candidate := ordinary
	candidate.Applied, candidate.FinalIdentity, candidate.FinalObserved = true, "K1ABD", "K1ABD"
	spots := map[string][]stability.Spot{
		"K1ABC": {{Ts: 10000, Freq: 7000}, {Ts: 10000, Freq: 7000}, {Ts: 10900, Freq: 7000.5}, {Ts: 10901, Freq: 7000}},
		"K1ABD": {{Ts: 9999, Freq: 7000}, {Ts: 10000, Freq: 7000}, {Ts: 10700, Freq: 7000.5001}, {Ts: 10900, Freq: 6999.5}},
	}
	ordinary.R1, candidate.R1 = studyReference{Strong: true, Identity: "K1ABC"}, studyReference{Strong: true, Identity: "K1ABC"}
	candidate.R2 = studyReference{Strong: true, Identity: "K1ABD"}
	var result studyPairedStability
	result.observe(&ordinary, &candidate, spots, studyWeightStability)
	if result.IncludingFocal.OrdinaryOnly != 1 || result.ExcludingFocal.Neither != 1 ||
		result.AddedRenames.OrdinaryOnly != 1 || result.R1.OrdinaryOnly != 1 || result.R2.OrdinaryUnknown != 1 {
		t.Fatalf("wrong common-window decision: %+v", result)
	}
	candidate.DecisionAtUTC = at.Add(time.Second)
	spots["K1ABD"] = append(spots["K1ABD"], stability.Spot{Ts: 10901, Freq: 7000}, stability.Spot{Ts: 10901, Freq: 7000})
	result = studyPairedStability{}
	result.observe(&ordinary, &candidate, spots, studyWeightStability)
	if result.IncludingFocal.CandidateOnly != 1 || result.ExcludingFocal.CandidateOnly != 1 || result.Retimed != 1 {
		t.Fatalf("result did not use the common candidate time: %+v", result)
	}
	candidate.Status = "license_rejected"
	result = studyPairedStability{}
	result.observe(&ordinary, &candidate, spots, studyWeightStability)
	if result.LostOutput != 1 || result.IncludingFocal != (studyPairedStabilityTally{}) {
		t.Fatalf("suppression counted as an emitted callsign: %+v", result)
	}
}
