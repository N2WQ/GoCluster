// File role: Score changed replay outputs on a common follow-on window.
// Only hash-bound archive rows supply corroboration. The focal row can be
// removed without removing other rows at the same time and frequency.
package main

import (
	"dxcluster/internal/stability"
	"dxcluster/spot"
)

type studyPairedStabilityTally struct {
	Both          int `json:"both"`
	OrdinaryOnly  int `json:"ordinary_only"`
	CandidateOnly int `json:"candidate_only"`
	Neither       int `json:"neither"`
}

func (t *studyPairedStabilityTally) add(ordinary, candidate bool) {
	switch {
	case ordinary && candidate:
		t.Both++
	case ordinary:
		t.OrdinaryOnly++
	case candidate:
		t.CandidateOnly++
	default:
		t.Neither++
	}
}

type studyPairedReferenceTally struct {
	OrdinaryOnly     int `json:"ordinary_only"`
	CandidateOnly    int `json:"candidate_only"`
	Neither          int `json:"neither"`
	Unknown          int `json:"unknown"`
	Conflicting      int `json:"conflicting"`
	OrdinaryUnknown  int `json:"ordinary_unknown"`
	CandidateUnknown int `json:"candidate_unknown"`
}

func (t *studyPairedReferenceTally) add(ordinary, candidate *studyDecision, left, right studyReference) {
	if !left.Strong {
		t.OrdinaryUnknown++
	}
	if !right.Strong {
		t.CandidateUnknown++
	}
	switch {
	case !left.Strong || !right.Strong:
		t.Unknown++
	case left.Identity != right.Identity:
		t.Conflicting++
	case left.Identity == ordinary.FinalIdentity:
		t.OrdinaryOnly++
	case right.Identity == candidate.FinalIdentity:
		t.CandidateOnly++
	default:
		t.Neither++
	}
}

type studyPairedStability struct {
	IncludingFocal studyPairedStabilityTally `json:"including_focal"`
	ExcludingFocal studyPairedStabilityTally `json:"excluding_focal"`
	AddedRenames   studyPairedStabilityTally `json:"added_renames_including_focal"`
	AddedNoFocal   studyPairedStabilityTally `json:"added_renames_excluding_focal"`
	R1             studyPairedReferenceTally `json:"r1"`
	R2             studyPairedReferenceTally `json:"r2"`
	GainedOutput   int                       `json:"gained_output"`
	LostOutput     int                       `json:"lost_output"`
	LostCorrection int                       `json:"lost_correction"`
	Retargeted     int                       `json:"retargeted"`
	Retimed        int                       `json:"retimed_outputs"`
}

func (s *studyPairedStability) observe(ordinary, candidate *studyDecision, spots map[string][]stability.Spot, cfg replayStabilityConfig) {
	before, after := ordinary.Status == "output", candidate.Status == "output"
	if !before && after {
		s.GainedOutput++
	}
	if before && !after {
		s.LostOutput++
	}
	if studyAuditApplied(ordinary) && !studyAuditApplied(candidate) {
		s.LostCorrection++
	}
	if !before || !after {
		return
	}
	if !ordinary.DecisionAtUTC.Equal(candidate.DecisionAtUTC) {
		s.Retimed++
	}
	if ordinary.FinalIdentity == candidate.FinalIdentity {
		return
	}
	if studyAuditApplied(ordinary) && studyAuditApplied(candidate) {
		s.Retargeted++
	}
	left, leftWithout := studyPairedFollowOn(ordinary.FinalObserved, candidate, spots, cfg)
	right, rightWithout := studyPairedFollowOn(candidate.FinalObserved, candidate, spots, cfg)
	s.IncludingFocal.add(left, right)
	s.ExcludingFocal.add(leftWithout, rightWithout)
	if studyAuditApplied(candidate) && !studyAuditApplied(ordinary) {
		s.AddedRenames.add(left, right)
		s.AddedNoFocal.add(leftWithout, rightWithout)
	}
	s.R1.add(ordinary, candidate, ordinary.R1, candidate.R1)
	s.R2.add(ordinary, candidate, ordinary.R2, candidate.R2)
}

func studyPairedFollowOn(call string, anchor *studyDecision, spots map[string][]stability.Spot, cfg replayStabilityConfig) (bool, bool) {
	winner := spot.NormalizeCallsign(call)
	if winner == "" {
		return false, false
	}
	event := stability.Correction{Ts: anchor.DecisionAtUTC.Unix(), Winner: winner, Freq: anchor.Raw.FreqKHz, Band: anchor.Raw.Band}
	rule := stability.Config{BucketMinutes: 60, WindowMinutes: cfg.WindowMinutes, MinFollowOn: cfg.MinFollowOn, FreqToleranceHz: cfg.FreqToleranceHz}
	followOn := stability.EvaluateFollowOn([]stability.Correction{event}, spots, anchor.Raw.Time.Unix(), rule)
	included := followOn.StableCount == 1
	// A bound focal row is in this population exactly once. If that exact
	// row is eligible for the query, a fourth row is necessary and sufficient
	// to meet the original three-row minimum after excluding it.
	if !included || anchor.Raw.Time.Unix() != event.Ts || spot.NormalizeSpotDXCallsign(anchor.Raw.DXCall) != winner {
		return included, included
	}
	rule.MinFollowOn++
	followOn = stability.EvaluateFollowOn([]stability.Correction{event}, spots, anchor.Raw.Time.Unix(), rule)
	return included, followOn.StableCount == 1
}
