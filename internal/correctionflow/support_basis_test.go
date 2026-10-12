// File role: Defines support-share golden vectors independent of glyph/model
// accuracy, distinguishing eligible zero from unavailable and explicit raw.
package correctionflow

import (
	"fmt"
	"testing"

	"dxcluster/spot"
)

func supportBasisSnapshot(basis spot.ResolverSupportBasis, total, weighted int, candidateRaw, candidateWeight int) spot.ResolverSnapshot {
	return spot.ResolverSnapshot{SupportBasis: basis, State: spot.ResolverStateConfident, Winner: "K1ABC",
		WinnerSupport: candidateRaw, WinnerWeightedSupportMilli: candidateWeight,
		TotalReporters: total, TotalWeightedSupportMilli: weighted,
		CandidateRanks: []spot.ResolverCandidateSupport{{Call: "K1ABC", Support: candidateRaw, WeightedSupportMilli: candidateWeight}},
	}
}

func TestSupportBasisPercentageGolden(t *testing.T) {
	for _, tc := range []struct {
		name                                         string
		basis                                        spot.ResolverSupportBasis
		total, weight, raw, candidateWeight, percent int
		available                                    bool
	}{
		{"known_zero", spot.ResolverSupportBasisWeighted, 3, 1000, 2, 0, 0, true},
		{"legacy_positive_zero_numerator", spot.ResolverSupportBasisUnspecified, 3, 1000, 2, 0, 0, true},
		{"rounded_zero", spot.ResolverSupportBasisWeighted, 102, 101000, 1, 1, 0, true},
		{"all_zero_weighted", spot.ResolverSupportBasisWeighted, 3, 0, 2, 0, 0, false},
		{"ambiguous_zero", spot.ResolverSupportBasisUnspecified, 3, 0, 2, 0, 0, false},
		{"explicit_raw", spot.ResolverSupportBasisReceivers, 3, 0, 2, 0, 66, true},
		{"positive_unequal", spot.ResolverSupportBasisWeighted, 3, 2850, 2, 1850, 64, true},
		{"competing_unequal", spot.ResolverSupportBasisWeighted, 3, 2850, 1, 1000, 35, true},
		{"declared_unavailable_positive", spot.ResolverSupportBasisUnavailable, 3, 2850, 2, 1850, 0, false},
		{"raw_with_weighted_header", spot.ResolverSupportBasisReceivers, 3, 2850, 2, 0, 0, false},
		{"raw_with_weighted_rank", spot.ResolverSupportBasisReceivers, 3, 0, 2, 1, 0, false},
		{"missing_call_evidence", spot.ResolverSupportBasisWeighted, 3, 1000, 0, 1000, 0, false},
		{"missing_population", spot.ResolverSupportBasisWeighted, 0, 1000, 1, 1000, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := supportBasisSnapshot(tc.basis, tc.total, tc.weight, tc.raw, tc.candidateWeight)
			for _, compact := range []bool{false, true} {
				s := snapshot
				if compact {
					s = s.Compact()
				}
				if got, ok := ResolverCallConfidencePercent(s, "K1ABC"); got != tc.percent || ok != tc.available {
					t.Fatalf("compact=%t percent=%d/%t want %d/%t", compact, got, ok, tc.percent, tc.available)
				}
				if got, ok := ResolverCallConfidencePercent(s, "K1ABD"); got != 0 || ok {
					t.Fatal("absent call acquired support coverage")
				}
			}
		})
	}
}

func TestSupportBasisRankZeroAndLossCannotRecoverFallback(t *testing.T) {
	for _, basis := range []spot.ResolverSupportBasis{spot.ResolverSupportBasisWeighted, spot.ResolverSupportBasisReceivers} {
		s := supportBasisSnapshot(basis, 3, 0, 0, 0)
		s.WinnerSupport = 2 // Stale winner cannot override explicit rank zero.
		if basis == spot.ResolverSupportBasisWeighted {
			s.TotalWeightedSupportMilli, s.WinnerWeightedSupportMilli = 1000, 1000
		}
		if got, ok := ResolverCallConfidencePercent(s, "K1ABC"); got != 0 || ok || classifyTemporalSnapshotState(s) != spot.ResolverStateUncertain {
			t.Fatalf("basis=%v stale fallback regained numerical authority", basis)
		}
		s.CandidateRanks[0].Call = "K1ABD"
		if got, ok := ResolverCallConfidencePercent(s, "K1ABC"); got != 0 || ok || classifyTemporalSnapshotState(s) != spot.ResolverStateUncertain {
			t.Fatal("call absent from the complete rank population regained stale winner support")
		}
		s.CandidateRanks[0].Call = "K1ABC"
		s.CandidateRanks[0].Support = 2
		s.EvidenceLoss = true
		for _, copy := range []spot.ResolverSnapshot{s, s.Compact()} {
			if got, ok := ResolverCallConfidencePercent(copy, "K1ABC"); got != 0 || ok {
				t.Fatal("protected loss supplied an available share")
			}
		}
	}
	// Legacy compact snapshots without ranks still retain eligible fallback
	// fields. Basis compatibility does not invent a known candidate.
	s := supportBasisSnapshot(spot.ResolverSupportBasisUnspecified, 3, 2850, 2, 1850)
	s.CandidateRanks = nil
	if got, ok := ResolverCallConfidencePercent(s, "K1ABC"); got != 64 || !ok {
		t.Fatal("eligible positive weighted legacy fields lost compatibility")
	}
}

func TestSupportBasisEmptyCallCannotUseMalformedEvidence(t *testing.T) {
	for _, basis := range []spot.ResolverSupportBasis{spot.ResolverSupportBasisWeighted, spot.ResolverSupportBasisReceivers} {
		for _, ranked := range []bool{false, true} {
			s := supportBasisSnapshot(basis, 3, 0, 2, 0)
			s.Winner = ""
			s.CandidateRanks[0].Call = ""
			if basis == spot.ResolverSupportBasisWeighted {
				s.TotalWeightedSupportMilli, s.WinnerWeightedSupportMilli, s.CandidateRanks[0].WeightedSupportMilli = 1000, 1000, 1000
			}
			if !ranked {
				s.CandidateRanks = nil
			}
			for _, call := range []string{"", " ", "\t\r\n"} {
				if percent, ok := ResolverCallConfidencePercent(s, call); percent != 0 || ok {
					t.Fatalf("basis=%v ranked=%t empty requested call %q used malformed evidence: %d/%t", basis, ranked, call, percent, ok)
				}
			}
		}
	}
}

func TestSupportBasisClassifierGoldenThresholds(t *testing.T) {
	for _, tc := range []struct {
		weight, percent int
		state           spot.ResolverState
	}{
		{0, 0, spot.ResolverStateUncertain}, {1, 0, spot.ResolverStateUncertain},
		{2499, 24, spot.ResolverStateUncertain}, {2500, 25, spot.ResolverStateProbable},
		{2501, 25, spot.ResolverStateProbable}, {5099, 50, spot.ResolverStateProbable},
		{5100, 51, spot.ResolverStateConfident}, {5101, 51, spot.ResolverStateConfident},
		{10000, 100, spot.ResolverStateConfident},
	} {
		t.Run(fmt.Sprint(tc.weight), func(t *testing.T) {
			s := supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 11, 10000, max(1, (tc.weight+999)/1000), tc.weight)
			if got, ok := ResolverCallConfidencePercent(s, s.Winner); !ok || got != tc.percent || classifyTemporalSnapshotState(s) != tc.state {
				t.Fatalf("independent threshold vector produced %d/%t state=%s want %d/%s", got, ok, classifyTemporalSnapshotState(s), tc.percent, tc.state)
			}
		})
	}
	knownZero := supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 1000, 2, 0)
	if classifyTemporalSnapshotState(knownZero) != spot.ResolverStateUncertain || ResolverConfidenceGlyphForCall(knownZero, true, "K1ABC") != "P" {
		t.Fatal("known weighted zero was promoted from raw two-of-three coverage")
	}
	for _, basis := range []spot.ResolverSupportBasis{spot.ResolverSupportBasisWeighted, spot.ResolverSupportBasisUnspecified, spot.ResolverSupportBasisUnavailable} {
		s := supportBasisSnapshot(basis, 3, 0, 2, 0)
		if classifyTemporalSnapshotState(s) != spot.ResolverStateUncertain {
			t.Fatal("undefined coverage entered a numerical classifier band")
		}
	}
	raw := supportBasisSnapshot(spot.ResolverSupportBasisReceivers, 3, 0, 2, 0)
	if classifyTemporalSnapshotState(raw) != spot.ResolverStateConfident {
		t.Fatal("deliberately raw two-of-three coverage lost its declared statistic")
	}
}
