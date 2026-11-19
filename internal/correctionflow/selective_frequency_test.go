package correctionflow

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
	"dxcluster/spot"
)

var selectiveOptions = FrequencyProposalOptions{150, time.Minute, 1}

func selectiveRows() []spot.ResolverEvidenceRow {
	return append(f0BaseRows(), f0Row("K1ABC", "R4", 7021, 0))
}

func selectiveAssess(t *testing.T, rows []spot.ResolverEvidenceRow) SelectiveFrequencyAssessment {
	t.Helper()
	k, o, s, cfg := f0Fixture(t, rows)
	return AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
}

func TestSelectiveFrequencyUsefulControlAndLegacyParity(t *testing.T) {
	k, o, s, cfg := f0Fixture(t, f0BaseRows())
	legacy := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	if got.Reason != "assessed" || len(got.Proposals) != 1 || len(got.Declines) != 0 || !reflect.DeepEqual(got.FrequencyAssessment, legacy) {
		t.Fatalf("useful rescue or v1 scalars changed: %+v / %+v", got, legacy)
	}
	p := got.Proposals[0]
	if p.Identity != "K1ABC" || p.Support != 2 || p.TotalReporters != 2 || p.ConfidencePercent != 100 || got.ExcludedComponents != 1 {
		t.Fatalf("literal control: %+v", got)
	}
	legacy = AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 3})
	if legacy.Reason != "unavailable" || len(legacy.Proposals) != 0 {
		t.Fatalf("v1 admitted distance-three option: %+v", legacy)
	}
}

func TestSelectiveFrequencyCredibleNonNominee(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights [2]int
		raw     bool
		decline bool
	}{
		{"one_positive", [2]int{1000, 0}, false, false},
		{"two_small_positive", [2]int{1, 1}, false, true},
		{"zero_weight_raw", [2]int{1000, 0}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(selectiveRows(), f0Row("W1ZZZ", "RX", 7021, 0), f0Row("W1ZZZ", "RY", 7021, 0))
			for i, weight := range tc.weights {
				rows[len(rows)-2+i].Observation.WeightMilli = weight
			}
			if tc.raw {
				// Explicit raw snapshots carry no weighted tallies, including the
				// ranks reconstructed from their evidence. Preserve that contract.
				for i := range rows {
					rows[i].Observation.WeightMilli = 0
				}
			}
			k, o, s, cfg := f0Fixture(t, rows)
			if tc.raw {
				s[2].Snapshot.SupportBasis = spot.ResolverSupportBasisReceivers
			}
			got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
			if got.Reason != "assessed" || (len(got.Proposals) == 0) != tc.decline || (len(got.Declines) == 1) != tc.decline {
				t.Fatalf("credibility: %+v", got)
			}
			if tc.decline {
				d := got.Declines[0]
				if d.NomineeIdentity != "K1ABC" || d.OpponentIdentity != "W1ZZZ" || d.CredibleSupport != 2 || d.Reason != "selective_competitor" {
					t.Fatalf("non-nominee witness: %+v", d)
				}
			} else if got.Proposals[0].TotalReporters != 5 || got.Proposals[0].ConfidencePercent != 75 {
				t.Fatalf("zero-weight dissent changed denominator: %+v", got)
			}
		})
	}
}

func TestSelectiveFrequencyRetainsUnknownAndSpanningRivals(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []spot.ResolverEvidenceRow
	}{
		{"unknown", []spot.ResolverEvidenceRow{f0Row("W1ZZZ", "RX", 0, 0), f0Row("W1ZZZ", "RY", 0, 0)}},
		{"spanning", []spot.ResolverEvidenceRow{f0Row("W1ZZZ", "RX", 7021, 0), f0Row("W1ZZZ", "R1", 7021.3, 0), f0Row("W1ZZZ", "R2", 7021.3, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectiveAssess(t, append(selectiveRows(), tc.rows...))
			if got.ExcludedComponents != 1 || len(got.Proposals) != 0 || len(got.Declines) != 1 || got.Declines[0].OpponentIdentity != "W1ZZZ" {
				t.Fatalf("unresolved rival erased: %+v", got)
			}
		})
	}
	// The same credible, unrelated identity is harmless only after the existing
	// shared-receiver witnesses actually prove its complete frequency exclusion.
	got := selectiveAssess(t, selectiveRows())
	if len(got.Proposals) != 1 || len(got.Declines) != 0 || got.Proposals[0].TotalReporters != 3 {
		t.Fatalf("proven separate rival retained: %+v", got)
	}
}

func TestSelectiveFrequencyCanonicalReceiverCredibility(t *testing.T) {
	rows := append(selectiveRows(), f0Row("KH6/W1ZZZ", "RX", 0, 0), f0Row("W1ZZZ/KH6", "RX", 0, 0))
	k, o, s, cfg := f0Fixture(t, rows)
	s[3] = s[2]
	s[3].RequestedKey.Bucket++
	s[3].Snapshot.Key = s[3].RequestedKey
	got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	if len(got.Proposals) != 1 || len(got.Declines) != 0 || got.Proposals[0].Support != 3 || got.Proposals[0].TotalReporters != 4 {
		t.Fatalf("alias/source copies counted as receiver labels: %+v", got)
	}
	view, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ/KH6", "RY", 0, 0)}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s[3].Evidence = view
	got = AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	if len(got.Proposals) != 0 || len(got.Declines) != 1 || got.Declines[0].OpponentIdentity != "W1ZZZ/KH6" || got.Declines[0].CredibleSupport != 2 {
		t.Fatalf("second distinct canonical label missed: %+v", got)
	}
}

func TestSelectiveFrequencyUnusableCompetitorBeforeNomination(t *testing.T) {
	for _, rival := range []string{strings.Repeat("A", 33), strings.Repeat("A", 1024), "W1ZZZ?"} {
		t.Run(fmt.Sprintf("length%d_%c", len(rival), rival[len(rival)-1]), func(t *testing.T) {
			// Tied supports and disjoint receivers make the old tie helper reach
			// its distance calculation. The selective branch must refuse before it.
			rows := []spot.ResolverEvidenceRow{f0Row("K1ABC", "R1", 7021, 0), f0Row("K1ABC", "R2", 7021, 0),
				f0Row(rival, "RX", 7021, 0), f0Row(rival, "RY", 7021, 0)}
			got := selectiveAssess(t, rows)
			if got.Reason != "selective_unusable_competitor" || len(got.Proposals) != 0 || len(got.Declines) != 1 ||
				got.Declines[0].OpponentIdentity != rival || got.Declines[0].CredibleSupport != 2 || got.Declines[0].NomineeIdentity != "" {
				t.Fatalf("unusable identity reached nomination: %+v", got)
			}
		})
	}
	// Exactly 32 supported characters remains a normal competing identity.
	rival := strings.Repeat("A", 32)
	got := selectiveAssess(t, append(selectiveRows(), f0Row(rival, "RX", 7021, 0), f0Row(rival, "RY", 7021, 0)))
	if got.Reason != "assessed" || len(got.Declines) != 1 || got.Declines[0].Reason != "selective_competitor" {
		t.Fatalf("32-character boundary refused: %+v", got)
	}
}

func TestSelectiveFrequencyConfiguredFamilyExemption(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enabled  bool
		delta    int
		suffix   bool
		wantKept bool
	}{
		{"configured_suffix", true, 2, true, true},
		{"disabled", false, 2, true, false},
		{"length_excluded", true, 1, true, false},
		{"wrong_end", true, 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(selectiveRows(), f0Row("ABC", "RX", 7021, 0), f0Row("ABC", "RY", 7021, 0))
			k, o, s, cfg := f0Fixture(t, rows)
			cfg.FamilyPolicy.Truncation = config.CallCorrectionTruncationFamilyConfig{Enabled: tc.enabled, MaxLengthDelta: tc.delta,
				MinShorterLength: 3, AllowSuffixMatch: tc.suffix, AllowPrefixMatch: !tc.suffix}
			cfg.ResolverNeighborhoodAllowTruncation = false // Family exemption is not neighborhood admission.
			got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
			if (len(got.Proposals) == 1) != tc.wantKept {
				t.Fatalf("family policy: %+v", got)
			}
			if tc.wantKept && (got.Proposals[0].TotalReporters != 5 || got.Proposals[0].ConfidencePercent != 60) {
				t.Fatalf("related dissent removed: %+v", got)
			}
		})
	}
}

func TestSelectiveFrequencyPreservesAuthorityAndBounds(t *testing.T) {
	k, o, s, cfg := f0Fixture(t, selectiveRows())
	for _, tc := range []struct {
		name   string
		change func(*ResolverPrimarySelection, *[5]FrequencySource)
		reason string
	}{
		{"protected", func(o *ResolverPrimarySelection, _ *[5]FrequencySource) { o.NeighborhoodSplit = true }, "ordinary_protected"},
		{"loss", func(_ *ResolverPrimarySelection, s *[5]FrequencySource) { s[2].Snapshot.EvidenceLoss = true }, "evidence_loss"},
		{"basis", func(_ *ResolverPrimarySelection, s *[5]FrequencySource) {
			s[2].Snapshot.SupportBasis = spot.ResolverSupportBasisUnavailable
		}, "support_basis"},
		{"authority", func(_ *ResolverPrimarySelection, s *[5]FrequencySource) { s[2].Snapshot.CandidateRanks = nil }, "assessed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinary, sources := o, s
			tc.change(&ordinary, &sources)
			got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, selectiveOptions)
			if got.Reason != tc.reason || len(got.Proposals) != 0 {
				t.Fatalf("authority bypassed: %+v", got)
			}
		})
	}
	rows := append(selectiveRows(), f0Row("K1ABE", "R1", 7021, 0))
	k, o, s, cfg = f0Fixture(t, rows)
	s[2].PendingIncumbent, s[2].PendingChallenger = "K1ABC", "K1ABE"
	if got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions); len(got.Proposals) != 0 {
		t.Fatalf("pending transition bypassed: %+v", got)
	}
	for _, receivers := range []int{1, 64} {
		k, o, s, cfg = selectiveMaximumFixture(t, receivers)
		options := FrequencyProposalOptions{150, time.Minute, 3}
		got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, options)
		if got.Reason != "assessed" || len(got.Proposals) != 16 || !got.BeamLimited {
			t.Fatalf("exact 80-slot/%d-incidence input not assessed: %+v", 80*receivers, got)
		}
		extra, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ", "EXTRA", 7021, 0)}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		s[4].Evidence = spot.MergeResolverEvidenceViews(s[4].Evidence, extra)
		got = AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, options)
		want := "spelling_limit"
		if receivers == 64 {
			want = "incidence_limit"
		}
		if got.Reason != want || len(got.Proposals) != 0 {
			t.Fatalf("raw bound exceeded: %+v want=%s", got, want)
		}
	}
	rows = nil
	for i := 0; i < 16; i++ {
		rows = append(rows, f0Row(fmt.Sprintf("K%dABC", i), "R1", 7021+float64(i)*.3, 0))
	}
	k, o, s, cfg = f0Fixture(t, rows)
	options := FrequencyProposalOptions{150, time.Minute, 3}
	if got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, options); got.Reason != "assessed" || got.Components != 16 || len(got.Proposals) != 1 {
		t.Fatalf("16 components rejected: %+v", got)
	}
	extra, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ", "R1", 7026, 0)}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s[3] = s[2]
	s[3].RequestedKey.Bucket++
	s[3].Snapshot.Key, s[3].Evidence = s[3].RequestedKey, extra
	if got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, options); got.Reason != "component_limit" || len(got.Proposals) != 0 {
		t.Fatalf("17 components truncated: %+v", got)
	}
}

func TestSelectiveFrequencyInputAdmission(t *testing.T) {
	type request struct {
		key     spot.ResolverSignalKey
		subject string
		focal   float64
		sources [5]FrequencySource
		options FrequencyProposalOptions
	}
	for _, tc := range []struct {
		name   string
		change func(*request)
		reason string
	}{
		{"non_cw", func(r *request) { r.key.Mode = "RTTY" }, "unavailable"},
		{"subject", func(r *request) { r.subject = "?" }, "unavailable"},
		{"missing_evidence", func(r *request) { r.sources[2].Evidence = nil }, "missing_evidence"},
		{"gap", func(r *request) { r.options.GapHz = 151 }, "unavailable"},
		{"witness", func(r *request) { r.options.MaxWitnessSeparation = 59 * time.Second }, "unavailable"},
		{"focal_zero", func(r *request) { r.focal = 0 }, "unavailable"},
		{"focal_negative", func(r *request) { r.focal = -1 }, "unavailable"},
		{"focal_nan", func(r *request) { r.focal = math.NaN() }, "unavailable"},
		{"focal_infinite", func(r *request) { r.focal = math.Inf(1) }, "unavailable"},
		{"distance_zero", func(r *request) { r.options.MaxDistance = 0 }, "unavailable"},
		{"distance_four", func(r *request) { r.options.MaxDistance = 4 }, "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k, ordinary, sources, cfg := f0Fixture(t, f0BaseRows())
			r := request{k, "K1ABD", 7021, sources, selectiveOptions}
			tc.change(&r)
			got := AssessSelectiveFrequencyProposals(r.key, r.subject, r.focal, ordinary, r.sources, cfg, r.options)
			if got.Reason != tc.reason || len(got.Proposals) != 0 || len(got.Declines) != 0 {
				t.Fatalf("invalid input admitted or misclassified: %+v want=%s", got, tc.reason)
			}
		})
	}
}

func TestSelectiveFrequencyDirectedMorseDistance(t *testing.T) {
	// S (...) -> E (.) deletes two elements at cost 4 each: ceil(8/4)=2.
	// E -> S inserts two at cost 1 each: ceil(2/4)=1. T -> E substitutes
	// once: ceil(1/2)=1. The outer callsign edit DP preserves these 2/1 costs.
	spot.ConfigureMorseWeights(1, 4, 1, 1)
	t.Cleanup(func() { spot.ConfigureMorseWeights(1, 1, 2, 2) })
	if forward, reverse := spot.CorrectionCandidateDistance("K1ABS", "K1ABE", "CW", "morse", "plain"),
		spot.CorrectionCandidateDistance("K1ABE", "K1ABS", "CW", "morse", "plain"); forward != 2 || reverse != 1 {
		t.Fatalf("hand-derived directed fixture: forward=%d reverse=%d", forward, reverse)
	}
	rows := []spot.ResolverEvidenceRow{f0Row("K1ABE", "R1", 7021, 0), f0Row("K1ABE", "R2", 7021, 0), f0Row("K1ABE", "R3", 7021, 0),
		f0Row("K1ABS", "RX", 7021, 0), f0Row("K1ABS", "RY", 7021, 0)}
	k, o, s, cfg := f0Fixture(t, rows)
	// Rival remains evidence but lacks source nomination authority.
	s[2].Snapshot.CandidateRanks = []spot.ResolverCandidateSupport{{Call: "K1ABE", Support: 3, WeightedSupportMilli: 3000}}
	cfg.DistanceModelCW = "morse"
	got := AssessSelectiveFrequencyProposals(k, "K1ABT", 7021, o, s, cfg, selectiveOptions)
	if len(got.Proposals) != 0 || len(got.Declines) != 1 || got.Declines[0].OpponentIdentity != "K1ABS" {
		t.Fatalf("rival-to-nominee distance not used: %+v", got)
	}
	cfg.DistanceModelCW = "plain"
	if plain := AssessSelectiveFrequencyProposals(k, "K1ABT", 7021, o, s, cfg, selectiveOptions); len(plain.Proposals) != 1 {
		t.Fatalf("mode model ignored: %+v", plain)
	}
}

func TestSelectiveFrequencyDistanceCaps(t *testing.T) {
	for _, global := range []int{0, 1, 2, 3, 99, -1} {
		for _, option := range []int{1, 2, 3, 4} {
			for i, truth := range []string{"K1ABD", "K1AXY", "K1XYZ", "K2XYZ"} {
				t.Run(fmt.Sprintf("global%d/option%d/distance%d", global, option, i+1), func(t *testing.T) {
					k, o, s, cfg := f0Fixture(t, []spot.ResolverEvidenceRow{f0Row(truth, "R1", 7021, 0), f0Row(truth, "R2", 7021, 0)})
					cfg.MaxEditDistance = global
					got := AssessSelectiveFrequencyProposals(k, "K1ABC", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, option})
					want := option <= 3 && i+1 <= option && (global < 0 || i+1 <= global)
					if (len(got.Proposals) == 1) != want || (got.Reason == "unavailable") != (option > 3) {
						t.Fatalf("cap intersection: %+v want proposal=%t", got, want)
					}
				})
			}
		}
	}
}

func TestSelectiveFrequencySameGenerationChangedCredibility(t *testing.T) {
	rows := append(selectiveRows(), f0Row("W1ZZZ", "RX", 0, 0), f0Row("W1ZZZ", "RY", 0, 0))
	rows[len(rows)-1].Observation.WeightMilli = 0
	k, o, s, cfg := f0Fixture(t, rows)
	before := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	rows[len(rows)-1].Observation.WeightMilli = 1
	view, err := spot.NewStudyResolverEvidence(rows, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s[2].Evidence = view
	after := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	if len(before.Proposals) != 1 || len(after.Proposals) != 0 || len(after.Declines) != 1 || before.Witnesses != after.Witnesses {
		t.Fatalf("cached lifecycle credibility: %+v / %+v", before, after)
	}
}

func TestSelectiveFrequencyDeterminismOwnershipAndConcurrentReads(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{}
	for i := 0; i < 4; i++ {
		rows = append(rows, f0Row("K1ABC", fmt.Sprintf("A%d", i), 7021, 0))
		if i < 3 {
			rows = append(rows, f0Row("K1ABE", fmt.Sprintf("B%d", i), 7021, 0))
		}
		if i < 2 {
			rows = append(rows, f0Row("W1ZZZ", fmt.Sprintf("W%d", i), 0, 0), f0Row("DL9ZZZ", fmt.Sprintf("D%d", i), 0, 0))
		}
	}
	want := selectiveAssess(t, rows)
	if len(want.Proposals) != 0 || len(want.Declines) != 2 || want.Declines[0].NomineeIdentity != "K1ABC" ||
		want.Declines[1].NomineeIdentity != "K1ABE" || want.Declines[0].OpponentIdentity != "DL9ZZZ" || want.Declines[1].OpponentIdentity != "DL9ZZZ" {
		t.Fatalf("deterministic witness control: %+v", want)
	}
	slices.Reverse(rows)
	if got := selectiveAssess(t, rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("row permutation: %+v / %+v", got, want)
	}
	k, o, s, cfg := f0Fixture(t, rows)
	var borrowedIdentity, borrowedCall string
	s[2].Evidence.RangeCanonical(func(identity, call string, _ spot.ResolverReceiverObservation) bool {
		if identity == "DL9ZZZ" {
			borrowedIdentity, borrowedCall = identity, call
		}
		return true
	})
	got := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	if unsafe.StringData(got.Declines[0].OpponentIdentity) == unsafe.StringData(borrowedIdentity) ||
		unsafe.StringData(got.Declines[0].OpponentCall) == unsafe.StringData(borrowedCall) {
		t.Fatal("opponent string backing borrowed")
	}
	got.Declines[0].OpponentIdentity = "CHANGED"
	// Actual simultaneous assessments share immutable publications and config,
	// while each caller mutates only its own detached result. The race lane runs it.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				fresh := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
				if !reflect.DeepEqual(fresh, want) {
					t.Errorf("shared result or input mutation: %+v", fresh)
					return
				}
				fresh.Declines[0].OpponentCall = "CALLER-OWNED"
			}
		}()
	}
	wg.Wait()
	// Owned successful proposals must have the same isolation as refusals.
	k, o, s, cfg = f0Fixture(t, f0BaseRows())
	first := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions)
	first.Proposals[0].Call = "CHANGED"
	if fresh := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, selectiveOptions); fresh.Proposals[0].Call != "K1ABC" {
		t.Fatal("proposal slice borrowed")
	}
}
