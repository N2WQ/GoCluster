package correctionflow

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

var f0At = time.Unix(10000, 0).UTC()

func f0Row(call, receiver string, f float64, seconds int) spot.ResolverEvidenceRow {
	return spot.ResolverEvidenceRow{Call: call, Observation: spot.ResolverReceiverObservation{
		Receiver: receiver, FrequencyKHz: f, ObservedAt: f0At.Add(time.Duration(seconds) * time.Second), WeightMilli: 1000, HasReport: true, Report: 0}}
}

func f0Fixture(t *testing.T, rows []spot.ResolverEvidenceRow) (spot.ResolverSignalKey, ResolverPrimarySelection, [5]FrequencySource, config.CallCorrectionConfig) {
	t.Helper()
	key := spot.NewResolverSignalKey(7021, "40m", "CW", 500)
	view, err := spot.NewStudyResolverEvidence(rows, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	total, weight, _ := view.ReceiverTotals()
	snapshot := spot.ResolverSnapshot{Key: key, State: spot.ResolverStateConfident, Winner: "DL9ZZZ", Generation: 17,
		SupportBasis: spot.ResolverSupportBasisWeighted, CandidateRanks: view.CanonicalSupport(2), TotalReporters: total, TotalWeightedSupportMilli: weight}
	var sources [5]FrequencySource
	sources[2] = FrequencySource{RequestedKey: key, Read: true, Present: true, Snapshot: snapshot, Evidence: view}
	cfg := config.CallCorrectionConfig{MaxEditDistance: 3, ResolverNeighborhoodEnabled: true, ResolverNeighborhoodBucketRadius: 2,
		ResolverNeighborhoodMaxDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain", FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6}
	return key, ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}, sources, cfg
}

func f0BaseRows() []spot.ResolverEvidenceRow {
	return []spot.ResolverEvidenceRow{f0Row("K1ABC", "R1", 7021, 0), f0Row("K1ABC", "R2", 7021, 0),
		f0Row("DL9ZZZ", "R1", 7021.3, 0), f0Row("DL9ZZZ", "R2", 7021.3, 0), f0Row("DL9ZZZ", "R3", 7021.3, 0)}
}

func f0Assess(t *testing.T, rows []spot.ResolverEvidenceRow) FrequencyAssessment {
	t.Helper()
	k, ordinary, sources, cfg := f0Fixture(t, rows)
	return AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
}

func TestCCV2F0MatchingSourceAuthority(t *testing.T) {
	k, ordinary, sources, cfg := f0Fixture(t, f0BaseRows())
	got := AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "assessed" || got.ExcludedComponents != 1 || len(got.Proposals) != 1 {
		t.Fatalf("unreachable: %+v", got)
	}
	p := got.Proposals[0]
	if p.Call != "K1ABC" || p.Support != 2 || p.TotalReporters != 2 || p.ConfidencePercent != 100 || p.SubjectSupport != 0 {
		t.Fatalf("group=%+v", p)
	}
	if ordinary.Snapshot.TotalReporters != 3 || ordinary.Snapshot.Winner != "DL9ZZZ" || len(ordinary.Snapshot.CandidateRanks) != 2 {
		t.Fatal("ordinary evidence changed")
	}
	cfg.MaxEditDistance = -1
	if g := AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2}); len(g.Proposals) != 1 {
		t.Fatal("negative distance sentinel changed")
	}
	for _, state := range []spot.ResolverState{spot.ResolverStateUncertain, spot.ResolverStateSplit} {
		sources[2].Snapshot.State = state
		got = AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
		if len(got.Proposals) != 0 {
			t.Fatalf("unsettled source nominated: %s", state)
		}
	}
	sources[2].Snapshot.State = spot.ResolverStateConfident
	sources[2].Snapshot.CandidateRanks = []spot.ResolverCandidateSupport{{Call: "DL9ZZZ", Support: 3}}
	if got = AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2}); len(got.Proposals) != 0 {
		t.Fatal("missing matching source authority")
	}
}

func TestCCV2F0RepresentableBoundaries(t *testing.T) {
	for _, gap := range []int{150, 250} {
		boundary := 7021 + float64(gap)/1000
		for _, direction := range []float64{math.Inf(-1), math.Inf(1)} {
			f := math.Nextafter(boundary, direction)
			rows := f0BaseRows()
			for i := 2; i < len(rows); i++ {
				rows[i].Observation.FrequencyKHz = f
			}
			k, o, s, cfg := f0Fixture(t, rows)
			got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{gap, time.Minute, 2})
			want := 1
			if direction > 0 {
				want = 2
			}
			if got.Components != want {
				t.Fatalf("gap=%d direction=%g f=%.17g components=%d", gap, direction, f, got.Components)
			}
		}
	}
	for _, direction := range []float64{math.Inf(-1), math.Inf(1)} {
		k, o, s, cfg := f0Fixture(t, f0BaseRows())
		f := math.Nextafter(7020.9, direction)
		got := AssessFrequencyProposals(k, "K1ABD", f, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
		if (got.Reason == "assessed") != (direction > 0) {
			t.Fatalf("attachment direction=%g f=%.17g: %+v", direction, f, got)
		}
	}
}

func TestCCV2F0RawBounds(t *testing.T) {
	var rows []spot.ResolverEvidenceRow
	for c := 0; c < 16; c++ {
		for r := 0; r < 64; r++ {
			rows = append(rows, f0Row(fmt.Sprintf("K%dABC", c), fmt.Sprintf("R%d", r), 7021, 0))
		}
	}
	k, o, sources, cfg := f0Fixture(t, rows)
	for i := range sources {
		sources[i] = sources[2]
		sources[i].RequestedKey.Bucket = k.Bucket + int64(i-2)
		sources[i].Snapshot.Key = sources[i].RequestedKey
	}
	got := AssessFrequencyProposals(k, "K1ABD", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "assessed" {
		t.Fatalf("exact raw limits rejected: %+v", got)
	}
	extra, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ", "EXTRA", 7021, 0)}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources[4].Evidence = spot.MergeResolverEvidenceViews(sources[4].Evidence, extra)
	got = AssessFrequencyProposals(k, "K1ABD", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "incidence_limit" {
		t.Fatalf("5121 raw rows not rejected: %+v", got)
	}
	rows = rows[:0]
	for c := 0; c < 16; c++ {
		rows = append(rows, f0Row(fmt.Sprintf("K%dABC", c), "R1", 7021, 0))
	}
	k, o, sources, cfg = f0Fixture(t, rows)
	for i := range sources {
		sources[i] = sources[2]
		sources[i].RequestedKey.Bucket = k.Bucket + int64(i-2)
		sources[i].Snapshot.Key = sources[i].RequestedKey
	}
	sources[4].Evidence = spot.MergeResolverEvidenceViews(sources[4].Evidence, extra)
	got = AssessFrequencyProposals(k, "K1ABD", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "spelling_limit" {
		t.Fatalf("81 raw slots not rejected: %+v", got)
	}
}

func TestCCV2F0ComponentOverflowAndBeam(t *testing.T) {
	var rows []spot.ResolverEvidenceRow
	for c := 0; c < 16; c++ {
		rows = append(rows, f0Row(fmt.Sprintf("K1AB%c", 'A'+c), "R1", 7021, 0))
	}
	k, o, sources, cfg := f0Fixture(t, rows)
	got := AssessFrequencyProposals(k, "K1ABZ", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) != 16 || !got.BeamLimited {
		t.Fatalf("beam silently truncated: %+v", got)
	}
	for i := range rows {
		rows[i].Observation.FrequencyKHz = 7021 + float64(i)*.3
	}
	k, o, sources, cfg = f0Fixture(t, rows)
	got = AssessFrequencyProposals(k, "K1ABZ", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Components != 16 || got.Reason != "assessed" {
		t.Fatalf("16 components failed: %+v", got)
	}
	extra, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ", "R1", 7026, 0)}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources[3] = sources[2]
	sources[3].RequestedKey.Bucket++
	sources[3].Snapshot.Key = sources[3].RequestedKey
	sources[3].Evidence = extra
	got = AssessFrequencyProposals(k, "K1ABZ", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "component_limit" || len(got.Proposals) != 0 {
		t.Fatalf("17 components truncated: %+v", got)
	}
}

func TestCCV2F0UnknownDissentAndRawConflict(t *testing.T) {
	rows := append(f0BaseRows(), f0Row("W1ZZZ", "RX", 0, 0))
	got := f0Assess(t, rows)
	if len(got.Proposals) != 1 || got.Proposals[0].TotalReporters != 3 || got.Proposals[0].ConfidencePercent != 66 {
		t.Fatalf("unknown dissent erased: %+v", got)
	}
	rows[len(rows)-1].Observation.FrequencyConflict = true
	got = f0Assess(t, rows)
	if got.Reason != "frequency_conflict" || len(got.Proposals) != 0 {
		t.Fatalf("raw conflict hidden: %+v", got)
	}
	// A favorable newer cross-source value must not hide that raw conflict.
	k, ordinary, sources, cfg := f0Fixture(t, rows)
	view, err := spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{f0Row("W1ZZZ", "RX", 7021, 1)}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	next := sources[2]
	next.RequestedKey.Bucket++
	next.Snapshot.Key = next.RequestedKey
	next.Evidence = view
	sources[3] = next
	got = AssessFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "frequency_conflict" {
		t.Fatal("cross-source join concealed conflict")
	}
}

func TestCCV2F0AssociationBoundaries(t *testing.T) {
	for _, gap := range []int{150, 250} {
		for _, delta := range []float64{-0.001, 0, .001} {
			t.Run(fmt.Sprintf("gap%d/%g", gap, delta), func(t *testing.T) {
				rows := f0BaseRows()
				for i := 2; i < len(rows); i++ {
					rows[i].Observation.FrequencyKHz = 7021 + (float64(gap)+delta)/1000
				}
				k, o, s, cfg := f0Fixture(t, rows)
				got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{gap, time.Minute, 2})
				want := 2
				if delta < 0 {
					want = 1
				}
				if got.Components != want {
					t.Fatalf("components=%d want=%d", got.Components, want)
				}
			})
		}
	}
	for _, maxTime := range []time.Duration{time.Minute, 2 * time.Minute} {
		for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			rows := f0BaseRows()
			for i := 2; i < len(rows); i++ {
				rows[i].Observation.ObservedAt = f0At.Add(maxTime + offset)
			}
			k, o, s, cfg := f0Fixture(t, rows)
			got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, maxTime, 2})
			want := 1
			if offset > 0 {
				want = 0
			}
			if got.ExcludedComponents != want {
				t.Fatalf("time=%v offset=%v excluded=%d", maxTime, offset, got.ExcludedComponents)
			}
		}
	}
	for _, delta := range []float64{99.999, 100, 100.001} {
		k, o, s, cfg := f0Fixture(t, f0BaseRows())
		got := AssessFrequencyProposals(k, "K1ABD", 7021-delta/1000, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
		if (got.Reason == "assessed") != (delta <= 100) {
			t.Fatalf("attachment %g: %+v", delta, got)
		}
	}
}

func TestCCV2F0DistinctSharedWitnesses(t *testing.T) {
	for shared := 0; shared <= 2; shared++ {
		rows := f0BaseRows()
		for i := shared; i < 2; i++ {
			rows[i+2].Observation.Receiver = fmt.Sprintf("OTHER%d", i)
		}
		got := f0Assess(t, rows)
		want := 0
		if shared == 2 {
			want = 1
		}
		if got.ExcludedComponents != want {
			t.Fatalf("shared=%d: %+v", shared, got)
		}
	}
	rows := f0BaseRows()
	for i := 2; i < len(rows); i++ {
		rows[i].Call = "KH6/DL9ZZZ"
	}
	rows[3].Call, rows[3].Observation.Receiver = "DL9ZZZ/KH6", "R1"
	if got := f0Assess(t, rows); got.ExcludedComponents != 0 {
		t.Fatal("aliases inflated shared witnesses")
	}
}

func TestCCV2F0ExclusionNotTransitive(t *testing.T) {
	rows := f0BaseRows()
	rows = append(rows, f0Row("DL9ZZZ", "R4", 7021.3, 0), f0Row("W1ZZZ", "R3", 7021.6, 0), f0Row("W1ZZZ", "R4", 7021.6, 0))
	got := f0Assess(t, rows)
	if got.ExcludedComponents != 1 || len(got.Proposals) != 1 || got.Proposals[0].TotalReporters != 4 || got.Proposals[0].ConfidencePercent != 50 {
		t.Fatalf("transitive exclusion: %+v", got)
	}
}

func TestCCV2F0SpanningIdentityUnresolved(t *testing.T) {
	rows := f0BaseRows()
	rows[0].Call, rows[1].Call = "KH6/K1ABC", "KH6/K1ABC"
	k, o, s, cfg := f0Fixture(t, rows)
	control := AssessFrequencyProposals(k, "K1ABD/KH6", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(control.Proposals) != 1 {
		t.Fatalf("spanning fixture lacks reachable positive control: %+v identity=%s distance=%d", control, spot.CorrectionVoteKey("KH6/K1ABC"), spot.CallDistance("K1ABD/KH6", "KH6/K1ABC", "CW", "plain", "plain"))
	}
	rows = append(rows, f0Row("K1ABC/KH6", "R4", 7021.3, 0))
	k, o, s, cfg = f0Fixture(t, rows)
	got := AssessFrequencyProposals(k, "K1ABD/KH6", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) != 0 {
		t.Fatalf("spanning alias nominated: %+v", got)
	}
}

func TestCCV2F0GroupSupportBasis(t *testing.T) {
	k, o, sources, cfg := f0Fixture(t, f0BaseRows())
	for _, basis := range []spot.ResolverSupportBasis{spot.ResolverSupportBasisUnavailable, spot.ResolverSupportBasisReceivers} {
		s := sources
		s[2].Snapshot.SupportBasis = basis
		got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
		if len(got.Proposals) != 0 {
			t.Fatalf("invalid/mixed basis regained confidence: %d", basis)
		}
	}
	rows := f0BaseRows()
	for i := range rows {
		rows[i].Observation.WeightMilli = 0
	}
	k, o, sources, cfg = f0Fixture(t, rows)
	got := AssessFrequencyProposals(k, "K1ABD", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) != 0 {
		t.Fatal("zero weighted total used raw fallback")
	}
	sources[2].Snapshot.SupportBasis = spot.ResolverSupportBasisReceivers
	got = AssessFrequencyProposals(k, "K1ABD", 7021, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) != 1 || got.Proposals[0].ConfidencePercent != 100 {
		t.Fatalf("explicit raw failed: %+v", got)
	}
}

func TestCCV2F0ProtectedAuthority(t *testing.T) {
	k, o, s, cfg := f0Fixture(t, f0BaseRows())
	for _, state := range []spot.ResolverState{spot.ResolverStateSplit, spot.ResolverStateUncertain} {
		changed := o
		changed.Snapshot.State = state
		if got := AssessFrequencyProposals(k, "K1ABD", 7021, changed, s, cfg, FrequencyProposalOptions{150, time.Minute, 2}); got.Reason != "ordinary_protected" {
			t.Fatal("ordinary authority discarded")
		}
	}
	s[2].Snapshot.EvidenceLoss = true
	if got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2}); got.Reason != "evidence_loss" {
		t.Fatal("source loss ignored")
	}
}

func TestCCV2F0PendingApplicabilityAndFreshInvocation(t *testing.T) {
	rows := append(f0BaseRows(), f0Row("K1ABE", "R1", 7021, 0))
	k, o, s, cfg := f0Fixture(t, rows)
	s[2].PendingIncumbent, s[2].PendingChallenger = "K1ABC", "K1ABE"
	got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) != 0 {
		t.Fatal("applicable pending endpoints discarded")
	}
	s[2].PendingIncumbent, s[2].PendingChallenger = "DL9ZZZ", "W9ZZZ"
	got = AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if len(got.Proposals) < 1 {
		t.Fatal("unrelated pending froze method")
	}
	// The lifecycle does not change. New evidence must still be reassessed.
	for i := range rows {
		rows[i].Observation.FrequencyKHz = 7021
	}
	v, err := spot.NewStudyResolverEvidence(rows, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s[2].Evidence = v
	fresh := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.ExcludedComponents != 1 || fresh.ExcludedComponents != 0 || fresh.Proposals[0].ConfidencePercent >= got.Proposals[0].ConfidencePercent || got.Witnesses != fresh.Witnesses {
		t.Fatalf("same-lifecycle rows not recomputed: %+v / %+v", got, fresh)
	}
}

func TestCCV2F0RawFrequencyComponents(t *testing.T) {
	rows := f0BaseRows()
	// Adjacent 100-Hz gaps form a chain; a hull/centroid does not split it.
	rows = append(rows, f0Row("W1ZZZ", "RX", 7021.1, 0), f0Row("W1ZZZ", "RY", 7021.2, 0))
	if got := f0Assess(t, rows); got.Components != 1 || got.ExcludedComponents != 0 {
		t.Fatalf("adjacent chain split: %+v", got)
	}
	// With a 250-Hz gap threshold this is one component, but the focal query
	// lies in a 240-Hz hole and is not within 100 Hz of an actual report.
	rows = f0BaseRows()
	for i := 2; i < len(rows); i++ {
		rows[i].Observation.FrequencyKHz = 7021.24
	}
	k, o, s, cfg := f0Fixture(t, rows)
	got := AssessFrequencyProposals(k, "K1ABD", 7021.12, o, s, cfg, FrequencyProposalOptions{250, time.Minute, 2})
	if got.Reason != "focal_association" {
		t.Fatal("component hull invented attachment")
	}
	// Two components both within 100 Hz: nearest is not unique association.
	for i := 2; i < len(rows); i++ {
		rows[i].Observation.FrequencyKHz = 7021.15
	}
	k, o, s, cfg = f0Fixture(t, rows)
	got = AssessFrequencyProposals(k, "K1ABD", 7021.075, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Reason != "focal_association" {
		t.Fatal("ambiguous focal attachment chose closest")
	}
}

func TestCCV2F0DeterminismAndIsolation(t *testing.T) {
	rows := f0BaseRows()
	want := f0Assess(t, rows)
	slices.Reverse(rows)
	if got := f0Assess(t, rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("reorder changed result: %+v / %+v", got, want)
	}
	k, o, s, cfg := f0Fixture(t, rows)
	s[1] = FrequencySource{Read: true, RequestedKey: k}
	s[1].RequestedKey.Bucket--
	s[3] = s[2]
	s[3].RequestedKey.Bucket++
	s[3].Snapshot.Key = s[3].RequestedKey
	s[3].Snapshot.Generation = 0
	got := AssessFrequencyProposals(k, "K1ABD", 7021, o, s, cfg, FrequencyProposalOptions{150, time.Minute, 2})
	if got.Witnesses[0].Read || !got.Witnesses[1].Read || got.Witnesses[1].Present || !got.Witnesses[3].Present || got.Witnesses[3].Generation != 0 {
		t.Fatalf("source distinctions lost: %+v", got.Witnesses)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].Support != 2 {
		t.Fatalf("source duplicates inflated support: %+v", got)
	}
}
