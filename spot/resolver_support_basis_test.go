// File role: Proves support basis through real owner publication, projection,
// compaction and loss controls without manufacturing weighted denominators.
package spot

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestResolverSupportBasisContracts(t *testing.T) {
	positive := ResolverSnapshot{Winner: "K1ABC", WinnerSupport: 2, WinnerWeightedSupportMilli: 1500,
		TotalReporters: 3, TotalWeightedSupportMilli: 2000,
		CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 2, WeightedSupportMilli: 1500}}}
	for _, tc := range []struct {
		name      string
		snapshot  ResolverSnapshot
		basis     ResolverSupportBasis
		configure func(*ResolverSnapshot)
	}{
		{"legacy_positive_weighted", positive, ResolverSupportBasisWeighted, nil},
		{"explicit_unavailable_positive", positive, ResolverSupportBasisUnavailable, func(s *ResolverSnapshot) { s.SupportBasis = ResolverSupportBasisUnavailable }},
		{"explicit_weighted_positive", positive, ResolverSupportBasisWeighted, func(s *ResolverSnapshot) { s.SupportBasis = ResolverSupportBasisWeighted }},
		{"weighted_zero_is_still_weighted", ResolverSnapshot{SupportBasis: ResolverSupportBasisWeighted, TotalReporters: 2, WinnerSupport: 2}, ResolverSupportBasisWeighted, nil},
		{"unknown_zero_is_not_raw", ResolverSnapshot{TotalReporters: 2, WinnerSupport: 2}, ResolverSupportBasisUnavailable, nil},
		{"explicit_raw_zero_weights", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 3, Winner: "K1ABC", WinnerSupport: 2, CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 2}}}, ResolverSupportBasisReceivers, nil},
		{"raw_positive_total_conflicts", positive, ResolverSupportBasisUnavailable, func(s *ResolverSnapshot) { s.SupportBasis = ResolverSupportBasisReceivers }},
		{"raw_positive_winner_conflicts", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 3, WinnerWeightedSupportMilli: 1}, ResolverSupportBasisUnavailable, nil},
		{"raw_positive_runner_conflicts", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 3, RunnerWeightedSupportMilli: 1}, ResolverSupportBasisUnavailable, nil},
		{"raw_positive_rank_conflicts", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 3, CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 2, WeightedSupportMilli: 1}}}, ResolverSupportBasisUnavailable, nil},
		{"raw_negative_rank_conflicts", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 3, CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 2, WeightedSupportMilli: -1}}}, ResolverSupportBasisUnavailable, nil},
		{"weighted_negative_total_unavailable", ResolverSnapshot{SupportBasis: ResolverSupportBasisWeighted, TotalWeightedSupportMilli: -1}, ResolverSupportBasisUnavailable, nil},
		{"unknown_negative_total_unavailable", ResolverSnapshot{TotalWeightedSupportMilli: -1}, ResolverSupportBasisUnavailable, nil},
		{"unknown_enum_positive_unavailable", positive, ResolverSupportBasisUnavailable, func(s *ResolverSnapshot) { s.SupportBasis = ResolverSupportBasis(255) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.snapshot
			if tc.configure != nil {
				tc.configure(&s)
			}
			before := s
			before.CandidateRanks = append([]ResolverCandidateSupport(nil), s.CandidateRanks...)
			if basis := s.EffectiveSupportBasis(); basis != tc.basis {
				t.Fatalf("effective basis=%d want %d", basis, tc.basis)
			}
			if !reflect.DeepEqual(s, before) {
				t.Fatal("basis interpretation mutated supplied evidence")
			}
			copied, compact := s, s.Compact()
			if copied.SupportBasis != s.SupportBasis || compact.SupportBasis != s.SupportBasis || compact.EffectiveSupportBasis() != tc.basis || !reflect.DeepEqual(compact.CandidateRanks, s.CandidateRanks) {
				t.Fatal("by-value copy or compaction lost basis/tallies")
			}
			encoded, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if _, exists := fields["SupportBasis"]; exists {
				t.Fatal("in-memory basis widened serialized snapshot fields")
			}
		})
	}
}

func TestResolverSupportBasisActualZeroPopulation(t *testing.T) {
	for _, mode := range []string{"CW", "RTTY"} {
		for _, count := range []int{1, 2, 10} {
			t.Run(fmt.Sprintf("%s/N%d", mode, count), func(t *testing.T) {
				weights := SpotterReliability{}
				for i := 0; i < count; i++ {
					weights[fmt.Sprintf("N0Z%02d", i)] = 0
				}
				r, d, _, at := freshProjectionOwner(t, SignalResolverConfig{QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 16, SpotterReliability: weights})
				t.Cleanup(r.Stop)
				key := NewResolverSignalKey(7010.05, "40m", mode, 500)
				for i := 0; i < count; i++ {
					if !r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: fmt.Sprintf("N0Z%02d", i), FrequencyKHz: 7010.05, RecencyWindow: time.Minute}) {
						t.Fatal("bounded zero-weight report was not queued")
					}
				}
				if consumed := d.Step(at.Add(time.Millisecond)); consumed != count {
					t.Fatalf("owner consumed=%d want %d", consumed, count)
				}
				s, ok := r.Lookup(key)
				if !ok || s.SupportBasis != ResolverSupportBasisWeighted || s.EffectiveSupportBasis() != ResolverSupportBasisWeighted || s.EvidenceLoss || s.TotalReporters != count || s.TotalWeightedSupportMilli != 0 || s.WinnerSupport != count || s.WinnerWeightedSupportMilli != 0 || s.State != ResolverStateUncertain || s.ReceiverEvidence().Len() != count {
					t.Fatalf("actual all-zero population changed support/basis/bounds: %+v", s)
				}
				freshProjectionCandidate(t, s, "K1ABC", count, 0)
				rows := 0
				s.ReceiverEvidence().Range(func(call string, row ResolverReceiverObservation) bool {
					rows++
					if call != "K1ABC" || row.WeightMilli != 0 || !row.ObservedAt.Equal(at) {
						t.Errorf("actual zero-weight row changed: %s %+v", call, row)
					}
					return true
				})
				if rows != count || r.MetricsSnapshot().CapLossDiscards != 0 || r.MetricsSnapshot().DropReliability != 0 {
					t.Fatal("zero-weight fixture silently lost receiver evidence")
				}
				compact := s.Compact()
				if compact.SupportBasis != ResolverSupportBasisWeighted || compact.ReceiverEvidence() != nil || compact.TotalWeightedSupportMilli != 0 || compact.WinnerSupport != count {
					t.Fatal("real owner compaction lost zero-population meaning")
				}
				for _, query := range []time.Time{at, at.Add(time.Minute + time.Nanosecond)} {
					projected, available := r.LookupEligible(key, query)
					if !available || projected.SupportBasis != ResolverSupportBasisWeighted || projected.EffectiveSupportBasis() != ResolverSupportBasisWeighted || projected.TotalWeightedSupportMilli != 0 || projected.State != ResolverStateUncertain {
						t.Fatalf("fresh/empty projection lost weighted-zero basis: %+v available=%t", projected, available)
					}
				}
			})
		}
	}
}

func TestResolverSupportBasisMissingReliabilityIsWeightedUnit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights SpotterReliability
	}{{"absent_table", nil}, {"absent_entry", SpotterReliability{"N0OTHER": .25}}} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{SpotterReliability: tc.weights})
			t.Cleanup(r.Stop)
			freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, time.Minute)
			s, ok := r.Lookup(key)
			if !ok || s.SupportBasis != ResolverSupportBasisWeighted || s.EffectiveSupportBasis() != ResolverSupportBasisWeighted || s.TotalWeightedSupportMilli != 1000 || s.WinnerWeightedSupportMilli != 1000 || s.TotalReporters != 1 {
				t.Fatalf("missing reliability must supply numerical unit weight: %+v", s)
			}
			rows := 0
			s.ReceiverEvidence().Range(func(call string, row ResolverReceiverObservation) bool {
				rows++
				if call != "K1ABC" || row.Receiver != "N0AAA" || row.WeightMilli != 1000 {
					t.Errorf("missing reliability lost unit weight in actual row: %s %+v", call, row)
				}
				return true
			})
			if rows != 1 {
				t.Fatal("unit-default owner omitted the eligible receiver row")
			}
		})
	}
}

func TestResolverSupportBasisProjectionExpiresLastPositiveWeight(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{SpotterReliability: SpotterReliability{"N0OLD": 1, "N0ZERO1": 0, "N0ZERO2": 0}})
	t.Cleanup(r.Stop)
	publicationAt := at.Add(time.Nanosecond)
	// One batch and one explicit sweep keep the positive row exactly at this
	// publication's inclusive cutoff. Per-row helper sweeps can prune it before
	// the projection under test, concealing the intended partial-expiry path.
	for _, row := range []struct {
		receiver string
		observed time.Time
	}{{"N0OLD", publicationAt.Add(-time.Minute)}, {"N0ZERO1", at}, {"N0ZERO2", at}} {
		if !r.Enqueue(ResolverEvidence{ObservedAt: row.observed, Key: key, DXCall: "K1ABC", Spotter: row.receiver, FrequencyKHz: 7010.05, RecencyWindow: time.Minute}) {
			t.Fatal("mixed-weight projection fixture report was not queued")
		}
	}
	if processed := d.Step(publicationAt); processed != 3 {
		t.Fatalf("mixed-weight owner processed=%d want 3", processed)
	}
	before, ok := r.Lookup(key)
	if !ok || !before.EvaluatedAt.Equal(publicationAt) || before.TotalReporters != 3 || before.TotalWeightedSupportMilli != 1000 || before.SupportBasis != ResolverSupportBasisWeighted {
		t.Fatalf("partial-expiry fixture lacks actual mixed-weight publication: %+v", before)
	}
	inclusive, ok := r.LookupEligible(key, publicationAt)
	if !ok || inclusive.TotalReporters != 3 || inclusive.TotalWeightedSupportMilli != 1000 || inclusive.SupportBasis != ResolverSupportBasisWeighted {
		t.Fatalf("last positive receiver must survive exact inclusive projection cutoff: %+v", inclusive)
	}
	projected, ok := r.LookupEligible(key, publicationAt.Add(time.Nanosecond))
	if !ok || projected.TotalReporters != 2 || projected.TotalWeightedSupportMilli != 0 || projected.WinnerWeightedSupportMilli != 0 || projected.SupportBasis != ResolverSupportBasisWeighted || projected.State != ResolverStateUncertain || projected.ReceiverEvidence().Len() != 2 {
		t.Fatalf("removing last positive weight must leave weighted zero, not raw: %+v", projected)
	}
	freshProjectionCandidate(t, projected, "K1ABC", 2, 0)
	if projected.Compact().SupportBasis != ResolverSupportBasisWeighted || before.TotalWeightedSupportMilli != 1000 || before.ReceiverEvidence().Len() != 3 {
		t.Fatal("projection compaction or retained publication changed evidence meaning")
	}
}

func TestResolverSupportBasisZeroLossControlRecovery(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxReportersPerCand: 2, SpotterReliability: SpotterReliability{"N0A1": 0, "N0A2": 0, "N0A3": 0}})
	t.Cleanup(r.Stop)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at.Add(-100*time.Second), 2*time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A2", at, 2*time.Minute)
	settled := at.Add(time.Second)
	d.Sweep(settled)
	ordinary := lossTestSnapshot(t, r, key)
	if ordinary.EvidenceLoss || ordinary.SupportBasis != ResolverSupportBasisWeighted || ordinary.TotalReporters != 2 || ordinary.TotalWeightedSupportMilli != 0 {
		t.Fatalf("ordinary zero publication must be healthy weighted evidence: %+v", ordinary)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", settled.Add(time.Millisecond), 2*time.Minute)
	for _, phase := range []struct {
		clock  time.Time
		window time.Duration
		loss   bool
	}{
		{settled.Add(time.Millisecond), 2 * time.Minute, true},
		{settled.Add(2 * time.Millisecond), 20 * time.Second, false},
		{settled.Add(3 * time.Millisecond), 2 * time.Minute, true},
		{settled.Add(4 * time.Millisecond), 20 * time.Second, false},
	} {
		lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", phase.clock, phase.window)
		s := lossTestSnapshot(t, r, key)
		if s.EvidenceLoss != phase.loss || s.SupportBasis != ResolverSupportBasisWeighted || s.EffectiveSupportBasis() != ResolverSupportBasisWeighted || s.TotalWeightedSupportMilli != 0 || s.TotalReporters != 2 || s.State != ResolverStateUncertain || s.ReceiverEvidence().Len() != 2 {
			t.Fatalf("zero-weight loss/control/recovery publication inconsistent: %+v", s)
		}
		fresh, ok := r.LookupEligible(key, phase.clock)
		if !ok || fresh.EvidenceLoss != phase.loss || fresh.SupportBasis != ResolverSupportBasisWeighted || fresh.TotalWeightedSupportMilli != 0 || fresh.Compact().SupportBasis != ResolverSupportBasisWeighted {
			t.Fatalf("loss/recovery projection lost weighted meaning: %+v available=%t", fresh, ok)
		}
	}
	if r.MetricsSnapshot().CapLossDiscards != 1 || r.MetricsSnapshot().LossPublicationRecoveries != 2 || ordinary.TotalWeightedSupportMilli != 0 || ordinary.ReceiverEvidence().Len() != 2 {
		t.Fatal("zero-weight controls changed loss units or retained immutable generation")
	}
}
