// File role: Exercises the real bounded arbitration with declared raw copies of
// actual zero-weight rows, and proves unavailable/mixed basis is absorbing.
package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func supportBasisRawPopulation(t *testing.T, overlap bool) spot.ResolverSnapshot {
	t.Helper()
	r, key, _ := accountingResolver(t, 1, "DL9ZZZ", overlap, spot.SpotterReliability{"N0AAA": 0, "N0BBB": 0, "N0CCC": 0})
	s, ok := r.Lookup(key)
	if !ok || s.TotalWeightedSupportMilli != 0 || s.WinnerWeightedSupportMilli != 0 || s.RunnerWeightedSupportMilli != 0 ||
		s.ReceiverEvidence() == nil || ResolverSupportForCall(s, "K1ABC") != 2 || ResolverSupportForCall(s, "DL9ZZZ") != 1 {
		t.Fatalf("raw fixture must first prove actual zero-weight published rows: %+v", s)
	}
	for _, candidate := range s.CandidateRanks {
		if candidate.WeightedSupportMilli != 0 {
			t.Fatal("actual zero-weight ranks unexpectedly contain weighted evidence")
		}
	}
	// Only this deliberate test interpretation supplies raw authority. The
	// actual owner above remains weighted/all-zero and supplies no percentage.
	s.SupportBasis, s.State, s.Winner, s.WinnerSupport = spot.ResolverSupportBasisReceivers, spot.ResolverStateConfident, "K1ABC", 2
	return s
}

func supportBasisNeighborhood(t *testing.T, populations []spot.ResolverSnapshot) ResolverPrimarySelection {
	t.Helper()
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	lookup := make(map[spot.ResolverSignalKey]spot.ResolverSnapshot, len(populations))
	for i, snapshot := range populations {
		neighbor := key
		neighbor.Bucket += int64(i - 1)
		snapshot.Key = neighbor
		lookup[neighbor] = snapshot
	}
	reads := 0
	selected := selectResolverPrimarySnapshotFromLookup(key, accountingConfig(), "K1ABC", func(k spot.ResolverSignalKey) (spot.ResolverSnapshot, bool) {
		reads++
		s, ok := lookup[k]
		return s, ok
	})
	if reads != 3 || !selected.SnapshotOK || !selected.UsedNeighborhood || selected.NeighborhoodSplit {
		t.Fatalf("actual bounded arbitration did not exercise admitted neighborhood rows: reads=%d selected=%+v", reads, selected)
	}
	return selected
}

func TestSupportBasisNeighborhoodAllRawReceiverUnion(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap_%t", overlap), func(t *testing.T) {
			raw := supportBasisRawPopulation(t, overlap)
			selected := supportBasisNeighborhood(t, []spot.ResolverSnapshot{raw, raw, raw})
			wantTotal, wantA, wantDissent := 3, 66, 33
			if overlap {
				wantTotal, wantA, wantDissent = 2, 100, 50
			}
			if selected.Snapshot.SupportBasis != spot.ResolverSupportBasisReceivers || selected.Snapshot.TotalReporters != wantTotal ||
				selected.Snapshot.TotalWeightedSupportMilli != 0 || ResolverSupportForCall(selected.Snapshot, "K1ABC") != 2 || ResolverSupportForCall(selected.Snapshot, "DL9ZZZ") != 1 ||
				selected.Snapshot.State != spot.ResolverStateConfident || selected.Snapshot.ReceiverEvidence() != nil {
				t.Fatalf("raw union manufactured receivers or lost its basis: %+v", selected)
			}
			for _, tc := range []struct {
				call    string
				percent int
			}{{"K1ABC", wantA}, {"DL9ZZZ", wantDissent}} {
				if got, ok := ResolverCallConfidencePercent(selected.Snapshot, tc.call); !ok || got != tc.percent {
					t.Fatalf("raw nonexclusive coverage for %s=%d/%t want %d", tc.call, got, ok, tc.percent)
				}
			}
			transformed := selectionForTemporalWinner(selected, "K1ABC")
			if transformed.Snapshot.SupportBasis != spot.ResolverSupportBasisReceivers || transformed.Snapshot.State != spot.ResolverStateConfident {
				t.Fatal("temporal copy lost deliberate raw scoring basis")
			}
		})
	}
}

func TestSupportBasisNeighborhoodMixedUnavailableAbsorbs(t *testing.T) {
	r, key, _ := accountingResolver(t, 1, "DL9ZZZ", false, nil)
	weighted, ok := r.Lookup(key)
	if !ok || weighted.TotalWeightedSupportMilli != 3000 || weighted.State != spot.ResolverStateConfident || weighted.Winner != "K1ABC" {
		t.Fatal("weighted fixture must first prove actual settled A2/dissent1 publication")
	}
	raw := supportBasisRawPopulation(t, false)
	unavailable := weighted
	unavailable.SupportBasis = spot.ResolverSupportBasisUnavailable
	for _, tc := range []struct {
		name      string
		snapshots []spot.ResolverSnapshot
	}{
		{"weighted_raw_weighted", []spot.ResolverSnapshot{weighted, raw, weighted}},
		{"raw_weighted_weighted", []spot.ResolverSnapshot{raw, weighted, weighted}},
		{"weighted_weighted_raw", []spot.ResolverSnapshot{weighted, weighted, raw}},
		{"unavailable_weighted_weighted", []spot.ResolverSnapshot{unavailable, weighted, weighted}},
		{"weighted_unavailable_weighted", []spot.ResolverSnapshot{weighted, unavailable, weighted}},
		{"weighted_weighted_unavailable", []spot.ResolverSnapshot{weighted, weighted, unavailable}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := supportBasisNeighborhood(t, tc.snapshots)
			if selected.Snapshot.SupportBasis != spot.ResolverSupportBasisUnavailable || selected.Snapshot.State != spot.ResolverStateUncertain ||
				selected.Snapshot.TotalReporters != 3 || ResolverSupportForCall(selected.Snapshot, "K1ABC") != 2 {
				t.Fatalf("mixed/unknown union regained numerical authority: %+v", selected)
			}
			for _, copy := range []spot.ResolverSnapshot{selected.Snapshot, selected.Snapshot.Compact(), selectionForTemporalWinner(selected, "K1ABC").Snapshot} {
				if got, ok := ResolverCallConfidencePercent(copy, "K1ABC"); got != 0 || ok || copy.EffectiveSupportBasis() != spot.ResolverSupportBasisUnavailable {
					t.Fatal("positive mixed tallies revived compatibility-weighted coverage")
				}
			}
		})
	}
	// Existing positive weighted producers without metadata remain compatible.
	weighted.SupportBasis = spot.ResolverSupportBasisUnspecified
	selected := supportBasisNeighborhood(t, []spot.ResolverSnapshot{weighted, weighted, weighted})
	if got, ok := ResolverCallConfidencePercent(selected.Snapshot, "K1ABC"); !ok || got != 66 || selected.Snapshot.SupportBasis != spot.ResolverSupportBasisWeighted {
		t.Fatal("unset-positive weighted neighborhood lost compatibility")
	}
}

func TestSupportBasisNeighborhoodClassifiesCompleteAggregateRanks(t *testing.T) {
	for _, tc := range []struct {
		name                                                string
		offsets                                             []int64
		candidateWeight, totalWeight, percent, stalePercent int
	}{
		{"cross_51", []int64{1}, 1000, 1500, 66, 38},
		{"cross_25_and_51", []int64{-2, -1, 1, 2}, 2260, 2760, 81, 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			weights := spot.SpotterReliability{"N0AAA": .580, "N0BBB": .500}
			for i := range tc.offsets {
				weights[fmt.Sprintf("N0C%d", i)] = .420
			}
			r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16, MaxActiveKeys: 8,
				MaxCandidatesPerKey: 4, MaxReportersPerCand: 4, HysteresisWindows: 1,
				EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, SpotterReliability: weights,
				FreqGuardMinSeparationKHz: 0, DistanceModelCW: "plain", MaxEditDistance: 1})
			driver, err := spot.NewSignalResolverDriver(r)
			if err != nil {
				t.Fatal(err)
			}
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
			admit := func(offset int64, call, receiver string) {
				neighbor := key
				neighbor.Bucket += offset
				if !r.Enqueue(spot.ResolverEvidence{Key: neighbor, ObservedAt: at, DXCall: call, Spotter: receiver,
					FrequencyKHz: 7010 + float64(offset)*.5, RecencyWindow: time.Minute}) {
					t.Fatal("threshold fixture refused actual receiver evidence")
				}
			}
			admit(0, "K1ABC", "N0AAA")
			admit(0, "K1ABD", "N0BBB")
			for i, offset := range tc.offsets {
				admit(offset, "K1ABC", fmt.Sprintf("N0C%d", i))
			}
			if driver.Drain() != 2+len(tc.offsets) {
				t.Fatal("actual threshold population did not reach the owner")
			}
			driver.Sweep(at.Add(time.Millisecond))
			best, ok := r.Lookup(key)
			if !ok || best.Winner != "K1ABC" || best.WinnerWeightedSupportMilli != 580 || best.TotalWeightedSupportMilli != 1080 ||
				best.State != spot.ResolverStateConfident || best.EvidenceLoss || best.ReceiverEvidence() == nil {
				t.Fatalf("inherited best bucket did not qualify actual A580/B500 rows: %+v", best)
			}
			cfg := config.CallCorrectionConfig{ResolverNeighborhoodEnabled: true, ResolverNeighborhoodBucketRadius: 2,
				ResolverNeighborhoodMaxDistance: 1, DistanceModelCW: "plain", FreqGuardRunnerUpRatio: .6}
			selected := selectResolverPrimarySnapshotFromLookup(key, cfg, "K1ABC", r.Lookup)
			union := selected.Snapshot
			if !selected.SnapshotOK || !selected.UsedNeighborhood || selected.NeighborhoodSplit || union.EvidenceLoss ||
				union.TotalReporters != 2+len(tc.offsets) || union.TotalWeightedSupportMilli != tc.totalWeight ||
				ResolverSupportForCall(union, "K1ABC") != 1+len(tc.offsets) || ResolverWeightedSupportForCall(union, "K1ABC") != tc.candidateWeight ||
				ResolverSupportForCall(union, "K1ABD") != 1 || ResolverWeightedSupportForCall(union, "K1ABD") != 500 ||
				union.State != spot.ResolverStateConfident || union.Winner != "K1ABC" {
				t.Fatalf("complete union was classified from stale bucket ranks: %+v", selected)
			}
			if percent, ok := ResolverCallConfidencePercent(union, "K1ABC"); !ok || percent != tc.percent {
				t.Fatalf("union share=%d/%t want independent %d%%", percent, ok, tc.percent)
			}
			stale := union
			stale.CandidateRanks = best.CandidateRanks
			if percent, ok := ResolverCallConfidencePercent(stale, "K1ABC"); !ok || percent != tc.stalePercent || classifyTemporalSnapshotState(stale) == spot.ResolverStateConfident {
				t.Fatal("threshold fixture failed to distinguish inherited best ranks from final aggregate ranks")
			}
		})
	}
}
