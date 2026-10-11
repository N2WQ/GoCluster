package cluster

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func newTestTelnetFamilySuppressor() *telnetFamilySuppressor {
	return newTelnetFamilySuppressor(30*time.Second, 32, spot.CorrectionFamilyPolicy{
		Configured: true, TruncationEnabled: true, TruncationMaxLengthDelta: 1,
		TruncationMinShorterLength: 3, TruncationAllowPrefix: true, TruncationAllowSuffix: true,
	}, 500)
}

func telnetFamilyTestConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		FrequencyToleranceHz: 500, VoiceFrequencyToleranceHz: 2000,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	}
}

func telnetFamilyTestSelection(total int, candidates ...spot.ResolverCandidateSupport) correctionflow.ResolverPrimarySelection {
	snapshot := spot.ResolverSnapshot{State: spot.ResolverStateProbable, TotalReporters: total, CandidateRanks: candidates}
	for _, candidate := range candidates {
		if candidate.Support > snapshot.WinnerSupport {
			snapshot.Winner, snapshot.WinnerSupport = candidate.Call, candidate.Support
		}
	}
	return correctionflow.ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}
}

func telnetFamilyRawSelection(left string, leftSupport int, right string, rightSupport int) correctionflow.ResolverPrimarySelection {
	return telnetFamilyTestSelection(leftSupport+rightSupport,
		spot.ResolverCandidateSupport{Call: left, Support: leftSupport},
		spot.ResolverCandidateSupport{Call: right, Support: rightSupport})
}

func telnetFamilyTestHandoff(tb testing.TB, s *telnetFamilySuppressor, sp *spot.Spot, cfg config.CallCorrectionConfig, now time.Time, allowed familyLanes, selection correctionflow.ResolverPrimarySelection, admitted bool, want familyLanes) {
	tb.Helper()
	published := 0
	got := s.handoff(sp, cfg, now, allowed,
		func(time.Time) correctionflow.ResolverPrimarySelection { return selection },
		func(lanes familyLanes) bool {
			published++
			if lanes != want {
				tb.Errorf("publisher mask=%+v, want %+v", lanes, want)
			}
			return admitted
		})
	wantPublished := 0
	if want != (familyLanes{}) {
		wantPublished = 1
	}
	if got != want || published != wantPublished {
		tb.Fatalf("attempt=%+v publisher calls=%d, want %+v/%d", got, published, want, wantPublished)
	}
}

func telnetFamilyAdmit(tb testing.TB, s *telnetFamilySuppressor, call string, frequency float64, mode string, cfg config.CallCorrectionConfig, now time.Time, lanes familyLanes) {
	tb.Helper()
	telnetFamilyTestHandoff(tb, s, spot.NewSpot(call, "W0TEST", frequency, mode), cfg, now, lanes, correctionflow.ResolverPrimarySelection{}, true, lanes)
}

type telnetFamilyTestEntry struct {
	seenAt time.Time
	lanes  [3]telnetFamilyWitness
}
type telnetFamilyTestState struct {
	entries map[string]telnetFamilyTestEntry
	order   []string
}

// Check the bound through maps and both LRU directions, not only the counter.
func telnetFamilyState(tb testing.TB, s *telnetFamilySuppressor, wantCount int) telnetFamilyTestState {
	tb.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	state := telnetFamilyTestState{entries: make(map[string]telnetFamilyTestEntry)}
	nodes := make(map[*telnetFamilyEntry]string)
	for bucket, calls := range s.buckets {
		if len(calls) == 0 {
			tb.Fatal("retained empty bucket")
		}
		for key, entry := range calls {
			if entry == nil || entry.bucket != bucket || entry.key != key {
				tb.Fatal("entry/map ownership mismatch")
			}
			if _, exists := nodes[entry]; exists {
				tb.Fatal("entry retained in multiple maps")
			}
			identity := fmt.Sprintf("%s:%d:%s", bucket.mode, bucket.freqBin, key)
			nodes[entry] = identity
			state.entries[identity] = telnetFamilyTestEntry{entry.seenAt, entry.lanes}
		}
	}
	seen := make(map[*telnetFamilyEntry]bool)
	var previous *telnetFamilyEntry
	for entry := s.head; entry != nil; entry = entry.next {
		if seen[entry] {
			tb.Fatal("LRU cycle")
		}
		seen[entry] = true
		identity, exists := nodes[entry]
		if !exists || entry.prev != previous {
			tb.Fatal("LRU/map ownership or reverse link mismatch")
		}
		state.order = append(state.order, identity)
		previous = entry
	}
	if previous != s.tail || len(seen) != len(nodes) || len(nodes) != s.totalEntries || len(nodes) != wantCount || len(nodes) > s.maxEntries {
		tb.Fatalf("retention maps=%d list=%d total=%d cap=%d want=%d", len(nodes), len(seen), s.totalEntries, s.maxEntries, wantCount)
	}
	return state
}

func TestTelnetFamilySuppressorPercentagePrecedence(t *testing.T) {
	all := familyLanes{true, true, true}
	for _, tc := range []struct {
		name, first, incoming string
		firstN, incomingN     int
		edit                  bool
		want                  familyLanes
	}{
		{"stronger_base_second", "KP4/N2WQ", "N2WQ", 1, 3, false, all},
		{"weaker_slash_second", "N2WQ", "KP4/N2WQ", 3, 1, false, familyLanes{}},
		{"stronger_shorter_second", "W1ABC", "W1AB", 1, 3, false, all},
		{"weaker_longer_second", "W1AB", "W1ABC", 3, 1, false, familyLanes{}},
		{"slash_tie_after_base", "N2WQ", "KP4/N2WQ", 1, 1, false, all},
		{"base_tie_after_slash", "KP4/N2WQ", "N2WQ", 1, 1, false, familyLanes{}},
		{"shorter_tie_after_longer", "W1ABC", "W1AB", 1, 1, false, all},
		{"longer_tie_after_shorter", "W1AB", "W1ABC", 1, 1, false, all},
		{"stronger_edit_neighbor", "K1ABC", "K1ABD", 1, 3, true, all},
		{"weaker_edit_neighbor", "K1ABD", "K1ABC", 3, 1, true, familyLanes{}},
		{"edit_tie_forward", "K1ABC", "K1ABD", 1, 1, true, all},
		{"edit_tie_reverse", "K1ABD", "K1ABC", 1, 1, true, all},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled = tc.edit
			telnetFamilyAdmit(t, s, tc.first, 7010, "CW", cfg, now, all)
			telnetFamilyTestHandoff(t, s, spot.NewSpot(tc.incoming, "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
				telnetFamilyRawSelection(tc.first, tc.firstN, tc.incoming, tc.incomingN), true, tc.want)
			count := 1
			if tc.want != (familyLanes{}) {
				count++
			}
			telnetFamilyState(t, s, count)
		})
	}
}

func TestTelnetFamilySuppressorExactWholePercentMath(t *testing.T) {
	// Constants come from the independent ratio oracle, not a production helper.
	for _, tc := range []struct {
		name, call      string
		total, weighted int
		ranks           []spot.ResolverCandidateSupport
		want            int
		ok              bool
	}{
		{"unequal_weight_A", "W1AB", 3, 2850, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 2, WeightedSupportMilli: 1850}, {Call: "W1ABC", Support: 1, WeightedSupportMilli: 1000}}, 64, true},
		{"unequal_weight_B", "W1ABC", 3, 2850, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 2, WeightedSupportMilli: 1850}, {Call: "W1ABC", Support: 1, WeightedSupportMilli: 1000}}, 35, true},
		{"union_dissent", "W1AB", 3, 3000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1, WeightedSupportMilli: 1000}, {Call: "W1ABC", Support: 1, WeightedSupportMilli: 1000}}, 33, true},
		{"overlap_A", "W1AB", 3, 3000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 2, WeightedSupportMilli: 2000}, {Call: "W1ABC", Support: 2, WeightedSupportMilli: 2000}}, 66, true},
		{"overlap_B", "W1ABC", 3, 3000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 2, WeightedSupportMilli: 2000}, {Call: "W1ABC", Support: 2, WeightedSupportMilli: 2000}}, 66, true},
		{"rounded_base", "N2WQ", 3, 1000, []spot.ResolverCandidateSupport{{Call: "N2WQ", Support: 1, WeightedSupportMilli: 334}, {Call: "KP4/N2WQ", Support: 1, WeightedSupportMilli: 330}, {Call: "W9XYZ", Support: 1, WeightedSupportMilli: 336}}, 33, true},
		{"rounded_slash", "KP4/N2WQ", 3, 1000, []spot.ResolverCandidateSupport{{Call: "N2WQ", Support: 1, WeightedSupportMilli: 334}, {Call: "KP4/N2WQ", Support: 1, WeightedSupportMilli: 330}, {Call: "W9XYZ", Support: 1, WeightedSupportMilli: 336}}, 33, true},
		{"boundary_50", "W1AB", 3, 1000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1, WeightedSupportMilli: 500}, {Call: "W1ABC", Support: 1, WeightedSupportMilli: 499}, {Call: "W9XYZ", Support: 1, WeightedSupportMilli: 1}}, 50, true},
		{"boundary_49", "W1ABC", 3, 1000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1, WeightedSupportMilli: 500}, {Call: "W1ABC", Support: 1, WeightedSupportMilli: 499}, {Call: "W9XYZ", Support: 1, WeightedSupportMilli: 1}}, 49, true},
		{"raw_no_weighted_total", "W1AB", 3, 0, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 2}, {Call: "W1ABC", Support: 1}}, 66, true},
		{"absent_candidate", "W1ABC", 4, 0, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1}}, 0, false},
		{"mixed_basis", "W1ABC", 3, 1000, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1, WeightedSupportMilli: 1000}, {Call: "W1ABC", Support: 2}}, 0, false},
		{"no_positive_support", "W1AB", 3, 1000, []spot.ResolverCandidateSupport{{Call: "W1AB", WeightedSupportMilli: 1000}}, 0, false},
		{"no_total", "W1AB", 0, 0, []spot.ResolverCandidateSupport{{Call: "W1AB", Support: 1}}, 0, false},
		{"literal_alias_not_pooled", "KP4/N2WQ", 3, 0, []spot.ResolverCandidateSupport{{Call: "N2WQ/KP4", Support: 2}}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := spot.ResolverSnapshot{TotalReporters: tc.total, TotalWeightedSupportMilli: tc.weighted, CandidateRanks: tc.ranks,
				Winner: "W9CONTROL", WinnerSupport: 3}
			got, ok := familyPercent(snapshot, tc.call)
			if ok != tc.ok || (ok && got != tc.want) {
				t.Fatalf("percentage=%d,%t want %d,%t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestTelnetFamilySuppressorWholePercentTieAndBoundary(t *testing.T) {
	all := familyLanes{true, true, true}
	for _, tc := range []struct {
		name, first, incoming                      string
		firstWeight, incomingWeight, dissentWeight int
		want                                       familyLanes
	}{
		{"rounded_tie_prefers_slash", "N2WQ", "KP4/N2WQ", 334, 330, 336, all},
		{"rounded_tie_hides_base", "KP4/N2WQ", "N2WQ", 330, 334, 336, familyLanes{}},
		{"boundary_stronger_shorter", "W1ABC", "W1AB", 499, 500, 1, all},
		{"boundary_hides_longer", "W1AB", "W1ABC", 500, 499, 1, familyLanes{}},
		{"other_rounded_tie", "W1ABC", "W1AB", 330, 334, 336, all},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			telnetFamilyAdmit(t, s, tc.first, 7010, "CW", cfg, now, all)
			selection := telnetFamilyTestSelection(3,
				spot.ResolverCandidateSupport{Call: tc.first, Support: 1, WeightedSupportMilli: tc.firstWeight},
				spot.ResolverCandidateSupport{Call: tc.incoming, Support: 1, WeightedSupportMilli: tc.incomingWeight},
				spot.ResolverCandidateSupport{Call: "W9XYZ", Support: 1, WeightedSupportMilli: tc.dissentWeight})
			selection.Snapshot.TotalWeightedSupportMilli = 1000
			telnetFamilyTestHandoff(t, s, spot.NewSpot(tc.incoming, "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all, selection, true, tc.want)
		})
	}
}

func TestTelnetFamilySuppressorUnavailableEvidenceCannotHide(t *testing.T) {
	all := familyLanes{true, true, true}
	for _, tc := range []struct {
		name  string
		alter func(*correctionflow.ResolverPrimarySelection)
	}{
		{"missing", func(s *correctionflow.ResolverPrimarySelection) { s.SnapshotOK = false }},
		{"split", func(s *correctionflow.ResolverPrimarySelection) { s.Snapshot.State = spot.ResolverStateSplit }},
		{"uncertain", func(s *correctionflow.ResolverPrimarySelection) { s.Snapshot.State = spot.ResolverStateUncertain }},
		{"loss", func(s *correctionflow.ResolverPrimarySelection) { s.Snapshot.EvidenceLoss = true }},
		{"global_split", func(s *correctionflow.ResolverPrimarySelection) { s.NeighborhoodSplit = true }},
		{"incoming_absent", func(s *correctionflow.ResolverPrimarySelection) {
			s.Snapshot.CandidateRanks = s.Snapshot.CandidateRanks[:1]
		}},
		{"rival_absent", func(s *correctionflow.ResolverPrimarySelection) {
			s.Snapshot.CandidateRanks = s.Snapshot.CandidateRanks[1:]
			s.Snapshot.Winner, s.Snapshot.WinnerSupport = "", 0
		}},
		{"mixed_basis", func(s *correctionflow.ResolverPrimarySelection) {
			s.Snapshot.TotalWeightedSupportMilli = 3000
			s.Snapshot.CandidateRanks[0].WeightedSupportMilli = 3000
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled = true
			telnetFamilyAdmit(t, s, "KP4/N2WQ", 7010, "CW", cfg, now, all)
			selection := telnetFamilyRawSelection("KP4/N2WQ", 3, "N2WQ", 1)
			tc.alter(&selection)
			telnetFamilyTestHandoff(t, s, spot.NewSpot("N2WQ", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all, selection, true, all)
			telnetFamilyState(t, s, 2)
		})
	}
}

func TestTelnetFamilySuppressorAdmittedLaneExpiry(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, familyLanes{fast: true})
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now.Add(10*time.Second), familyLanes{med: true})
	for _, tc := range []struct {
		delta time.Duration
		want  familyLanes
	}{
		{30 * time.Second, familyLanes{slow: true}},
		{30*time.Second + time.Nanosecond, familyLanes{fast: true, slow: true}},
		{40 * time.Second, familyLanes{fast: true, slow: true}},
		{40*time.Second + time.Nanosecond, all},
	} {
		telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(tc.delta), all,
			telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, tc.want)
	}
	telnetFamilyState(t, s, 0)
}

func TestTelnetFamilySuppressorRepeatedFastDoesNotRefreshOtherLanes(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, all)
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now.Add(10*time.Second), familyLanes{fast: true})
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(31*time.Second), all,
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, familyLanes{med: true, slow: true})
	telnetFamilyState(t, s, 1)
}

func TestTelnetFamilySuppressorAdmittedLaneFrequency(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	cfg.FrequencyToleranceHz = 1000
	telnetFamilyAdmit(t, s, "W1ABC", 7010.1, "CW", cfg, now, familyLanes{fast: true})
	telnetFamilyAdmit(t, s, "W1ABC", 7010.4, "CW", cfg, now.Add(time.Second), familyLanes{med: true})
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7011.2, "CW"), cfg, now.Add(2*time.Second), familyLanes{true, true, true},
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, familyLanes{fast: true, slow: true})
	telnetFamilyState(t, s, 1)
}

func TestTelnetFamilySuppressorLiteralWitnessPerLane(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	telnetFamilyAdmit(t, s, "KP4/N2WQ", 7010, "CW", cfg, now, familyLanes{fast: true})
	telnetFamilyAdmit(t, s, "N2WQ/KP4", 7010, "CW", cfg, now.Add(time.Second), familyLanes{med: true})
	selection := telnetFamilyTestSelection(3,
		spot.ResolverCandidateSupport{Call: "KP4/N2WQ", Support: 1, WeightedSupportMilli: 600},
		spot.ResolverCandidateSupport{Call: "N2WQ", Support: 1, WeightedSupportMilli: 300},
		spot.ResolverCandidateSupport{Call: "N2WQ/KP4", Support: 1, WeightedSupportMilli: 100})
	selection.Snapshot.TotalWeightedSupportMilli = 1000
	telnetFamilyTestHandoff(t, s, spot.NewSpot("N2WQ", "W0TEST", 7010, "CW"), cfg, now.Add(2*time.Second), familyLanes{true, true, true}, selection, false, familyLanes{med: true, slow: true})
	for _, entry := range telnetFamilyState(t, s, 1).entries {
		if entry.lanes[0].call != "KP4/N2WQ" || entry.lanes[1].call != "N2WQ/KP4" || entry.lanes[2].call != "" {
			t.Fatalf("literal witnesses pooled or crossed lanes: %+v", entry.lanes)
		}
	}
}

func TestTelnetFamilySuppressorWinnerRunnerFallback(t *testing.T) {
	all := familyLanes{true, true, true}
	for _, weighted := range []bool{false, true} {
		for _, rivalIsWinner := range []bool{false, true} {
			t.Run(fmt.Sprintf("weighted_%t_rival_winner_%t", weighted, rivalIsWinner), func(t *testing.T) {
				s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
				telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, all)
				snapshot := spot.ResolverSnapshot{
					State: spot.ResolverStateProbable, TotalReporters: 4,
					Winner: "W1AB", WinnerSupport: 1, RunnerUp: "W1ABC", RunnerSupport: 3,
				}
				if rivalIsWinner {
					snapshot.Winner, snapshot.RunnerUp = snapshot.RunnerUp, snapshot.Winner
					snapshot.WinnerSupport, snapshot.RunnerSupport = snapshot.RunnerSupport, snapshot.WinnerSupport
				}
				if weighted {
					snapshot.TotalWeightedSupportMilli = 4000
					snapshot.WinnerWeightedSupportMilli = snapshot.WinnerSupport * 1000
					snapshot.RunnerWeightedSupportMilli = snapshot.RunnerSupport * 1000
				}
				if percent, ok := familyPercent(snapshot, "W1ABC"); !ok || percent != 75 {
					t.Fatalf("rival fallback=%d,%t, want 75,true", percent, ok)
				}
				if percent, ok := familyPercent(snapshot, "W1AB"); !ok || percent != 25 {
					t.Fatalf("incoming fallback=%d,%t, want 25,true", percent, ok)
				}
				before := telnetFamilyState(t, s, 1)
				telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
					correctionflow.ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}, true, familyLanes{})
				if after := telnetFamilyState(t, s, 1); !reflect.DeepEqual(before, after) {
					t.Fatal("fallback hiding changed admitted history")
				}
			})
		}
	}
}

func TestTelnetFamilySuppressorRankedSupportOverridesWinnerFallback(t *testing.T) {
	all := familyLanes{true, true, true}
	for _, tc := range []struct {
		name     string
		weighted bool
		raw      int
	}{
		{"ranked_zero_raw", false, 0},
		{"ranked_zero_weighted", true, 0},
		{"ranked_mixed_basis", true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, all)
			snapshot := spot.ResolverSnapshot{
				State: spot.ResolverStateConfident, TotalReporters: 4, Winner: "W1ABC", WinnerSupport: 3,
				CandidateRanks: []spot.ResolverCandidateSupport{{Call: "W1ABC", Support: tc.raw}, {Call: "W1AB", Support: 1}},
			}
			if tc.weighted {
				snapshot.TotalWeightedSupportMilli, snapshot.WinnerWeightedSupportMilli = 4000, 3000
				snapshot.CandidateRanks[1].WeightedSupportMilli = 1000
			}
			if _, ok := familyPercent(snapshot, "W1ABC"); ok {
				t.Fatal("ranked unavailable support reused positive winner fallback")
			}
			telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
				correctionflow.ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}, true, all)
			telnetFamilyState(t, s, 2)
		})
	}
}

func TestTelnetFamilySuppressorBothLiteralAliasesProtectTheirLanes(t *testing.T) {
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("reversed_ranks_%t", reversed), func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			telnetFamilyAdmit(t, s, "KP4/N2WQ", 7010, "CW", cfg, now, familyLanes{fast: true})
			telnetFamilyAdmit(t, s, "N2WQ/KP4", 7010, "CW", cfg, now.Add(time.Second), familyLanes{med: true})
			// Four receiver weights 400,200,100,300: the slash aliases share
			// the first receiver; base shares the second; the fourth dissents.
			selection := telnetFamilyTestSelection(4,
				spot.ResolverCandidateSupport{Call: "KP4/N2WQ", Support: 2, WeightedSupportMilli: 600},
				spot.ResolverCandidateSupport{Call: "N2WQ/KP4", Support: 2, WeightedSupportMilli: 500},
				spot.ResolverCandidateSupport{Call: "N2WQ", Support: 1, WeightedSupportMilli: 200},
				spot.ResolverCandidateSupport{Call: "W9XYZ", Support: 1, WeightedSupportMilli: 300})
			selection.Snapshot.State, selection.Snapshot.TotalWeightedSupportMilli = spot.ResolverStateConfident, 1000
			if reversed {
				selection.Snapshot.CandidateRanks[0], selection.Snapshot.CandidateRanks[1] = selection.Snapshot.CandidateRanks[1], selection.Snapshot.CandidateRanks[0]
			}
			before := telnetFamilyState(t, s, 1)
			telnetFamilyTestHandoff(t, s, spot.NewSpot("N2WQ", "W0TEST", 7010, "CW"), cfg, now.Add(2*time.Second), familyLanes{true, true, true},
				selection, false, familyLanes{slow: true})
			if after := telnetFamilyState(t, s, 1); !reflect.DeepEqual(before, after) {
				t.Fatal("refused remaining lane changed either literal witness")
			}
		})
	}
}

func TestTelnetFamilySuppressorRepeatedCallRechecksRivals(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1AB", 7010, "CW", cfg, now, all)
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now.Add(time.Second), all)
	before := telnetFamilyState(t, s, 2)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(2*time.Second), all,
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), true, familyLanes{})
	if after := telnetFamilyState(t, s, 2); !reflect.DeepEqual(before, after) {
		t.Fatal("hidden repeated call changed retained history")
	}
}

func TestTelnetFamilySuppressorKeepsCompetitorsUntilExpiryOrCap(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1AB", 7010, "CW", cfg, now, all)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1ABC", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
		telnetFamilyRawSelection("W1AB", 1, "W1ABC", 3), true, all)
	before := telnetFamilyState(t, s, 2)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1ABC", "W0TEST", 7010, "CW"), cfg, now.Add(2*time.Second), all,
		telnetFamilyRawSelection("W1AB", 3, "W1ABC", 1), true, familyLanes{})
	if after := telnetFamilyState(t, s, 2); !reflect.DeepEqual(before, after) {
		t.Fatal("support reversal lost competing history")
	}
}

func TestTelnetFamilySuppressorAllRivalsBeforeMutation(t *testing.T) {
	orders := [][]string{
		{"WA1AB", "A1ABC", "ZWA1ABC"}, {"WA1AB", "ZWA1ABC", "A1ABC"},
		{"A1ABC", "WA1AB", "ZWA1ABC"}, {"A1ABC", "ZWA1ABC", "WA1AB"},
		{"ZWA1ABC", "WA1AB", "A1ABC"}, {"ZWA1ABC", "A1ABC", "WA1AB"},
	}
	for _, unavailable := range []bool{false, true} {
		for index, order := range orders {
			t.Run(fmt.Sprintf("missing_rival_%t_order_%d", unavailable, index), func(t *testing.T) {
				s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
				all := familyLanes{true, true, true}
				for i, call := range order {
					telnetFamilyAdmit(t, s, call, 7010, "CW", cfg, now.Add(time.Duration(i)*time.Second), all)
				}
				selection := telnetFamilyTestSelection(13,
					spot.ResolverCandidateSupport{Call: "WA1AB", Support: 6},
					spot.ResolverCandidateSupport{Call: "A1ABC", Support: 1},
					spot.ResolverCandidateSupport{Call: "WA1ABC", Support: 3})
				if !unavailable {
					selection.Snapshot.CandidateRanks = append(selection.Snapshot.CandidateRanks, spot.ResolverCandidateSupport{Call: "ZWA1ABC", Support: 3})
				}
				before := telnetFamilyState(t, s, 3)
				for attempt := 0; attempt < 8; attempt++ {
					telnetFamilyTestHandoff(t, s, spot.NewSpot("WA1ABC", "W0TEST", 7010, "CW"), cfg, now.Add(3*time.Second), all, selection, true, familyLanes{})
				}
				if after := telnetFamilyState(t, s, 3); !reflect.DeepEqual(before, after) {
					t.Fatal("comparison mutated history")
				}
			})
		}
	}
}

func TestTelnetFamilySuppressorFailedAdmissionDoesNotRefresh(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, all)
	telnetFamilyAdmit(t, s, "K9XYZ", 7020, "CW", cfg, now.Add(time.Second), all)
	before := telnetFamilyState(t, s, 2)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1ABC", "W0TEST", 7010.1, "CW"), cfg, now.Add(20*time.Second), familyLanes{fast: true},
		correctionflow.ResolverPrimarySelection{}, false, familyLanes{fast: true})
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(21*time.Second), all,
		telnetFamilyRawSelection("W1ABC", 1, "W1AB", 3), false, all)
	if after := telnetFamilyState(t, s, 2); !reflect.DeepEqual(before, after) {
		t.Fatal("refused handoff inserted, refreshed, promoted, or removed history")
	}
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(30*time.Second+time.Nanosecond), all,
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, all)
	telnetFamilyState(t, s, 1)
}

func TestTelnetFamilySuppressorPartialMaskAndNoOrdinaryLane(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, familyLanes{fast: true})
	selection := telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all, selection, true, familyLanes{med: true, slow: true})
	found := false
	for _, entry := range telnetFamilyState(t, s, 2).entries {
		if entry.lanes[1].call == "W1AB" {
			found = true
			if entry.lanes[0].call != "" || entry.lanes[2].call != "W1AB" {
				t.Fatalf("partial admission crossed lanes: %+v", entry.lanes)
			}
		}
	}
	if !found {
		t.Fatal("successful partial admission did not retain its witness")
	}
	before := telnetFamilyState(t, s, 2)
	selected := 0
	got := s.handoff(spot.NewSpot("WA1ABC", "W0TEST", 7010, "CW"), cfg, now.Add(2*time.Second), familyLanes{},
		func(time.Time) correctionflow.ResolverPrimarySelection { selected++; return selection },
		func(familyLanes) bool { t.Error("no-ordinary-lane handoff published"); return true })
	if got != (familyLanes{}) || selected != 0 || !reflect.DeepEqual(before, telnetFamilyState(t, s, 2)) {
		t.Fatal("no ordinary lane selected evidence or taught history")
	}
}

func TestTelnetFamilySuppressorFrequencyAndModeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, firstMode, incomingMode                  string
		firstFrequency, incomingFrequency, toleranceHz float64
		hidden                                         bool
	}{
		{"different_bucket", "CW", "CW", 7010, 7012, 500, false},
		{"adjacent_exact_tolerance", "CW", "CW", 7010.5, 7011.5, 1000, true},
		{"adjacent_beyond_tolerance", "CW", "CW", 7010.5, 7011.6, 1000, false},
		{"independent_mode", "CW", "RTTY", 7010, 7010, 500, false},
		{"RTTY", "RTTY", "RTTY", 7010, 7010.4, 500, true},
		{"USB_voice_tolerance", "USB", "USB", 14200, 14201.5, 500, true},
		{"LSB_voice_tolerance", "LSB", "LSB", 7100, 7101.5, 500, true},
		{"unsupported_FT8", "FT8", "FT8", 7010, 7010, 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			cfg.FrequencyToleranceHz = tc.toleranceHz
			all, want := familyLanes{true, true, true}, familyLanes{true, true, true}
			if tc.hidden {
				want = familyLanes{}
			}
			telnetFamilyAdmit(t, s, "W1ABC", tc.firstFrequency, tc.firstMode, cfg, now, all)
			telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", tc.incomingFrequency, tc.incomingMode), cfg, now.Add(time.Second), all,
				telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), true, want)
			if tc.incomingMode == "FT8" {
				telnetFamilyState(t, s, 0)
			}
		})
	}
}

func TestTelnetFamilySuppressorConfiguredRelationships(t *testing.T) {
	for _, tc := range []struct {
		name, first, incoming string
		alter                 func(*telnetFamilySuppressor, *config.CallCorrectionConfig)
		hidden                bool
	}{
		{"truncation_disabled", "W1ABC", "W1AB", func(s *telnetFamilySuppressor, _ *config.CallCorrectionConfig) { s.family.TruncationEnabled = false }, false},
		{"prefix_disabled", "WA1ABC", "WA1AB", func(s *telnetFamilySuppressor, _ *config.CallCorrectionConfig) {
			s.family.TruncationAllowPrefix = false
		}, false},
		{"suffix_disabled", "WA1ABC", "A1ABC", func(s *telnetFamilySuppressor, _ *config.CallCorrectionConfig) {
			s.family.TruncationAllowSuffix = false
		}, false},
		{"delta_exceeded", "WA1ABC", "WA1A", func(*telnetFamilySuppressor, *config.CallCorrectionConfig) {}, false},
		{"shorter_minimum", "W1ABC", "W1AB", func(s *telnetFamilySuppressor, _ *config.CallCorrectionConfig) {
			s.family.TruncationMinShorterLength = 5
		}, false},
		{"edit_disabled", "K1ABC", "K1ABD", func(*telnetFamilySuppressor, *config.CallCorrectionConfig) {}, false},
		{"edit_enabled", "K1ABC", "K1ABD", func(_ *telnetFamilySuppressor, cfg *config.CallCorrectionConfig) {
			cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled = true
		}, true},
		{"fallback_tolerance", "W1ABC", "W1AB", func(_ *telnetFamilySuppressor, cfg *config.CallCorrectionConfig) { cfg.FrequencyToleranceHz = 0 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
			tc.alter(s, &cfg)
			all, want := familyLanes{true, true, true}, familyLanes{true, true, true}
			if tc.hidden {
				want = familyLanes{}
			}
			telnetFamilyAdmit(t, s, tc.first, 7010, "CW", cfg, now, all)
			telnetFamilyTestHandoff(t, s, spot.NewSpot(tc.incoming, "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
				telnetFamilyRawSelection(tc.first, 3, tc.incoming, 1), true, want)
		})
	}
}

func TestTelnetFamilySuppressorLRUCapacityAndChurn(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	s.maxEntries = 2
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, all)
	telnetFamilyAdmit(t, s, "K9XYZ", 7020, "CW", cfg, now.Add(time.Second), all)
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now.Add(2*time.Second), familyLanes{fast: true})
	telnetFamilyAdmit(t, s, "N2AAA", 7030, "CW", cfg, now.Add(3*time.Second), all)
	for _, entry := range telnetFamilyState(t, s, 2).entries {
		if entry.lanes[0].call == "K9XYZ" {
			t.Fatal("successful refresh failed LRU promotion")
		}
	}
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(4*time.Second), all,
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, familyLanes{})
	for i := 0; i < 128; i++ {
		telnetFamilyAdmit(t, s, fmt.Sprintf("K%dXYZ", i+10), 7040+float64(i), "CW", cfg, now.Add(5*time.Second+time.Duration(i)*time.Millisecond), all)
		telnetFamilyState(t, s, 2)
	}
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(6*time.Second), all,
		telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1), false, all)
	telnetFamilyTestHandoff(t, s, spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Minute), all,
		correctionflow.ResolverPrimarySelection{}, false, all)
	telnetFamilyState(t, s, 0)
	telnetFamilyAdmit(t, s, "N3NEW", 7010, "CW", cfg, now.Add(time.Minute+time.Second), all)
	telnetFamilyState(t, s, 1)
}

func TestTelnetFamilySuppressorSelectionAdmissionSerialization(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	publishing, release := make(chan struct{}), make(chan struct{})
	firstDone, secondDone := make(chan familyLanes, 1), make(chan familyLanes, 1)
	go func() {
		firstDone <- s.handoff(spot.NewSpot("W1ABC", "W0TEST", 7010, "CW"), cfg, now, all,
			func(time.Time) correctionflow.ResolverPrimarySelection {
				return correctionflow.ResolverPrimarySelection{}
			},
			func(familyLanes) bool { close(publishing); <-release; return true })
	}()
	<-publishing
	go func() {
		secondDone <- s.handoff(spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
			func(time.Time) correctionflow.ResolverPrimarySelection {
				return telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1)
			},
			func(familyLanes) bool {
				t.Error("competing handoff published despite admitted protection")
				return true
			})
	}()
	close(release)
	if got := <-firstDone; got != all {
		t.Fatalf("first mask=%+v", got)
	}
	if got := <-secondDone; got != (familyLanes{}) {
		t.Fatalf("second mask=%+v", got)
	}
	telnetFamilyState(t, s, 1)
}

func TestTelnetFamilySuppressorConcurrentAdmissionAndCap(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	s.maxEntries = 8
	var inPublisher int32
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 40; i++ {
				lanes := familyLanes{fast: worker%3 == 0, med: worker%3 == 1, slow: worker%3 == 2}
				sp := spot.NewSpot(fmt.Sprintf("K%dX%03d", worker, i), "W0TEST", 7010, "CW")
				got := s.handoff(sp, cfg, now, lanes,
					func(time.Time) correctionflow.ResolverPrimarySelection {
						return correctionflow.ResolverPrimarySelection{}
					},
					func(familyLanes) bool {
						if active := atomic.AddInt32(&inPublisher, 1); active != 1 {
							t.Errorf("concurrent publishers=%d", active)
						}
						defer atomic.AddInt32(&inPublisher, -1)
						return i%4 != 0
					})
				if got != lanes {
					t.Errorf("attempt=%+v want %+v", got, lanes)
				}
			}
		}(worker)
	}
	workers.Wait()
	for _, entry := range telnetFamilyState(t, s, 8).entries {
		for _, lane := range entry.lanes {
			if lane.call != "" {
				var worker, index int
				if _, err := fmt.Sscanf(lane.call, "K%dX%d", &worker, &index); err != nil || index%4 == 0 {
					t.Fatalf("refused witness retained: %q", lane.call)
				}
			}
		}
	}
}

func TestTelnetFamilySuppressorOneSelectionPerArbitration(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	telnetFamilyAdmit(t, s, "W1ABC", 7010, "CW", cfg, now, familyLanes{fast: true, med: true})
	selected, published := 0, 0
	got := s.handoff(spot.NewSpot("W1AB", "W0TEST", 7010, "CW"), cfg, now.Add(time.Second), all,
		func(at time.Time) correctionflow.ResolverPrimarySelection {
			selected++
			if !at.Equal(now.Add(time.Second)) {
				t.Errorf("selection clock=%s", at)
			}
			return telnetFamilyRawSelection("W1ABC", 3, "W1AB", 1)
		}, func(lanes familyLanes) bool {
			published++
			if lanes != (familyLanes{slow: true}) || s.totalEntries != 1 {
				t.Errorf("pre-admission mask/history=%+v/%d", lanes, s.totalEntries)
			}
			return false
		})
	if got != (familyLanes{slow: true}) || selected != 1 || published != 1 {
		t.Fatalf("mask=%+v selections=%d publications=%d", got, selected, published)
	}
	telnetFamilyState(t, s, 1)
}

// Substrings can retain a complete transport input; admitted compact witnesses
// must own their key, literal and mode rather than keep that backing allocation.
func TestTelnetFamilySuppressorOwnsAdmittedStrings(t *testing.T) {
	s, cfg, now := newTestTelnetFamilySuppressor(), telnetFamilyTestConfig(), time.Unix(1700000000, 0).UTC()
	backing := strings.Repeat("x", 8192) + "N2WQ" + "CW" + strings.Repeat("y", 8192)
	call, mode := backing[8192:8196], backing[8196:8198]
	sp := &spot.Spot{DXCall: call, DXCallNorm: call, Mode: mode, ModeNorm: mode, Frequency: 7010}
	telnetFamilyTestHandoff(t, s, sp, cfg, now, familyLanes{fast: true}, correctionflow.ResolverPrimarySelection{}, true, familyLanes{fast: true})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totalEntries != 1 {
		t.Fatalf("entries=%d want1", s.totalEntries)
	}
	entry := s.head
	if entry.key != "N2WQ" || entry.bucket.mode != "CW" || entry.lanes[0].call != "N2WQ" {
		t.Fatal("owned strings changed identity")
	}
	if unsafe.StringData(entry.key) == unsafe.StringData(call) || unsafe.StringData(entry.lanes[0].call) == unsafe.StringData(call) || unsafe.StringData(entry.bucket.mode) == unsafe.StringData(mode) {
		t.Fatal("admitted witness retained borrowed substring backing")
	}
}
