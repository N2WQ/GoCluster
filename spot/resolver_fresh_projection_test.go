package spot

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Tests for the approved release projection contract. Owner-backed
// fixtures publish through Enqueue/Drain/Sweep; direct publications are confined
// to explicit immutable-view boundaries, never used as RF truth evidence.
func freshProjectionOwner(t testing.TB, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time) {
	t.Helper()
	if cfg.EvalMinInterval == 0 {
		cfg.EvalMinInterval = time.Nanosecond
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = time.Hour
	}
	if cfg.HysteresisWindows == 0 {
		cfg.HysteresisWindows = 1
	}
	cfg.MaxEditDistance = 6
	r := NewSignalResolver(cfg)
	d, err := NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, d, NewResolverSignalKey(7010.05, "40m", "CW", 500), time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
}

func freshProjectionObserve(t testing.TB, r *SignalResolver, d *SignalResolverDriver, key ResolverSignalKey, call, receiver string, at time.Time, frequency float64, window time.Duration) {
	t.Helper()
	if !r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver, FrequencyKHz: frequency, RecencyWindow: window}) {
		t.Fatal("fresh-projection enqueue rejected")
	}
	d.Drain()
	// Repeated literal timestamps must still publish the final dirty generation.
	// Sweeping only at at+1ns can remain behind the prior next-evaluation clock.
	sweepAt := at.Add(time.Nanosecond)
	if st := d.states[key]; st != nil && st.nextEvalAt.After(sweepAt) {
		sweepAt = st.nextEvalAt
	}
	d.Sweep(sweepAt)
}

func freshProjectionCandidate(t testing.TB, s ResolverSnapshot, call string, count, weight int) {
	t.Helper()
	for _, candidate := range s.CandidateRanks {
		if candidate.Call == call {
			if candidate.Support != count || candidate.WeightedSupportMilli != weight {
				t.Fatalf("%s fresh support=%d/%d; want %d/%d", call, candidate.Support, candidate.WeightedSupportMilli, count, weight)
			}
			return
		}
	}
	if count != 0 || weight != 0 {
		t.Fatalf("fresh ranks omit %s: %+v", call, s)
	}
}

func freshProjectionCannotAuthorize(t testing.TB, s ResolverSnapshot, ok bool) {
	t.Helper()
	if ok && (s.State == ResolverStateConfident || s.State == ResolverStateProbable) {
		t.Fatalf("filtered evidence manufactured settled authority: %+v", s)
	}
}

func TestP3LookupEligibleInclusiveCutoffAndFuture(t *testing.T) {
	r, _, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	// The owner evaluates at input event clocks and can prune earlier rows when
	// a future event arrives. A direct immutable publication isolates the release
	// interval contract from that already-preserved owner behavior.
	view := &ResolverEvidenceView{window: 10 * time.Second, count: 4, candidates: []resolverEvidenceCandidate{{call: "K1ABC", receivers: []ResolverReceiverObservation{
		{Receiver: "N0AAA", ObservedAt: at.Add(-10 * time.Second), FrequencyKHz: 7010.05, WeightMilli: 900},
		{Receiver: "N0BBB", ObservedAt: at.Add(-10*time.Second - time.Nanosecond), FrequencyKHz: 7010.05, WeightMilli: 950},
		{Receiver: "N0CCC", ObservedAt: at, FrequencyKHz: 7010.05, WeightMilli: 1000},
		{Receiver: "N0DDD", ObservedAt: at.Add(time.Nanosecond), FrequencyKHz: 7010.05, WeightMilli: 1000},
	}}}}
	r.publishSnapshot(key, ResolverSnapshot{Key: key, EvaluatedAt: at, State: ResolverStateConfident, Winner: "K1ABC", WinnerSupport: 4, WinnerWeightedSupportMilli: 3850, TotalReporters: 4, TotalWeightedSupportMilli: 3850,
		CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 4, WeightedSupportMilli: 3850}}, receiverEvidence: view})
	s, ok := r.LookupEligible(key, at)
	if !ok || s.Winner != "K1ABC" || s.TotalReporters != 2 || s.TotalWeightedSupportMilli != 1900 {
		t.Fatalf("inclusive/future projection=%+v available=%t; want A2 total 2/1900", s, ok)
	}
	freshProjectionCandidate(t, s, "K1ABC", 2, 1900)
	if s.ReceiverEvidence().Len() != 2 || view.Len() != 4 {
		t.Fatal("projection lost provenance or changed published view")
	}
}

func TestP3LookupEligibleNewestFutureDoesNotResurrectOlder(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, time.Minute)
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at.Add(time.Second), 7010.10, time.Minute)
	captured, ok := r.Lookup(key)
	if !ok || captured.ReceiverEvidence().Len() != 1 {
		t.Fatal("owner did not retain exactly the newest receiver record")
	}
	s, available := r.LookupEligible(key, at)
	freshProjectionCannotAuthorize(t, s, available)
	if s.TotalReporters != 0 || s.TotalWeightedSupportMilli != 0 || len(s.CandidateRanks) != 0 {
		t.Fatalf("future newest row resurrected older support: %+v", s)
	}
	if after, ok := r.Lookup(key); !ok || after.ReceiverEvidence() != captured.ReceiverEvidence() || after.TotalReporters != 1 {
		t.Fatal("fresh lookup mutated the owner publication")
	}
}

func TestP3LookupEligibleWeightedUnionOverlapAndDissent(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{SpotterReliability: SpotterReliability{"N0AAA": .9, "N0BBB": .95, "N0DDD": .8}})
	freshProjectionObserve(t, r, d, key, "K1ABE", "N0DDD", at, 7010.05, 10*time.Second)
	newAt := at.Add(5 * time.Second)
	for _, row := range []struct{ call, receiver string }{
		{"K1ABC", " n0aaa "}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0AAA"}, {"K1ABE", "N0CCC"},
	} {
		freshProjectionObserve(t, r, d, key, row.call, row.receiver, newAt, 7010.05, 10*time.Second)
	}
	published, ok := r.Lookup(key)
	if !ok || published.Winner != "K1ABC" || published.State != ResolverStateProbable {
		t.Fatalf("fixture did not settle A1850 over C1800 before filtering: %+v", published)
	}
	s, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
	if !ok || s.TotalReporters != 3 || s.TotalWeightedSupportMilli != 2850 || s.Winner != "K1ABC" {
		t.Fatalf("weighted overlapping union=%+v available=%t; want total 3/2850 and A", s, ok)
	}
	freshProjectionCandidate(t, s, "K1ABC", 2, 1850)
	freshProjectionCandidate(t, s, "K1ABD", 1, 900)
	freshProjectionCandidate(t, s, "K1ABE", 1, 1000)
	if percent := s.WinnerWeightedSupportMilli * 100 / s.TotalWeightedSupportMilli; percent != 64 {
		t.Fatalf("A share=%d; want literal 64 percent", percent)
	}
}

func TestP3LookupEligiblePreservesReceiverIdentityAndStoredWeight(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{SpotterReliability: SpotterReliability{"N0AAA": .9}})
	for _, receiver := range []string{" n0aaa ", "N0AAA", "N0AAA-1"} {
		freshProjectionObserve(t, r, d, key, "K1ABC", receiver, at, 7010.05, time.Minute)
	}
	s, ok := r.LookupEligible(key, at.Add(time.Second))
	// The existing owner preserves SSIDs as distinct receivers; the configured
	// base call weighs 900 and the separately named SSID has its default 1000.
	if !ok || s.TotalReporters != 2 || s.TotalWeightedSupportMilli != 1900 {
		t.Fatalf("lookup renormalized incidence or weights: %+v available=%t; want 2/1900", s, ok)
	}
}

func TestP3LookupEligibleSplitAndConflictExpiryCannotInventSettledWinner(t *testing.T) {
	for _, kind := range []string{"narrow guard", "equal-time conflict"} {
		t.Run(kind, func(t *testing.T) {
			r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6})
			if kind == "equal-time conflict" {
				freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, 10*time.Second)
				freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.10, 10*time.Second)
			} else {
				for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
					freshProjectionObserve(t, r, d, key, "K1ABC", receiver, at, 7010.05, 10*time.Second)
				}
			}
			for _, receiver := range []string{"N0DDD", "N0EEE"} {
				freshProjectionObserve(t, r, d, key, "K1ABD", receiver, at.Add(5*time.Second), 7010.10, 10*time.Second)
			}
			published, ok := r.Lookup(key)
			if !ok || published.State != ResolverStateSplit || published.Winner != "" {
				t.Fatalf("fixture never established binding split: %+v", published)
			}
			inclusive, ok := r.LookupEligible(key, at.Add(10*time.Second))
			if !ok || inclusive.State != ResolverStateSplit {
				t.Fatalf("inclusive cutoff removed split too soon: %+v available=%t", inclusive, ok)
			}
			filtered, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
			freshProjectionCannotAuthorize(t, filtered, ok)
			if !ok || filtered.State != ResolverStateUncertain || filtered.TotalReporters != 2 || filtered.TotalWeightedSupportMilli != 2000 {
				t.Fatalf("expired split must become fresh uncertain B2: %+v available=%t", filtered, ok)
			}
			freshProjectionCandidate(t, filtered, "K1ABC", 0, 0)
			freshProjectionCandidate(t, filtered, "K1ABD", 2, 2000)
			if after, _ := r.Lookup(key); after.State != ResolverStateSplit || after.ReceiverEvidence() != published.ReceiverEvidence() {
				t.Fatal("read-side projection mutated owner split")
			}
		})
	}
}

func TestP3LookupEligibleExpiryCanCreateFreshNarrowSplit(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6})
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, 10*time.Second)
	for _, receiver := range []string{"N0BBB", "N0CCC", "N0DDD"} {
		freshProjectionObserve(t, r, d, key, "K1ABC", receiver, at.Add(5*time.Second), 7010.05, 10*time.Second)
	}
	for _, receiver := range []string{"N0EEE", "N0FFF"} {
		freshProjectionObserve(t, r, d, key, "K1ABD", receiver, at.Add(5*time.Second), 7010.10, 10*time.Second)
	}
	published, ok := r.Lookup(key)
	if !ok || published.State != ResolverStateConfident || published.Winner != "K1ABC" || published.WinnerSupport != 4 || published.RunnerSupport != 2 {
		t.Fatalf("fixture did not establish nonsplit settled A4/B2: %+v", published)
	}
	s, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
	if !ok || s.State != ResolverStateSplit || s.Winner != "" || s.TotalReporters != 5 || s.TotalWeightedSupportMilli != 5000 {
		t.Fatalf("fresh A3/B2 narrow split was bypassed: %+v available=%t", s, ok)
	}
	freshProjectionCandidate(t, s, "K1ABC", 3, 3000)
	freshProjectionCandidate(t, s, "K1ABD", 2, 2000)
	if after, _ := r.Lookup(key); after.State != ResolverStateConfident || after.WinnerSupport != 4 || after.ReceiverEvidence() != published.ReceiverEvidence() {
		t.Fatal("fresh split was written back into the owner generation")
	}
}

func TestP3LookupEligibleCannotAdvanceOwnerHysteresis(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{HysteresisWindows: 2})
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, 10*time.Second)
	for _, receiver := range []string{"N0BBB", "N0CCC"} {
		freshProjectionObserve(t, r, d, key, "K1ABD", receiver, at.Add(time.Second), 7010.05, 10*time.Second)
	}
	published, ok := r.Lookup(key)
	if !ok || published.State != ResolverStateUncertain || published.Winner != "K1ABC" {
		t.Fatalf("fixture did not publish pending challenger: %+v", published)
	}
	st := d.states[key]
	stable, pending, wins := st.stableWinner, st.pendingWinner, st.pendingWins
	for i := 0; i < 3; i++ {
		projected, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
		freshProjectionCannotAuthorize(t, projected, ok)
		if !ok || projected.State != ResolverStateUncertain || projected.TotalReporters != 2 {
			t.Fatalf("lookup turned pending B2 into settled authority: %+v", projected)
		}
	}
	if st.stableWinner != stable || st.pendingWinner != pending || st.pendingWins != wins {
		t.Fatal("fresh lookup advanced owner hysteresis")
	}
}

func TestP3LookupEligibleExpiredPublishedWinnerCannotAuthorizeRankChange(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		freshProjectionObserve(t, r, d, key, "K1ABC", receiver, at, 7010.05, 10*time.Second)
	}
	for _, receiver := range []string{"N0DDD", "N0EEE"} {
		freshProjectionObserve(t, r, d, key, "K1ABD", receiver, at.Add(5*time.Second), 7010.05, 10*time.Second)
	}
	published, _ := r.Lookup(key)
	if published.Winner != "K1ABC" || published.State != ResolverStateConfident {
		t.Fatalf("fixture did not settle A3/B2: %+v", published)
	}
	s, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
	freshProjectionCannotAuthorize(t, s, ok)
	if !ok || s.TotalReporters != 2 || s.State != ResolverStateUncertain {
		t.Fatalf("expired winner produced nonconservative projection: %+v", s)
	}
	freshProjectionCandidate(t, s, "K1ABC", 0, 0)
	freshProjectionCandidate(t, s, "K1ABD", 2, 2000)
}

func TestP3LookupEligibleStillSettledTopCanRetainAuthority(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	freshProjectionObserve(t, r, d, key, "K1ABD", "N0AAA", at, 7010.05, 10*time.Second)
	for _, receiver := range []string{"N0BBB", "N0CCC", "N0DDD"} {
		freshProjectionObserve(t, r, d, key, "K1ABC", receiver, at.Add(5*time.Second), 7010.05, 10*time.Second)
	}
	s, ok := r.LookupEligible(key, at.Add(10*time.Second+time.Nanosecond))
	if !ok || s.State != ResolverStateConfident || s.Winner != "K1ABC" || s.WinnerSupport != 3 || s.TotalReporters != 3 || s.TotalWeightedSupportMilli != 3000 {
		t.Fatalf("still-settled A lost or overstated authority: %+v available=%t", s, ok)
	}
}

func TestP3LookupEligibleFilteredTieRetainsPublishedOrder(t *testing.T) {
	r, _, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	// Published ranks preserve the owner's chosen tie order (including its
	// confusion model). View order is lexical and cannot replace that provenance.
	view := &ResolverEvidenceView{window: time.Second, count: 3, candidates: []resolverEvidenceCandidate{
		{call: "K1ABC", receivers: []ResolverReceiverObservation{{Receiver: "N0AAA", ObservedAt: at, FrequencyKHz: 7010.05, WeightMilli: 1000}}},
		{call: "K1ABD", receivers: []ResolverReceiverObservation{{Receiver: "N0BBB", ObservedAt: at, FrequencyKHz: 7010.05, WeightMilli: 1000}}},
		{call: "K1ABE", receivers: []ResolverReceiverObservation{{Receiver: "N0CCC", ObservedAt: at.Add(-2 * time.Second), FrequencyKHz: 7010.05, WeightMilli: 1000}}},
	}}
	ranks := []ResolverCandidateSupport{{Call: "K1ABD", Support: 1, WeightedSupportMilli: 1000}, {Call: "K1ABC", Support: 1, WeightedSupportMilli: 1000}, {Call: "K1ABE", Support: 1, WeightedSupportMilli: 1000}}
	r.publishSnapshot(key, ResolverSnapshot{Key: key, EvaluatedAt: at, State: ResolverStateProbable, Winner: "K1ABD", RunnerUp: "K1ABC", WinnerSupport: 1, RunnerSupport: 1, TotalReporters: 3,
		WinnerWeightedSupportMilli: 1000, RunnerWeightedSupportMilli: 1000, TotalWeightedSupportMilli: 3000, CandidateRanks: ranks, receiverEvidence: view})
	s, ok := r.LookupEligible(key, at)
	if !ok || len(s.CandidateRanks) != 2 || s.CandidateRanks[0].Call != "K1ABD" || s.Winner != "K1ABD" || s.TotalReporters != 2 {
		t.Fatalf("filtered projection invented a new tied winner: %+v", s)
	}
	if !reflect.DeepEqual(ranks, []ResolverCandidateSupport{{Call: "K1ABD", Support: 1, WeightedSupportMilli: 1000}, {Call: "K1ABC", Support: 1, WeightedSupportMilli: 1000}, {Call: "K1ABE", Support: 1, WeightedSupportMilli: 1000}}) {
		t.Fatal("projection sorted published ranks in place")
	}
}

func TestP3LookupEligibleAllFreshZeroAllocationAndConcurrentImmutability(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, time.Minute)
	old, _ := r.Lookup(key)
	beforeRanks := append([]ResolverCandidateSupport(nil), old.CandidateRanks...)
	clock := at.Add(time.Second)
	if allocations := testing.AllocsPerRun(100, func() { r.LookupEligible(key, clock) }); allocations != 0 {
		t.Fatalf("unchanged eligible projection allocates: %g", allocations)
	}
	s, ok := r.LookupEligible(key, clock)
	if !ok || s.ReceiverEvidence() != old.ReceiverEvidence() || s.Winner != old.Winner || s.TotalWeightedSupportMilli != old.TotalWeightedSupportMilli {
		t.Fatal("unchanged eligible projection failed immutable reuse")
	}
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 200; i++ {
				fresh, available := r.LookupEligible(key, clock)
				if !available || fresh.TotalReporters < 1 || fresh.TotalReporters > 64 {
					t.Errorf("concurrent publication projection invalid: %+v", fresh)
					return
				}
				fresh.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool {
					row.FrequencyKHz = -1 // Callback values cannot mutate a generation.
					return true
				})
			}
		}()
	}
	for i := 0; i < 32; i++ {
		freshProjectionObserve(t, r, d, key, "K1ABC", fmt.Sprintf("N0%03d", i), at.Add(time.Duration(i+1)*time.Nanosecond), 7010.05, time.Minute)
	}
	readers.Wait()
	if old.ReceiverEvidence().Len() != 1 || !reflect.DeepEqual(old.CandidateRanks, beforeRanks) {
		t.Fatal("held old generation changed during projection/writer churn")
	}
	old.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool {
		if row.Receiver != "N0AAA" || row.FrequencyKHz != 7010.05 || row.WeightMilli != 1000 || !row.ObservedAt.Equal(at) {
			t.Errorf("held old generation row changed: %+v", row)
		}
		return true
	})
}

func TestP3LookupEligibleCompactPublicationFailsClosed(t *testing.T) {
	r, d, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	freshProjectionObserve(t, r, d, key, "K1ABC", "N0AAA", at, 7010.05, time.Minute)
	published, _ := r.Lookup(key)
	r.publishSnapshot(key, published.Compact())
	s, ok := r.LookupEligible(key, at)
	freshProjectionCannotAuthorize(t, s, ok)
	if s.TotalReporters != 0 || s.TotalWeightedSupportMilli != 0 || len(s.CandidateRanks) != 0 {
		t.Fatalf("compact saved tallies became fresh authority: %+v", s)
	}
}

func TestP3LookupEligibleNilAndAbsentFailClosed(t *testing.T) {
	r, _, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	for _, resolver := range []*SignalResolver{nil, r} {
		s, ok := resolver.LookupEligible(key, at)
		freshProjectionCannotAuthorize(t, s, ok)
		if ok || s.TotalReporters != 0 || s.TotalWeightedSupportMilli != 0 || len(s.CandidateRanks) != 0 {
			t.Fatalf("nil/absent resolver manufactured fresh evidence: %+v available=%t", s, ok)
		}
	}
}

func TestP3LookupEligiblePreservesZeroWeightReceivers(t *testing.T) {
	r, _, key, at := freshProjectionOwner(t, SignalResolverConfig{})
	view := &ResolverEvidenceView{window: time.Minute, count: 2, candidates: []resolverEvidenceCandidate{{call: "K1ABC", receivers: []ResolverReceiverObservation{
		{Receiver: "N0AAA", ObservedAt: at, FrequencyKHz: 7010, WeightMilli: 0},
		{Receiver: "N0BBB", ObservedAt: at.Add(-2 * time.Minute), FrequencyKHz: 7010, WeightMilli: 1000},
	}}}}
	r.publishSnapshot(key, ResolverSnapshot{Key: key, EvaluatedAt: at, State: ResolverStateUncertain, Winner: "K1ABC", TotalReporters: 2, TotalWeightedSupportMilli: 1000, CandidateRanks: []ResolverCandidateSupport{{Call: "K1ABC", Support: 2, WeightedSupportMilli: 1000}}, receiverEvidence: view})
	s, ok := r.LookupEligible(key, at)
	if !ok || s.TotalReporters != 1 || s.TotalWeightedSupportMilli != 0 || s.State != ResolverStateUncertain {
		t.Fatalf("eligible zero-weight receiver lost: %+v", s)
	}
	freshProjectionCandidate(t, s, "K1ABC", 1, 0)
}
