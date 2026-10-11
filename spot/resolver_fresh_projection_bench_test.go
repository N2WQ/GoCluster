package spot

import (
	"fmt"
	"testing"
	"time"
)

func freshProjectionBenchmarkPublication(b testing.TB, candidates, receivers int, expired, partial bool) (*SignalResolver, ResolverSignalKey, time.Time) {
	b.Helper()
	r, _, key, at := freshProjectionOwner(b, SignalResolverConfig{})
	view := &ResolverEvidenceView{window: time.Minute, count: candidates * receivers, candidates: make([]resolverEvidenceCandidate, candidates)}
	ranks := make([]ResolverCandidateSupport, candidates)
	for candidate := range view.candidates {
		call := fmt.Sprintf("K1A%02d", candidate)
		rows := make([]ResolverReceiverObservation, receivers)
		for receiver := range rows {
			rows[receiver] = ResolverReceiverObservation{Receiver: fmt.Sprintf("R%04d", candidate*receivers+receiver), ObservedAt: at, FrequencyKHz: 7010.05, WeightMilli: 1000}
			if partial && receiver%2 == 0 {
				rows[receiver].ObservedAt = at.Add(-2 * time.Minute)
			}
		}
		view.candidates[candidate] = resolverEvidenceCandidate{call: call, receivers: rows}
		ranks[candidate] = ResolverCandidateSupport{Call: call, Support: receivers, WeightedSupportMilli: receivers * 1000}
	}
	state := ResolverStateUncertain
	if candidates == 2 {
		state = ResolverStateProbable // Literal 50 percent for the typical fixture.
	}
	r.publishSnapshot(key, ResolverSnapshot{Key: key, EvaluatedAt: at, State: state, Winner: ranks[0].Call, RunnerUp: ranks[1].Call, WinnerSupport: receivers, RunnerSupport: receivers,
		WinnerWeightedSupportMilli: receivers * 1000, RunnerWeightedSupportMilli: receivers * 1000,
		TotalReporters: candidates * receivers, TotalWeightedSupportMilli: candidates * receivers * 1000, CandidateRanks: ranks, receiverEvidence: view})
	if expired {
		return r, key, at.Add(time.Minute + time.Nanosecond)
	}
	return r, key, at.Add(time.Second)
}

func BenchmarkP3LookupEligibleProjection(b *testing.B) {
	for _, tc := range []struct {
		name                  string
		candidates, receivers int
		expired               bool
		partial               bool
	}{
		{"all_fresh_typical", 2, 3, false, false},
		{"all_fresh_capped", 16, 64, false, false},
		{"all_expired_typical", 2, 3, true, false},
		{"all_expired_capped", 16, 64, true, false},
		{"partially_eligible_typical", 2, 3, false, true},
		{"partially_eligible_capped", 16, 64, false, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			r, key, clock := freshProjectionBenchmarkPublication(b, tc.candidates, tc.receivers, tc.expired, tc.partial)
			s, ok := r.LookupEligible(key, clock)
			if tc.expired {
				freshProjectionCannotAuthorize(b, s, ok)
				if s.TotalReporters != 0 || len(s.CandidateRanks) != 0 {
					b.Fatal("expired benchmark still has evidence")
				}
			} else if tc.partial {
				if !ok || s.TotalReporters != tc.candidates*(tc.receivers/2) || s.ReceiverEvidence().Len() != s.TotalReporters {
					b.Fatal("partial benchmark fixture lost rows")
				}
			} else if !ok || s.TotalReporters != tc.candidates*tc.receivers {
				b.Fatal("all-fresh benchmark fixture lost evidence")
			}
			if !tc.partial && testing.AllocsPerRun(100, func() { r.LookupEligible(key, clock) }) != 0 {
				b.Fatal("all-fresh benchmark misses zero-allocation contract")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.LookupEligible(key, clock)
			}
		})
	}
}
