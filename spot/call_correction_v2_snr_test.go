package spot

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestCCV2ReceiverSNRJoinPermutations(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		name   string
		values []resolverReporterObservation
		want   resolverReporterObservation
	}{
		{"numeric_conflict", []resolverReporterObservation{{observedAt: at, frequencyKHz: 7010, report: 3, hasReport: true}, {observedAt: at, frequencyKHz: 7010, report: 18, hasReport: true}, {observedAt: at, frequencyKHz: 7010, report: 3, hasReport: true}}, resolverReporterObservation{observedAt: at, frequencyKHz: 7010, snrConflict: true}},
		{"presence_conflict", []resolverReporterObservation{{observedAt: at, frequencyKHz: 7010, report: 3, hasReport: true}, {observedAt: at, frequencyKHz: 7010, report: 99}, {observedAt: at, frequencyKHz: 7010}}, resolverReporterObservation{observedAt: at, frequencyKHz: 7010, snrConflict: true}},
		{"both_missing", []resolverReporterObservation{{observedAt: at, frequencyKHz: 7010, report: 3}, {observedAt: at, frequencyKHz: 7010, report: 99}}, resolverReporterObservation{observedAt: at, frequencyKHz: 7010}},
		{"newer_resolves", []resolverReporterObservation{{observedAt: at, frequencyKHz: 7010, report: 3, hasReport: true}, {observedAt: at, frequencyKHz: 7010, report: 18, hasReport: true}, {observedAt: at.Add(time.Second), frequencyKHz: 7010.1, report: 0, hasReport: true}}, resolverReporterObservation{observedAt: at.Add(time.Second), frequencyKHz: 7010.1, report: 0, hasReport: true}},
		{"independent_frequency_conflict", []resolverReporterObservation{{observedAt: at, frequencyKHz: 7010, report: 0, hasReport: true}, {observedAt: at, frequencyKHz: 7010.1, report: 0, hasReport: true}}, resolverReporterObservation{observedAt: at, report: 0, hasReport: true, frequencyConflict: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var permutations func(int)
			permutations = func(index int) {
				if index == len(tc.values) {
					got := tc.values[0]
					for _, row := range tc.values[1:] {
						got = joinResolverReporterObservation(got, row)
					}
					if got != tc.want {
						t.Fatalf("join=%+v want %+v order=%+v", got, tc.want, tc.values)
					}
					if repeat := joinResolverReporterObservation(got, got); repeat != got {
						t.Fatal("join is not idempotent")
					}
					return
				}
				for i := index; i < len(tc.values); i++ {
					tc.values[i], tc.values[index] = tc.values[index], tc.values[i]
					permutations(index + 1)
					tc.values[i], tc.values[index] = tc.values[index], tc.values[i]
				}
			}
			permutations(0)
		})
	}
	t.Logf("retained owner row=%d bytes; public row=%d bytes", unsafe.Sizeof(resolverReporterObservation{}), unsafe.Sizeof(ResolverReceiverObservation{}))
}

func TestCCV2ConfusionPerReceiverOracle(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	rows := []ResolverReceiverObservation{
		{Receiver: "N0AAA", Report: 0, HasReport: true, WeightMilli: 900},
		{Receiver: "N0AAB", Report: 10, HasReport: true, WeightMilli: 1000},
		{Receiver: "N0AAC", Report: 0, HasReport: false, WeightMilli: 700},
		{Receiver: "N0AAD", Report: 0, HasReport: true, SNRConflict: true, WeightMilli: 700},
		{Receiver: "N0AAE", Report: 11, HasReport: true, WeightMilli: 1000},
		{Receiver: "N0AAF", Report: 0, HasReport: true, WeightMilli: 0},
	}
	a := rankedResolverCandidate{call: "K1AAA", callRunes: []rune("K1AAA")}
	b := rankedResolverCandidate{call: "K1AAB", callRunes: []rune("K1AAB"), receivers: rows}
	// Literal independent counts: P(B|A,first)=(3+1)/(7+3)=.4;
	// P(B|A,second)=(7+1)/(7+3)=.8. Missing/conflicted/out-of-domain
	// and zero-weight rows supply no optional model term.
	want := 900*math.Log(.4) + 1000*math.Log(.8)
	var workspace confusionAlignmentWorkspace
	for n := 0; n < len(rows); n++ {
		got := resolverConfusionAggregateScore(a, []rankedResolverCandidate{a, b}, "CW", model, &workspace)
		if math.Abs(got-want) > 1e-10 {
			t.Fatalf("receiver reduction=%v want literal %v", got, want)
		}
		// Independent direct scorer recomputes alignment for each eligible row.
		direct := 0.0
		for _, row := range rows {
			if row.HasReport && !row.SNRConflict && row.WeightMilli > 0 {
				direct += float64(row.WeightMilli) * model.ScoreCandidate(b.call, a.call, "CW", float64(row.Report))
			}
		}
		if math.Abs(got-direct) > 1e-10 {
			t.Fatalf("optimized=%v direct=%v", got, direct)
		}
		rows[0], rows[n] = rows[n], rows[0]
	}
	if own := resolverConfusionAggregateScore(b, []rankedResolverCandidate{b}, "CW", model, &workspace); own != 0 {
		t.Fatal("self match contributed edits")
	}
}

func TestCCV2ConfusionTopCohortPopulation(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	a := rankedResolverCandidate{call: "K1AAA", callRunes: []rune("K1AAA"), support: 2, weightedSupportMilli: 2000,
		receivers: []ResolverReceiverObservation{{Report: 0, HasReport: true, WeightMilli: 1000}, {Report: 0, HasReport: true, WeightMilli: 1000}}}
	b := rankedResolverCandidate{call: "K1AAB", callRunes: []rune("K1AAB"), support: 2, weightedSupportMilli: 2000,
		receivers: []ResolverReceiverObservation{{Report: 0, HasReport: true, WeightMilli: 1000}, {Report: 10, HasReport: true, WeightMilli: 1000}}}
	for _, lower := range []rankedResolverCandidate{
		{call: "K1BBB", callRunes: []rune("K1BBB"), support: 1, weightedSupportMilli: 2000},
		{call: "K1BBB", callRunes: []rune("K1BBB"), support: 2, weightedSupportMilli: 1900},
	} {
		lower.receivers = []ResolverReceiverObservation{{Report: 0, HasReport: true, WeightMilli: 1000}}
		ranked := []rankedResolverCandidate{a, b, lower}
		var workspace confusionAlignmentWorkspace
		applyResolverConfusionTieBreak(ranked, "CW", model, 1, &workspace)
		if ranked[0].call != a.call || ranked[2].call != lower.call || ranked[2].confusionScore != 0 {
			t.Fatalf("outside-cohort evidence or hypothesis entered: %+v", ranked)
		}
		if math.Abs(ranked[0].confusionScore-(1000*math.Log(.4)+1000*math.Log(.8))) > 1e-10 || math.Abs(ranked[1].confusionScore-2000*math.Log(.5)) > 1e-10 {
			t.Fatalf("top cohort score includes outside observations: %+v", ranked)
		}
	}
}

func TestCCV2ReceiverSNRPublicationLifecycle(t *testing.T) {
	r, d, key, at := b22SNRPresenceOwner(t)
	first := b22SNRPresenceAdmit(t, r, d, b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 0, true, time.Minute))
	conflict := b22SNRPresenceAdmit(t, r, d, b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 10, true, time.Minute))
	if first.ReceiverEvidence() == conflict.ReceiverEvidence() {
		t.Fatal("SNR-only conflict did not dirty immutable publication")
	}
	var old, new ResolverReceiverObservation
	first.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool { old = row; return true })
	conflict.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool { new = row; return true })
	if !old.HasReport || old.Report != 0 || old.SNRConflict || new.HasReport || !new.SNRConflict || conflict.TotalReporters != 1 {
		t.Fatalf("publication changed history/support: old=%+v new=%+v", old, new)
	}
	repeat := b22SNRPresenceAdmit(t, r, d, b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 0, true, time.Minute))
	if repeat.ReceiverEvidence() != conflict.ReceiverEvidence() {
		t.Fatal("equal-time conflict was not absorbing/idempotent")
	}
	newest := b22SNRPresenceAdmit(t, r, d, b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at.Add(time.Second), 0, true, time.Minute))
	newest.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool {
		if !row.HasReport || row.SNRConflict {
			t.Fatal("strictly newer report did not resolve SNR")
		}
		return true
	})
	if compact := newest.Compact(); compact.ReceiverEvidence() != nil || !reflect.DeepEqual(compact.CandidateRanks, newest.CandidateRanks) {
		t.Fatal("compact retained rows or lost tallies")
	}
	d.Sweep(at.Add(2 * time.Minute))
	if snap, ok := r.Lookup(key); ok && snap.ReceiverEvidence().Len() != 0 {
		t.Fatal("expired receiver SNR outlived owner")
	}
}

func TestCCV2ReceiverSNRCompletedEvidenceRanking(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	key := NewResolverSignalKey(7010, "40m", "CW", 500)
	rows := []ResolverEvidence{
		{ObservedAt: at, Key: key, DXCall: "K1AAA", Spotter: "N0AAA", FrequencyKHz: 7010, Report: 0, HasReport: true, RecencyWindow: time.Minute},
		{ObservedAt: at, Key: key, DXCall: "K1AAA", Spotter: "N0AAB", FrequencyKHz: 7010, Report: 0, HasReport: true, RecencyWindow: time.Minute},
		{ObservedAt: at, Key: key, DXCall: "K1AAB", Spotter: "N0BBB", FrequencyKHz: 7010, Report: 0, HasReport: true, RecencyWindow: time.Minute},
		{ObservedAt: at, Key: key, DXCall: "K1AAB", Spotter: "N0BBC", FrequencyKHz: 7010, Report: 10, HasReport: true, RecencyWindow: time.Minute},
	}
	var expected ResolverSnapshot
	var permute func(int)
	permute = func(index int) {
		if index == len(rows) {
			r := NewSignalResolver(SignalResolverConfig{ConfusionModel: model, ConfusionWeight: 1, HysteresisWindows: 1, EvalMinInterval: time.Millisecond})
			d, err := NewSignalResolverDriver(r)
			if err != nil {
				t.Fatal(err)
			}
			// The evaluation schedule and initial hysteresis state are identical
			// for every permutation; no arbitrary live-emission parity is claimed.
			d.states[key] = &resolverKeyState{key: key, lastSeen: at, nextEvalAt: at.Add(time.Millisecond), recencyWindow: time.Minute, candidates: make(map[string]*resolverCandidate), reporterRefs: make(map[string]resolverReporterRef)}
			for _, row := range rows {
				if !r.Enqueue(row) {
					t.Fatal("receiver input refused")
				}
			}
			if d.Drain() != 4 {
				t.Fatal("owner missed input")
			}
			d.Sweep(at.Add(time.Millisecond))
			got, ok := r.Lookup(key)
			if !ok || got.Winner != "K1AAA" || got.TotalReporters != 4 {
				t.Fatalf("completed evidence rank=%+v", got)
			}
			if expected.Winner == "" {
				expected = got
			} else if got.Winner != expected.Winner || !reflect.DeepEqual(got.CandidateRanks, expected.CandidateRanks) || !reflect.DeepEqual(got.ReceiverEvidence(), expected.ReceiverEvidence()) {
				t.Fatal("arrival order changed completed SNR evidence/rank")
			}
			return
		}
		for i := index; i < len(rows); i++ {
			rows[index], rows[i] = rows[i], rows[index]
			permute(index + 1)
			rows[index], rows[i] = rows[i], rows[index]
		}
	}
	permute(0)
}

func TestCCV2FreshSNRAuthority(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	r, _, key, at := freshProjectionOwner(t, SignalResolverConfig{ConfusionModel: model, ConfusionWeight: 1})
	// The original owner settled A using B's second-band report. After equal
	// removal from A/B, only first-band reports remain and freshly rank B.
	view := &ResolverEvidenceView{window: 10 * time.Second, count: 4, candidates: []resolverEvidenceCandidate{
		{call: "K1AAA", receivers: []ResolverReceiverObservation{{Receiver: "N0AAA", ObservedAt: at, FrequencyKHz: 7010, WeightMilli: 1000, Report: 0, HasReport: true}, {Receiver: "N0AAB", ObservedAt: at.Add(-11 * time.Second), FrequencyKHz: 7010, WeightMilli: 1000, Report: 0, HasReport: true}}},
		{call: "K1AAB", receivers: []ResolverReceiverObservation{{Receiver: "N0BBB", ObservedAt: at, FrequencyKHz: 7010, WeightMilli: 1000, Report: 0, HasReport: true}, {Receiver: "N0BBC", ObservedAt: at.Add(-11 * time.Second), FrequencyKHz: 7010, WeightMilli: 1000, Report: 10, HasReport: true}}},
	}}
	previous := ResolverSnapshot{Key: key, State: ResolverStateProbable, Winner: "K1AAA", WinnerSupport: 2, TotalReporters: 4, TotalWeightedSupportMilli: 4000, receiverEvidence: view, CandidateRanks: []ResolverCandidateSupport{{Call: "K1AAA", Support: 2, WeightedSupportMilli: 2000}, {Call: "K1AAB", Support: 2, WeightedSupportMilli: 2000}}}
	r.publishSnapshot(key, previous)
	fresh, ok := r.LookupEligible(key, at)
	if !ok || fresh.CandidateRanks[0].Call != "K1AAB" || fresh.Winner != "" || fresh.State != ResolverStateUncertain || fresh.TotalReporters != 2 {
		t.Fatalf("fresh model kept obsolete SNR or settled new authority: %+v", fresh)
	}
	if view.Len() != 4 || previous.Winner != "K1AAA" {
		t.Fatal("fresh rerank mutated publication")
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 100 {
				concurrent, available := r.LookupEligible(key, at)
				if !available || concurrent.Winner != "" || !reflect.DeepEqual(concurrent.CandidateRanks, fresh.CandidateRanks) {
					t.Error("concurrent model projection changed immutable/fresh authority")
					return
				}
			}
		})
	}
	readers.Wait()
	previous.Winner = "K1AAB"
	r.publishSnapshot(key, previous)
	retained, ok := r.LookupEligible(key, at)
	if !ok || retained.Winner != "K1AAB" || retained.State != ResolverStateProbable {
		t.Fatalf("freshly leading already settled identity lost authority: %+v", retained)
	}
}
