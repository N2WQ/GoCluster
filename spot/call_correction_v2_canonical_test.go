package spot

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func canonicalEvidence(t testing.TB, r *SignalResolver, d *SignalResolverDriver, key ResolverSignalKey, call, receiver string, at time.Time, report int) {
	t.Helper()
	if !r.Enqueue(ResolverEvidence{Key: key, DXCall: call, Spotter: receiver, ObservedAt: at, FrequencyKHz: 7010, Report: report, HasReport: true, RecencyWindow: time.Minute}) {
		t.Fatal("canonical fixture refused")
	}
	d.Drain()
}

func TestCCV2CanonicalReceiverUnionAndObservedDisplay(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	canonicalEvidence(t, r, d, key, "KH6/W1AW", "N0AAA", at, 3)
	canonicalEvidence(t, r, d, key, "W1AW/KH6", "N0AAA", at, 18)
	canonicalEvidence(t, r, d, key, "W1AW/KH6", "N0BBB", at, 5)
	d.Sweep(at.Add(time.Nanosecond))
	s, ok := r.Lookup(key)
	if !ok || len(s.CandidateRanks) != 1 || s.TotalReporters != 2 || s.Winner != "W1AW/KH6" || s.WinnerSupport != 2 || s.TotalWeightedSupportMilli != 2000 {
		t.Fatalf("canonical union=%+v", s)
	}
	a, ok := s.CandidateForCall("KH6/W1AW")
	if !ok || a.VoteKey != "W1AW/KH6" || a.Call != "W1AW/KH6" || a.Support != 2 || !s.HasObservedCall("KH6/W1AW") {
		t.Fatalf("identity/display metadata=%+v", a)
	}
	conflicts := 0
	s.ReceiverEvidence().RangeCanonical(func(identity, display string, row ResolverReceiverObservation) bool {
		if identity != "W1AW/KH6" || display != "W1AW/KH6" {
			t.Error("canonical identity/display changed")
		}
		if row.Receiver == "N0AAA" {
			if row.HasReport || !row.SNRConflict {
				t.Error("equivalent-spelling SNR conflict retained authority")
			}
			conflicts++
		}
		return true
	})
	if conflicts != 1 || s.ReceiverEvidence().Len() != 3 {
		t.Fatal("lost raw provenance or duplicated receiver")
	}
	compact := s.Compact()
	if compact.ReceiverEvidence() != nil || !compact.HasObservedCall("KH6/W1AW") || !reflect.DeepEqual(compact.CandidateRanks, s.CandidateRanks) {
		t.Fatal("compact lost bounded variant metadata")
	}
}

func TestCCV2CanonicalDisplayAndFinalAliasLifecycle(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{HysteresisWindows: 100, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	canonicalEvidence(t, r, d, key, "KH6/W1AW", "N0AAA", at, 3)
	canonicalEvidence(t, r, d, key, "W1AW/KH6", "N0BBB", at.Add(time.Second), 3)
	d.Sweep(at.Add(time.Second))
	s, _ := r.Lookup(key)
	if s.Winner != "W1AW/KH6" || s.State != ResolverStateConfident || d.states[key].stableWinner != "W1AW/KH6" || d.states[key].pendingWinner != "" {
		t.Fatalf("display change created transition: %+v", s)
	}
	st := d.states[key]
	removeResolverCandidate(st, "W1AW/KH6")
	r.evaluateKey(st, at.Add(2*time.Second))
	s, _ = r.Lookup(key)
	if s.Winner != "KH6/W1AW" || st.stableWinner != "W1AW/KH6" || st.pendingWinner != "" {
		t.Fatalf("single alias removal revoked identity: %+v", s)
	}
	removeResolverCandidate(st, "KH6/W1AW")
	if st.stableWinner != "" || st.pendingWinner != "" || st.ordinaryWinner != "" {
		t.Fatal("final alias left semantic authority")
	}
}

func TestCCV2CanonicalFreshDoesNotResurrectSupersededAlias(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	canonicalEvidence(t, r, d, key, "KH6/W1AW", "N0AAA", at, 3)
	canonicalEvidence(t, r, d, key, "W1AW/KH6", "N0AAA", at.Add(time.Second), 18)
	s, ok := r.LookupEligible(key, at)
	if !ok || s.TotalReporters != 0 || s.Winner != "" || s.State != ResolverStateUncertain {
		t.Fatalf("older spelling resurrected behind future newest: %+v", s)
	}
	latest, ok := r.LookupEligible(key, at.Add(time.Second))
	if !ok || latest.Winner != "W1AW/KH6" || latest.TotalReporters != 1 {
		t.Fatalf("latest alias unavailable: %+v", latest)
	}
}

func TestCCV2CanonicalSlashThresholdKeepsBareDissent(t *testing.T) {
	for _, threshold := range []int{2, 3} {
		r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{SlashPrecedenceMinReports: threshold, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
		for i := 0; i < 4; i++ {
			canonicalEvidence(t, r, d, key, "W1AW", fmt.Sprintf("N0B%02d", i), at, 5)
		}
		canonicalEvidence(t, r, d, key, "KH6/W1AW", "N0AAA", at, 5)
		canonicalEvidence(t, r, d, key, "W1AW/KH6", "N0BBB", at, 5)
		d.Sweep(at.Add(time.Second))
		s, _ := r.Lookup(key)
		bare, _ := s.CandidateForCall("W1AW")
		if s.TotalReporters != 6 || bare.Support != 4 || bare.ReplacementBlocked != (threshold == 2) {
			t.Fatalf("threshold %d lost independent eligibility/dissent: %+v", threshold, s)
		}
		if threshold == 2 && s.Winner != "KH6/W1AW" {
			t.Fatalf("slash union did not select lexical observed display: %+v", s)
		}
		if threshold == 3 && s.Winner != "W1AW" {
			t.Fatalf("below-threshold slash blocked bare: %+v", s)
		}
	}
}

func TestCCV2CanonicalUnionExceedsRawReceiverCap(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 16, MaxReportersPerCand: 64, HysteresisWindows: 1})
	for variant, call := range []string{"KH6/W1AW", "W1AW/KH6"} {
		for i := 0; i < 64; i++ {
			canonicalEvidence(t, r, d, key, call, fmt.Sprintf("R%d%03d", variant, i), at, 5)
		}
	}
	d.Sweep(at.Add(time.Second))
	s, _ := r.Lookup(key)
	if s.TotalReporters != 128 || s.WinnerSupport != 128 || len(d.states[key].candidates) != 2 || s.EvidenceLoss {
		t.Fatalf("canonical group silently recapped: %+v", s)
	}
}

func TestCCV2CanonicalModelSelfContributionIsZero(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	a := rankedResolverCandidate{call: "KH6/W1AW", callRunes: []rune("KH6/W1AW")}
	b := rankedResolverCandidate{call: "W1AW/KH6", callRunes: []rune("W1AW/KH6"), receivers: []ResolverReceiverObservation{{Receiver: "N0AAA", Report: 0, HasReport: true, WeightMilli: 1000}}}
	var workspace confusionAlignmentWorkspace
	if got := resolverConfusionAggregateScore(a, []rankedResolverCandidate{a, b}, "CW", model, &workspace); got != 0 {
		t.Fatalf("canonical self contributed %v", got)
	}
}

func TestCCV2CanonicalModeledSlashEligibilityBoundary(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	ranked := []rankedResolverCandidate{
		{call: "KH6/K1AAA", identity: normalizeCorrectionCallIdentity("KH6/K1AAA"), callRunes: []rune("K1AAA/KH6"), support: 2, weightedSupportMilli: 2000, receivers: []ResolverReceiverObservation{{Report: 0, HasReport: true, WeightMilli: 1000}}},
		{call: "K1AAA", identity: normalizeCorrectionCallIdentity("K1AAA"), callRunes: []rune("K1AAA"), support: 2, weightedSupportMilli: 2000, replacementBlocked: true, receivers: []ResolverReceiverObservation{{Report: 10, HasReport: true, WeightMilli: 1000}}},
	}
	var workspace confusionAlignmentWorkspace
	applyResolverConfusionTieBreak(ranked, "CW", model, 100, &workspace)
	if ranked[0].replacementBlocked || ranked[0].call != "KH6/K1AAA" || ranked[1].confusionScore != 0 {
		t.Fatalf("blocked bare entered model cohort: %+v", ranked)
	}
}

func TestCCV2CanonicalModelNormalizesBothOperands(t *testing.T) {
	model := mustBuildResolverConfusionModelForTieBreak(t)
	var expected float64
	for i, candidate := range []string{"KH6/AA1AA", "AA1AA/KH6"} {
		for j, observed := range []string{"KH6/ZZ1ZZ", "ZZ1ZZ/KH6"} {
			a := rankedResolverCandidate{call: candidate, callRunes: []rune(candidate)}
			b := rankedResolverCandidate{call: observed, callRunes: []rune(observed), receivers: []ResolverReceiverObservation{{Report: 10, HasReport: true, WeightMilli: 1000}}}
			var workspace confusionAlignmentWorkspace
			got := resolverConfusionAggregateScore(a, []rankedResolverCandidate{a, b}, "CW", model, &workspace)
			want := 1000 * model.ScoreCandidate(CorrectionVoteKey(observed), CorrectionVoteKey(candidate), "CW", 10)
			if got != want {
				t.Fatalf("normalized pair (%s,%s)=%v want %v", candidate, observed, got, want)
			}
			if i == 0 && j == 0 {
				expected = got
			} else if got != expected {
				t.Fatal("equivalent permutation changed non-self scoring")
			}
		}
	}
}

func TestCCV2CanonicalRawCapLossAndIdleRetirement(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 16, MaxReportersPerCand: 64, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	variants := func(base string) []string {
		return []string{base + "/A/B/C", base + "/A/C/B", base + "/B/A/C", base + "/B/C/A", base + "/C/A/B", base + "/C/B/A", "A/" + base + "/B/C", "B/" + base + "/A/C", "C/" + base + "/A/B"}
	}
	for group, base := range []string{"W1AW", "K1ABC"} {
		for variant, call := range variants(base)[:8] {
			for rx := 0; rx < 64; rx++ {
				canonicalEvidence(t, r, d, key, call, fmt.Sprintf("R%d%02d%02d", group, variant, rx), at, 5)
			}
		}
	}
	d.Sweep(at.Add(time.Second))
	before, ok := r.Lookup(key)
	if !ok || len(d.states[key].candidates) != 16 || before.TotalReporters != 1024 || len(before.CandidateRanks) != 2 {
		t.Fatalf("raw caps/large groups wrong: %+v", before)
	}
	for _, candidate := range before.CandidateRanks {
		if candidate.Support != 512 {
			t.Fatalf("large group silently recapped: %+v", candidate)
		}
	}
	canonicalEvidence(t, r, d, key, variants("W1AW")[8], "N0NEW", at.Add(2*time.Second), 5)
	lost, ok := r.Lookup(key)
	if !ok || !lost.EvidenceLoss || r.MetricsSnapshot().EvictedCandidates == 0 || len(d.states[key].candidates) != 16 || !lost.Compact().EvidenceLoss {
		t.Fatalf("raw spelling eviction bypassed canonical loss: %+v", lost)
	}
	if len(before.CandidateRanks) != 2 || before.TotalReporters != 1024 {
		t.Fatal("eviction mutated prior publication")
	}
	if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(key); ok {
		t.Fatal("logical idle deadline retained canonical authority")
	}
	d.Sweep(at.Add(2 * time.Second))
	if len(d.states) != 0 || r.MetricsSnapshot().PublishedReceiverRecords != 0 {
		t.Fatal("retirement retained canonical owner")
	}
	canonicalEvidence(t, r, d, key, "KH6/W1AW", "N0NEW", at.Add(3*time.Second), 0)
	fresh, ok := r.Lookup(key)
	if !ok || fresh.Generation == before.Generation || fresh.EvidenceLoss || fresh.WinnerSupport != 1 {
		t.Fatalf("new lifecycle reused lost canonical state: %+v", fresh)
	}
}

func TestCCV2CanonicalMergeImmutableSourceFrequencyConflict(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	view := &ResolverEvidenceView{window: time.Minute, count: 1, candidates: []resolverEvidenceCandidate{{call: "W1AW", receivers: []ResolverReceiverObservation{{Receiver: "N0AAA", ObservedAt: at, WeightMilli: 1000, FrequencyConflict: true}}}}}
	view.canonical = buildCanonicalResolverCandidates(view.candidates)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for i := 0; i < 200; i++ {
			view.RangeCanonical(func(_, _ string, row ResolverReceiverObservation) bool {
				if !row.FrequencyConflict {
					t.Error("source conflict mutated")
				}
				return true
			})
		}
	}()
	for i := 0; i < 200; i++ {
		merged := MergeResolverEvidenceViews(view)
		merged.RangeCanonical(func(_, _ string, row ResolverReceiverObservation) bool {
			if !row.FrequencyConflict {
				t.Error("source conflict lost by merge")
			}
			return true
		})
	}
	readers.Wait()
	if !view.candidates[0].receivers[0].FrequencyConflict || !view.canonical[0].receivers[0].FrequencyConflict {
		t.Fatal("merge changed source generation")
	}
}
