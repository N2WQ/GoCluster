package spot

import (
	"fmt"
	"testing"
	"time"
)

func TestResolverLossWindowControlsRecoverCoherentCurrentCounts(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{})
	for i, age := range []time.Duration{100 * time.Second, 50 * time.Second, 10 * time.Second, 5 * time.Second} {
		lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i+1), at.Add(-age), 2*time.Minute)
	}
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B1", at, 2*time.Minute)
	settledAt := at.Add(time.Second)
	d.Sweep(settledAt)
	before := lossTestSnapshot(t, r, key)
	if before.Winner != "K1ABC" || before.WinnerSupport != 4 || before.TotalReporters != 5 || before.EvidenceLoss {
		t.Fatalf("ordinary source %+v", before)
	}
	st := d.states[key]
	lastEval, nextEval, pending := st.lastEvalAt, st.nextEvalAt, st.pendingWins
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A5", settledAt.Add(time.Millisecond), 2*time.Minute)
	active := lossTestSnapshot(t, r, key)
	if !active.EvidenceLoss || active.lossMinAt != at.Add(-100*time.Second) {
		t.Fatalf("loss admission %+v", active)
	}
	for _, tc := range []struct {
		offset time.Duration
		window time.Duration
		loss   bool
	}{
		{2 * time.Millisecond, 20 * time.Second, false},
		{3 * time.Millisecond, 2 * time.Minute, true},
		{4 * time.Millisecond, 20 * time.Second, false},
	} {
		query := settledAt.Add(tc.offset)
		lossTestAdmit(t, r, d, key, "K1ABC", "N0A5", query, tc.window)
		s := lossTestSnapshot(t, r, key)
		if s.Generation != before.Generation || s.EvidenceLoss != tc.loss || s.Winner != "K1ABC" || s.State != ResolverStateConfident || s.TotalReporters != 4 || s.TotalWeightedSupportMilli != 4000 || s.WinnerSupport != 3 || s.WinnerWeightedSupportMilli != 3000 || s.ReceiverEvidence().Len() != 4 || s.ReceiverEvidence().RecencyWindow() != tc.window {
			t.Fatalf("window control stale/incoherent %+v", s)
		}
		lossTestRank(t, s, "K1ABC", 3, 3000)
		lossTestRank(t, s, "K1ABD", 1, 1000)
		eligible, ok := r.LookupEligible(key, query)
		if !ok || eligible.EvidenceLoss != tc.loss || eligible.WinnerSupport != 3 || eligible.TotalWeightedSupportMilli != 4000 || eligible.Winner != "K1ABC" {
			t.Fatalf("all-fresh projection reused old counts %+v / %v", eligible, ok)
		}
	}
	if !st.lastEvalAt.Equal(lastEval) || !st.nextEvalAt.Equal(nextEval) || st.pendingWins != pending {
		t.Fatal("window controls advanced owner authority")
	}
	if row, exists := lossTestRow(before, "K1ABC", "N0A1"); !exists || row.ObservedAt != at.Add(-100*time.Second) || before.TotalReporters != 5 || before.WinnerSupport != 4 {
		t.Fatal("prior healthy population mutated")
	}
}

func TestResolverFutureLossControlRestoresOnlyLastOrdinaryIdentity(t *testing.T) {
	weights := SpotterReliability{}
	for i := 1; i <= 4; i++ {
		weights[fmt.Sprintf("N0A%d", i)] = 0.5
		weights[fmt.Sprintf("N0D%d", i)] = 0.1
	}
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{SpotterReliability: weights})
	for i := 1; i <= 4; i++ {
		lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i), at, 2*time.Hour)
	}
	future := at.Add(time.Hour)
	for i := 1; i <= 4; i++ {
		lossTestAdmit(t, r, d, key, "K1ABF", fmt.Sprintf("N0D%d", i), future, 2*time.Hour)
	}
	d.Sweep(future.Add(time.Second))
	ordinary := lossTestSnapshot(t, r, key)
	if ordinary.Winner != "K1ABC" || ordinary.State != ResolverStateConfident || ordinary.WinnerSupport != 4 || ordinary.TotalReporters != 8 || ordinary.TotalWeightedSupportMilli != 2400 {
		t.Fatalf("future ordinary source %+v", ordinary)
	}
	st := d.states[key]
	nextEval, lastEval := st.nextEvalAt, st.lastEvalAt
	// New, stronger future-only E is not evaluated ordinarily. Its fifth
	// receiver causes cap loss and a complete control, whose raw top is E.
	controlAt := future.Add(time.Second + time.Millisecond)
	for i := 1; i <= 5; i++ {
		lossTestAdmit(t, r, d, key, "K1ABE", fmt.Sprintf("N0E%d", i), controlAt, 2*time.Hour)
	}
	control := lossTestSnapshot(t, r, key)
	if !control.EvidenceLoss || control.Winner != "" || control.State != ResolverStateUncertain || control.CandidateRanks[0].Call != "K1ABE" || control.TotalReporters != 12 || control.TotalWeightedSupportMilli != 6400 || control.ordinaryWinner != "K1ABC" {
		t.Fatalf("future control fabricated authority %+v", control)
	}
	for i := 6; i <= 8; i++ {
		lossTestAdmit(t, r, d, key, "K1ABE", fmt.Sprintf("N0E%d", i), controlAt, 2*time.Hour)
		projected, ok := r.LookupEligible(key, at)
		if !ok || projected.EvidenceLoss || projected.Generation != ordinary.Generation || projected.Winner != "K1ABC" || projected.State != ResolverStateConfident || projected.WinnerSupport != 4 || projected.TotalReporters != 4 || projected.TotalWeightedSupportMilli != 2000 || projected.ReceiverEvidence().Len() != 4 {
			t.Fatalf("future cadence latched loss/control %+v / %v", projected, ok)
		}
		lossTestRank(t, projected, "K1ABC", 4, 2000)
	}
	if st.nextEvalAt != nextEval || st.lastEvalAt != lastEval || st.pendingWins != 0 {
		t.Fatal("controls advanced ordinary future cadence")
	}
	if row, exists := lossTestRow(control, "K1ABE", "N0E2"); !exists || row.ObservedAt != controlAt || control.TotalReporters != 12 {
		t.Fatal("prior control rows mutated")
	}
	if row, exists := lossTestRow(ordinary, "K1ABF", "N0D1"); !exists || row.ObservedAt != future || row.WeightMilli != 100 {
		t.Fatal("ordinary publication rows changed")
	}
	// A expires under a shorter window. Clearing its retained candidate must
	// clear the hint, so projected current B cannot become a new settled winner.
	currentAt := at.Add(20 * time.Second)
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B1", currentAt, 10*time.Second)
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B2", currentAt, 11*time.Second)
	projected, ok := r.LookupEligible(key, currentAt)
	if !ok || projected.EvidenceLoss || projected.Winner != "" || projected.State != ResolverStateUncertain || projected.TotalReporters != 2 || projected.TotalWeightedSupportMilli != 2000 || projected.ordinaryWinner != "" || st.stableWinner != "" {
		t.Fatalf("removed identity hint authorized B %+v / %v", projected, ok)
	}
	lossTestRank(t, projected, "K1ABD", 2, 2000)
	if st.nextEvalAt != nextEval || st.lastEvalAt != lastEval {
		t.Fatal("hint clearing advanced future evaluation")
	}
}

func TestResolverLossControlsDoNotAdvanceHysteresis(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{HysteresisWindows: 2})
	for i := 1; i <= 3; i++ {
		lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i), at, 2*time.Minute)
	}
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B0", at.Add(-100*time.Second), 2*time.Minute)
	settledAt := at.Add(time.Second)
	d.Sweep(settledAt)
	before := lossTestSnapshot(t, r, key)
	if before.Winner != "K1ABC" || before.State != ResolverStateConfident {
		t.Fatalf("source not settled %+v", before)
	}
	st := d.states[key]
	for i := 1; i <= 4; i++ {
		lossTestAdmit(t, r, d, key, "K1ABD", fmt.Sprintf("N0B%d", i), settledAt.Add(time.Millisecond), 2*time.Minute)
	}
	for i := 2; i <= 5; i++ {
		window := 20 * time.Second
		if i%2 != 0 {
			window = 30 * time.Second
		}
		lossTestAdmit(t, r, d, key, "K1ABD", "N0B4", settledAt.Add(time.Duration(i)*time.Millisecond), window)
		s := lossTestSnapshot(t, r, key)
		if s.EvidenceLoss || s.Winner != "" || s.State != ResolverStateUncertain || s.TotalReporters != 7 || s.CandidateRanks[0].Call != "K1ABD" || st.pendingWins != 0 || st.pendingWinner != "" {
			t.Fatalf("control counted as hysteresis window %+v pending=%s/%d", s, st.pendingWinner, st.pendingWins)
		}
	}
	firstEval := settledAt.Add(500 * time.Millisecond)
	d.Sweep(firstEval)
	first := lossTestSnapshot(t, r, key)
	if first.Winner != "K1ABC" || first.State != ResolverStateUncertain || st.pendingWinner != "K1ABD" || st.pendingWins != 1 {
		t.Fatalf("first genuine window %+v pending=%s/%d", first, st.pendingWinner, st.pendingWins)
	}
	d.Sweep(firstEval.Add(500 * time.Millisecond))
	second := lossTestSnapshot(t, r, key)
	if second.Winner != "K1ABD" || second.State != ResolverStateConfident || st.pendingWins != 0 {
		t.Fatalf("second genuine window did not settle %+v", second)
	}
}

func TestResolverLossControlsPreserveNarrowSplitWhenLossExpires(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxReportersPerCand: 3, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 3})
	for i, age := range []time.Duration{100 * time.Second, 0, 0} {
		lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i+1), at.Add(-age), 2*time.Minute)
	}
	for i := 1; i <= 2; i++ {
		if !r.Enqueue(ResolverEvidence{Key: key, DXCall: "K1ABD", Spotter: fmt.Sprintf("N0B%d", i), ObservedAt: at, FrequencyKHz: 7010.2, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("enqueue narrow conflict")
		}
		d.Drain()
	}
	settledAt := at.Add(time.Second)
	d.Sweep(settledAt)
	before := lossTestSnapshot(t, r, key)
	if before.State != ResolverStateSplit || before.EvidenceLoss {
		t.Fatalf("narrow split source %+v", before)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A4", settledAt.Add(time.Millisecond), 2*time.Minute)
	if s := lossTestSnapshot(t, r, key); !s.EvidenceLoss || s.State != ResolverStateSplit {
		t.Fatalf("loss control erased split %+v", s)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A4", settledAt.Add(2*time.Millisecond), 20*time.Second)
	s, ok := r.LookupEligible(key, settledAt.Add(2*time.Millisecond))
	if !ok || s.EvidenceLoss || s.State != ResolverStateSplit || s.Winner != "" || s.TotalReporters != 5 || s.WinnerSupport != 3 || s.RunnerSupport != 2 || s.Generation != before.Generation {
		t.Fatalf("expired-loss control erased split %+v / %v", s, ok)
	}
}
