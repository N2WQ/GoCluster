package spot

import (
	"fmt"
	"testing"
	"time"
)

func TestMethodAmbiguityReportPermutations(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	rows := []ResolverEvidence{
		{ObservedAt: at, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7009.8, RecencyWindow: time.Minute},
		{ObservedAt: at, DXCall: "K1ABC", Spotter: "N0BBB", FrequencyKHz: 7010.20, RecencyWindow: time.Minute},
		{ObservedAt: at, DXCall: "K1ABC", Spotter: "N0CCC", FrequencyKHz: 7010.21, RecencyWindow: time.Minute},
		{ObservedAt: at, DXCall: "K1ABD", Spotter: "N0DDD", FrequencyKHz: 7010.22, RecencyWindow: time.Minute},
		{ObservedAt: at, DXCall: "K1ABD", Spotter: "N0EEE", FrequencyKHz: 7010.23, RecencyWindow: time.Minute},
	}
	var visit func(int)
	count := 0
	visit = func(index int) {
		if index < len(rows) {
			for i := index; i < len(rows); i++ {
				rows[index], rows[i] = rows[i], rows[index]
				visit(index + 1)
				rows[index], rows[i] = rows[i], rows[index]
			}
			return
		}
		r := NewSignalResolver(SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1, FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 6, DistanceModelCW: "morse"})
		d, err := NewSignalResolverDriver(r)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			row.Key = NewResolverSignalKey(row.FrequencyKHz, "40m", "CW", 500)
			if !r.Enqueue(row) {
				t.Fatal("enqueue permutation")
			}
		}
		d.Drain()
		d.Sweep(at.Add(time.Millisecond))
		s, ok := r.Lookup(NewResolverSignalKey(7010, "40m", "CW", 500))
		if !ok || s.State != ResolverStateSplit {
			t.Errorf("permutation %d lost close conflict: %+v", count, s)
		}
		count++
	}
	visit(0)
	if count != 120 {
		t.Fatalf("permutations=%d", count)
	}
}

func TestMethodAmbiguityGuardBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		winner, runner, overlap, distance int
		frequency, ratio                  float64
		related, want                     bool
	}{
		{"narrow", 3, 2, 0, 1, .2, .6, false, true},
		{"wide", 3, 2, 0, 1, .31, .6, false, false},
		{"exact equality", 3, 2, 0, 1, .3, .6, false, false},
		{"equal support", 2, 2, 0, 1, .2, .6, false, true},
		{"weak runner", 2, 1, 0, 1, .2, .6, false, false},
		{"support gap", 4, 2, 0, 1, .2, .6, false, false},
		{"ratio rejects", 3, 2, 0, 1, .2, .8, false, false},
		{"overlap rejects", 3, 2, 1, 1, .2, .6, false, false},
		{"overlap equality", 5, 4, 1, 1, .2, .6, false, true},
		{"distance rejects", 3, 2, 0, 4, .2, .6, false, false},
		{"family exclusion", 3, 2, 0, 1, .2, .6, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRejectAsAmbiguousMultiSignal(tc.winner, tc.runner, 0, tc.frequency, .3, tc.ratio, tc.overlap, tc.distance, 3, tc.related); got != tc.want {
				t.Fatalf("guard=%t want %t", got, tc.want)
			}
		})
	}
	// Preserve actual binary64 subtraction, with no epsilon or quantization.
	for _, tc := range []struct {
		base float64
		want bool
	}{{14000, true}, {7000, false}} {
		if got := shouldRejectAsAmbiguousMultiSignal(3, 2, tc.base, tc.base+.3, .3, .6, 0, 1, 3, false); got != tc.want {
			t.Fatalf("stored 300 Hz boundary at %g: %t want %t", tc.base, got, tc.want)
		}
	}
}

func TestMethodAmbiguityClosestInteriorWitness(t *testing.T) {
	view := &ResolverEvidenceView{candidates: []resolverEvidenceCandidate{
		{call: "K1ABC", receivers: []ResolverReceiverObservation{{FrequencyKHz: 7009.8}, {FrequencyKHz: 7010.10}, {FrequencyKHz: 7010.24}}},
		{call: "K1ABD", receivers: []ResolverReceiverObservation{{FrequencyKHz: 7009.75}, {FrequencyKHz: 7010.11}, {FrequencyKHz: 7010.20}}},
	}}
	a, b, ok := closestResolverFrequencyPair(view, "K1ABC", "K1ABD")
	if !ok || a != 7010.10 || b != 7010.11 {
		t.Fatalf("unique interior closest pair = %g/%g/%t, want 7010.10/7010.11/true", a, b, ok)
	}
	if !shouldRejectAsAmbiguousMultiSignal(3, 2, a, b, .02, .6, 0, 1, 3, false) {
		t.Fatal("interior witness must retain the qualifying narrow conflict")
	}
	for _, pair := range [][2]float64{{7009.8, 7009.75}, {7010.24, 7010.20}} {
		if shouldRejectAsAmbiguousMultiSignal(3, 2, pair[0], pair[1], .02, .6, 0, 1, 3, false) {
			t.Fatal("endpoint-only representatives would accidentally pass this oracle")
		}
	}
}

func BenchmarkResolverClosestFrequencyPair(b *testing.B) {
	for _, count := range []int{3, 64} {
		b.Run(fmt.Sprintf("receivers=%d", count), func(b *testing.B) {
			view := &ResolverEvidenceView{candidates: []resolverEvidenceCandidate{{call: "K1ABC", receivers: make([]ResolverReceiverObservation, count)}, {call: "K1ABD", receivers: make([]ResolverReceiverObservation, count)}}}
			for i := 0; i < count; i++ {
				view.candidates[0].receivers[i].FrequencyKHz = 7009.8
				view.candidates[1].receivers[i].FrequencyKHz = 7010.11
			}
			a, c, ok := closestResolverFrequencyPair(view, "K1ABC", "K1ABD")
			if !ok || a != 7009.8 || c != 7010.11 {
				b.Fatal("frequency witnesses lost")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				closestResolverFrequencyPair(view, "K1ABC", "K1ABD")
			}
		})
	}
}

func TestMethodAmbiguityEqualTimeConflictAbstains(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1})
	evidenceTestObserve(t, r, d, key, at, 7010.1, time.Minute)
	s := evidenceTestObserve(t, r, d, key, at, 7010.2, time.Minute)
	if s.State != ResolverStateSplit || s.Winner != "" {
		t.Errorf("conflicting metadata did not abstain: %+v", s)
	}
	s = evidenceTestObserve(t, r, d, key, at.Add(time.Second), 7010.1, time.Minute)
	if s.State == ResolverStateSplit || s.Winner != "K1ABC" {
		t.Fatalf("newer metadata did not clear conflict: %+v", s)
	}
}

func TestMethodAmbiguityConflictExpiresWithoutLatch(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1})
	evidenceTestObserve(t, r, d, key, at, 7010.1, time.Minute)
	evidenceTestObserve(t, r, d, key, at, 7010.2, time.Minute)
	if !r.Enqueue(ResolverEvidence{ObservedAt: at.Add(30 * time.Second), Key: key, DXCall: "K1ABD", Spotter: "N0BBB", FrequencyKHz: 7010.1, RecencyWindow: time.Minute}) {
		t.Fatal("enqueue unaffected call")
	}
	d.Drain()
	d.Sweep(at.Add(time.Minute))
	if s, ok := r.Lookup(key); !ok || s.State != ResolverStateSplit {
		t.Fatalf("inclusive cutoff lost protection: %+v", s)
	}
	d.Sweep(at.Add(time.Minute + time.Nanosecond))
	if s, ok := r.Lookup(key); !ok || s.State == ResolverStateSplit || s.Winner != "K1ABD" || s.TotalReporters != 1 {
		t.Fatalf("expired conflict remained latched: %+v", s)
	}
}
