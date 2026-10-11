package spot

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSignalResolverOlderMetadataCannotRollback(t *testing.T) {
	r := NewSignalResolver(SignalResolverConfig{EvalMinInterval: time.Nanosecond})
	d, err := NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 10, 12, 0, 10, 0, time.UTC)
	key := NewResolverSignalKey(7010, "40m", "CW", 500)
	for _, ev := range []ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.1, Report: 20},
		{ObservedAt: now.Add(-time.Second), Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.2, Report: 5},
	} {
		if !r.Enqueue(ev) {
			t.Fatal("enqueue metadata")
		}
	}
	d.Drain()
	st := d.states[key]
	c := st.candidates["K1ABC"]
	if !st.lastSeen.Equal(now) || !c.lastSeen.Equal(now) || c.lastReport != 20 || c.lastFreqKHz != 7010.1 {
		t.Fatalf("older evidence rolled back key/candidate metadata: key=%s candidate=%+v", st.lastSeen, c)
	}
}

func evidenceTestDriver(t testing.TB, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time) {
	t.Helper()
	r := NewSignalResolver(cfg)
	d, err := NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, d, NewResolverSignalKey(7010, "40m", "CW", 500), time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
}

func evidenceTestObserve(t testing.TB, r *SignalResolver, d *SignalResolverDriver, key ResolverSignalKey, at time.Time, frequency float64, window time.Duration) ResolverSnapshot {
	t.Helper()
	if !r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: "N0AAA",
		FrequencyKHz: frequency, RecencyWindow: window}) {
		t.Fatal("enqueue observation")
	}
	d.Drain()
	sweepAt := at.Add(time.Millisecond)
	if !sweepAt.After(d.lastSweepAt) {
		sweepAt = d.lastSweepAt.Add(time.Millisecond)
	}
	d.Sweep(sweepAt)
	s, ok := r.Lookup(key)
	if !ok {
		t.Fatal("missing observation publication")
	}
	return s
}

func evidenceTestRow(t testing.TB, view *ResolverEvidenceView) ResolverReceiverObservation {
	t.Helper()
	var row ResolverReceiverObservation
	view.Range(func(call string, observation ResolverReceiverObservation) bool {
		if call == "K1ABC" && observation.Receiver == "N0AAA" {
			row = observation
		}
		return true
	})
	if row.Receiver == "" {
		t.Fatal("missing receiver N0AAA")
	}
	return row
}

func TestSignalResolverReceiverPublicationGenerations(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	first := evidenceTestObserve(t, r, d, key, at, 7010.1, time.Minute)
	d.Sweep(at.Add(2 * time.Millisecond))
	unchanged, _ := r.Lookup(key)
	if unchanged.ReceiverEvidence() != first.ReceiverEvidence() {
		t.Fatal("unchanged sweep copied evidence")
	}
	newer := evidenceTestObserve(t, r, d, key, at.Add(time.Second), 7010.1, time.Minute)
	if newer.ReceiverEvidence() == first.ReceiverEvidence() {
		t.Fatal("timestamp-only update reused stale generation")
	}
	first.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool { row.FrequencyKHz = 123; return true })
	if row := evidenceTestRow(t, first.ReceiverEvidence()); !row.ObservedAt.Equal(at) || row.FrequencyKHz != 7010.1 {
		t.Fatalf("old publication mutated: %+v", row)
	}
	window := evidenceTestObserve(t, r, d, key, at.Add(time.Second), 7010.1, 2*time.Minute)
	if window.ReceiverEvidence() == newer.ReceiverEvidence() || window.ReceiverEvidence().RecencyWindow() != 2*time.Minute {
		t.Fatal("window-only change not published")
	}
	compact := window.Compact()
	if compact.ReceiverEvidence() != nil || compact.WinnerSupport != window.WinnerSupport || len(compact.CandidateRanks) != len(window.CandidateRanks) {
		t.Fatal("compaction changed scoring data")
	}
	if allocations := testing.AllocsPerRun(100, func() { r.Lookup(key) }); allocations != 0 {
		t.Fatalf("Lookup allocations=%g", allocations)
	}
}

func TestSignalResolverReceiverFrequencyConflict(t *testing.T) {
	for _, frequencies := range [][]float64{{7010.1, 7010.2}, {7010.2, 7010.1}} {
		r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
		evidenceTestObserve(t, r, d, key, at, frequencies[0], time.Minute)
		s := evidenceTestObserve(t, r, d, key, at, frequencies[1], time.Minute)
		if row := evidenceTestRow(t, s.ReceiverEvidence()); !row.FrequencyConflict || row.FrequencyKHz != 0 || s.TotalReporters != 1 {
			t.Fatalf("equal-time conflict=%+v snapshot=%+v", row, s)
		}
		for _, timestamp := range []time.Time{at, at.Add(-time.Second)} {
			s = evidenceTestObserve(t, r, d, key, timestamp, frequencies[0], time.Minute)
			if !evidenceTestRow(t, s.ReceiverEvidence()).FrequencyConflict {
				t.Fatal("duplicate/older evidence cleared conflict")
			}
		}
		s = evidenceTestObserve(t, r, d, key, at.Add(time.Second), 7010.15, time.Minute)
		if row := evidenceTestRow(t, s.ReceiverEvidence()); row.FrequencyConflict || row.FrequencyKHz != 7010.15 {
			t.Fatalf("newer observation did not supersede: %+v", row)
		}
		d.Sweep(at.Add(time.Minute + time.Second))
		if s, ok := r.Lookup(key); !ok || s.TotalReporters != 1 {
			t.Fatal("lower recency cutoff must be inclusive")
		}
		d.Sweep(at.Add(time.Minute + time.Second + time.Nanosecond))
		if _, ok := r.Lookup(key); ok || r.MetricsSnapshot().PublishedReceiverRecords != 0 {
			t.Fatal("expiry retained publication")
		}
	}
}

func TestSignalResolverReceiverRetentionChurn(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{MaxActiveKeys: 2, MaxCandidatesPerKey: 2,
		MaxReportersPerCand: 2, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	for cycle := 0; cycle < 20; cycle++ {
		for k := 0; k < 3; k++ {
			frequency := 7010 + float64(k)*2
			for c := 0; c < 4; c++ {
				for receiver := 0; receiver < 4; receiver++ {
					r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: fmt.Sprintf("K1AB%c", 'A'+c),
						Spotter: fmt.Sprintf("N0%03d", receiver), FrequencyKHz: frequency, RecencyWindow: time.Second})
					d.Drain()
				}
			}
		}
		d.Sweep(at.Add(time.Millisecond))
		if len(d.states) != 2 || r.MetricsSnapshot().PublishedReceiverRecords > 8 {
			t.Fatal("owner/publication cap exceeded")
		}
		for _, st := range d.states {
			if len(st.candidates) > 2 || len(st.reporterRefs) > 4 {
				t.Fatal("candidate/receiver refs exceeded cap")
			}
			for _, c := range st.candidates {
				if len(c.reporters) > 2 {
					t.Fatal("reporter cap exceeded")
				}
			}
		}
		at = at.Add(2 * time.Second)
		d.Sweep(at)
		if len(d.states) != 0 || r.MetricsSnapshot().PublishedReceiverRecords != 0 {
			t.Fatal("churn cleanup left derived publication")
		}
	}
	if r.MetricsSnapshot().EvictedCandidates == 0 || r.MetricsSnapshot().EvictedReporters == 0 || r.MetricsSnapshot().DropMaxKeys == 0 {
		t.Fatal("fixture did not exercise all bounds")
	}
}

func TestSignalResolverConcurrentImmutableEvidence(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	old := evidenceTestObserve(t, r, d, key, at, 7010.1, time.Minute)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			s, ok := r.Lookup(key)
			if !ok || s.ReceiverEvidence().Len() != 1 || s.WinnerSupport != 1 {
				t.Error("incoherent captured generation")
				return
			}
			var row ResolverReceiverObservation
			s.ReceiverEvidence().Range(func(_ string, observation ResolverReceiverObservation) bool { row = observation; return true })
			if row.WeightMilli != 1000 || row.FrequencyConflict {
				t.Error("publication mutated")
			}
		}
	}()
	for i := 1; i <= 500; i++ {
		evidenceTestObserve(t, r, d, key, at.Add(time.Duration(i)*time.Millisecond), 7010.1, time.Minute)
	}
	wg.Wait()
	if !evidenceTestRow(t, old.ReceiverEvidence()).ObservedAt.Equal(at) {
		t.Fatal("held old generation changed")
	}
}

func BenchmarkSignalResolverReceiverPublication(b *testing.B) {
	for _, size := range []int{16, 1024} {
		for _, dirty := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/dirty=%t", size, dirty), func(b *testing.B) {
				r, d, key, at := evidenceTestDriver(b, SignalResolverConfig{QueueSize: 2048, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64})
				for i := 0; i < size; i++ {
					r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: fmt.Sprintf("K1AB%c", 'A'+i/64),
						Spotter: fmt.Sprintf("N0%03d", i%64), FrequencyKHz: 7010, RecencyWindow: time.Hour})
				}
				d.Drain()
				st := d.states[key]
				r.evaluateKey(st, at)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if dirty {
						upsertResolverReporter(st, st.candidates["K1ABA"], "N0000", at.Add(time.Duration(i+1)), 7010, 1000)
					}
					r.evaluateKey(st, at)
				}
			})
		}
	}
}

func BenchmarkSignalResolverLookup(b *testing.B) {
	r := NewSignalResolver(SignalResolverConfig{})
	key := NewResolverSignalKey(7010, "40m", "CW", 500)
	r.publishSnapshot(key, ResolverSnapshot{Key: key, Winner: "K1ABC", WinnerSupport: 2, TotalReporters: 3})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Lookup(key)
	}
}

// Retain only the last bounded benchmark owner for inuse_space inspection.
var benchmarkResolverEvidenceDriverSink *SignalResolverDriver

func BenchmarkSignalResolverReceiverChurn(b *testing.B) {
	r, d, key, at := evidenceTestDriver(b, SignalResolverConfig{QueueSize: 2048, MaxActiveKeys: 2,
		MaxCandidatesPerKey: 16, MaxReportersPerCand: 64, EvalMinInterval: time.Hour, SweepInterval: time.Nanosecond})
	b.ReportAllocs()
	b.ResetTimer()
	for cycle := 0; cycle < b.N; cycle++ {
		for c := 0; c < 16; c++ {
			for receiver := 0; receiver < 64; receiver++ {
				if !r.Enqueue(ResolverEvidence{ObservedAt: at, Key: key, DXCall: fmt.Sprintf("K1AB%c", 'A'+c),
					Spotter: fmt.Sprintf("N0%03d", receiver), FrequencyKHz: 7010, RecencyWindow: time.Second}) {
					b.Fatal("enqueue")
				}
			}
		}
		d.Drain()
		r.evaluateKey(d.states[key], at)
		if r.MetricsSnapshot().PublishedReceiverRecords != 1024 {
			b.Fatal("capped generation incomplete")
		}
		at = at.Add(2 * time.Second)
		d.Sweep(at)
		if len(d.states) != 0 || r.MetricsSnapshot().PublishedReceiverRecords != 0 {
			b.Fatal("churn retained expired records")
		}
	}
	b.StopTimer()
	// A small live generation distinguishes retained state from historical churn.
	evidenceTestObserve(b, r, d, key, at, 7010, time.Second)
	benchmarkResolverEvidenceDriverSink = d
}
