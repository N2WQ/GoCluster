package spot

import (
	"fmt"
	"testing"
	"time"
)

func TestResolverLossReadsAndNoopActivityDoNotAllocateOrRepublish(t *testing.T) {
	r, d, key, at, _ := lossTestQuarantine(t, SignalResolverConfig{})
	_, cell, _ := r.lookupSnapshot(key)
	published := cell.snapshot.Load()
	view := published.ReceiverEvidence()
	if allocations := testing.AllocsPerRun(100, func() { r.Lookup(key); r.LookupEligible(key, at) }); allocations != 0 {
		t.Fatalf("raw/all-fresh eligible loss read allocations %g", allocations)
	}
	ev := ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: "N0A2", ObservedAt: at, FrequencyKHz: 7010.1, RecencyWindow: time.Hour}
	for i := 0; i < 100; i++ {
		if err := d.SetProcessElapsed(time.Duration(i) * time.Second); err != nil {
			t.Fatal(err)
		}
		if !r.Enqueue(ev) || d.Drain() != 1 {
			t.Fatal("admit no-op activity")
		}
	}
	if cell.snapshot.Load() != published || d.states[key].publishedReceiverEvidence != view || time.Duration(cell.activity.Load()) != 99*time.Second {
		t.Fatal("activity-only receipts republished rows/ranks or failed to refresh")
	}
}

func BenchmarkResolverLossLookup(b *testing.B) {
	for _, loss := range []bool{false, true} {
		for _, eligible := range []bool{false, true} {
			b.Run(fmt.Sprintf("loss=%v/eligible=%v", loss, eligible), func(b *testing.B) {
				r, d, key, at := lossTestDriver(b, SignalResolverConfig{MaxReportersPerCand: 1})
				lossTestAdmit(b, r, d, key, "K1ABC", "N0A1", at, time.Minute)
				if loss {
					lossTestAdmit(b, r, d, key, "K1ABC", "N0A2", at, time.Minute)
				}
				if s := lossTestSnapshot(b, r, key); s.EvidenceLoss != loss {
					b.Fatal("benchmark loss fixture")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var s ResolverSnapshot
					var ok bool
					if eligible {
						s, ok = r.LookupEligible(key, at)
					} else {
						s, ok = r.Lookup(key)
					}
					if !ok || s.Generation == 0 {
						b.Fatal("benchmark lookup lost generation")
					}
				}
			})
		}
	}
}

func BenchmarkResolverLossNoopAdmittedActivity(b *testing.B) {
	r, d, key, at, _ := lossTestQuarantine(b, SignalResolverConfig{})
	ev := ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: "N0A2", ObservedAt: at, FrequencyKHz: 7010.1, RecencyWindow: time.Hour}
	_, cell, _ := r.lookupSnapshot(key)
	before := cell.snapshot.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.Enqueue(ev) || d.Drain() != 1 {
			b.Fatal("benchmark activity admission")
		}
	}
	b.StopTimer()
	if cell.snapshot.Load() != before {
		b.Fatal("duplicate activity republished immutable data")
	}
}

func BenchmarkResolverLossFullBoundedControl(b *testing.B) {
	for _, tc := range []struct{ candidates, receivers int }{{2, 4}, {16, 64}} {
		b.Run(fmt.Sprintf("candidates=%d/receivers=%d", tc.candidates, tc.receivers), func(b *testing.B) {
			r, d, key, at := lossTestDriver(b, SignalResolverConfig{MaxCandidatesPerKey: tc.candidates, MaxReportersPerCand: tc.receivers, EvalMinInterval: time.Hour})
			for candidate := 0; candidate < tc.candidates; candidate++ {
				for receiver := 0; receiver < tc.receivers; receiver++ {
					lossTestAdmit(b, r, d, key, fmt.Sprintf("K1A%d", candidate), fmt.Sprintf("N0C%dR%d", candidate, receiver), at, 4*time.Hour)
				}
			}
			d.Sweep(at.Add(time.Hour))
			// A fixed cap-plus-one ring and increasing timestamps ensure each
			// receipt evicts the oldest retained receiver, including after wrap.
			names := make([]string, tc.receivers+1)
			for i := range names {
				names[i] = fmt.Sprintf("N0ZZ%d", i)
			}
			controlAt := at.Add(time.Hour + time.Millisecond)
			lossTestAdmit(b, r, d, key, "K1A0", names[0], controlAt, 4*time.Hour)
			st := d.states[key]
			if st.publishedReceiverEvidence.Len() != tc.candidates*tc.receivers || !lossTestSnapshot(b, r, key).EvidenceLoss {
				b.Fatal("full bounded loss-control fixture")
			}
			beforeEvictions := r.MetricsSnapshot().EvictedReporters
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lossTestAdmit(b, r, d, key, "K1A0", names[(i+1)%len(names)], controlAt.Add(time.Duration(i+1)*time.Nanosecond), 4*time.Hour)
			}
			b.StopTimer()
			if st.publishedReceiverEvidence.Len() != tc.candidates*tc.receivers || len(st.candidates) != tc.candidates {
				b.Fatal("control grew past existing row/key caps")
			}
			if r.MetricsSnapshot().EvictedReporters-beforeEvictions != uint64(b.N) {
				b.Fatal("benchmark receipt did not cause an actual cap discard")
			}
		})
	}
}
