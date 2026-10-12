// File role: Measures the existing percentage/classifier/Compact boundaries,
// with correctness and allocation guards qualified before timed repetition.
package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestSupportBasisHelpersNoAllocation(t *testing.T) {
	s := supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 1000, 2, 0)
	if percent, ok := ResolverCallConfidencePercent(s, "K1ABC"); !ok || percent != 0 || classifyTemporalSnapshotState(s) != spot.ResolverStateUncertain {
		t.Fatal("allocation fixture did not qualify known weighted zero")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		ResolverCallConfidencePercent(s, "K1ABC")
		classifyTemporalSnapshotState(s)
		s.Compact()
	}); allocs != 0 {
		t.Fatalf("compact scalar support processing allocated %.2f times", allocs)
	}
}

func BenchmarkSupportBasisHelpers(b *testing.B) {
	for _, mode := range []string{"known_zero", "weighted_positive", "raw", "unavailable"} {
		b.Run(mode, func(b *testing.B) {
			s := supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 1000, 2, 0)
			wantPercent, wantAvailable, wantState := 0, true, spot.ResolverStateUncertain
			switch mode {
			case "weighted_positive":
				s = supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 2850, 2, 1850)
				wantPercent, wantState = 64, spot.ResolverStateConfident
			case "raw":
				s = supportBasisSnapshot(spot.ResolverSupportBasisReceivers, 3, 0, 2, 0)
				wantPercent, wantState = 66, spot.ResolverStateConfident
			case "unavailable":
				s = supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 0, 2, 0)
				wantAvailable = false
			}
			percent, available := ResolverCallConfidencePercent(s, "K1ABC")
			if percent != wantPercent || available != wantAvailable || classifyTemporalSnapshotState(s) != wantState || s.Compact().SupportBasis != s.SupportBasis {
				b.Fatal("support processing benchmark failed its fixed mathematical oracle")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				percent, available = ResolverCallConfidencePercent(s, "K1ABC")
				state := classifyTemporalSnapshotState(s)
				compact := s.Compact()
				if percent != wantPercent || available != wantAvailable || state != wantState || compact.SupportBasis != s.SupportBasis {
					b.Fatal("support processing lost its qualified behavior")
				}
			}
		})
	}
}

func BenchmarkSupportBasisExactSelection(b *testing.B) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	b.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		b.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	for _, row := range []struct{ call, receiver string }{{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"}} {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: 14020, RecencyWindow: time.Minute}) {
			b.Fatal("selection cost fixture lost actual receiver evidence")
		}
	}
	driver.Step(at.Add(time.Millisecond))
	now, cfg := at.Add(time.Second), config.CallCorrectionConfig{}
	selected := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", now)
	if percent, ok := ResolverCallConfidencePercent(selected.Snapshot, "K1ABC"); !ok || percent != 66 || !selected.SnapshotOK || selected.UsedNeighborhood || selected.Snapshot.ReceiverEvidence() == nil {
		b.Fatal("exact selection benchmark failed its real A2/B1 evidence control")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected = SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", now)
	}
	b.StopTimer()
	if percent, ok := ResolverCallConfidencePercent(selected.Snapshot, "K1ABC"); !ok || percent != 66 || selected.Snapshot.SupportBasis != spot.ResolverSupportBasisWeighted {
		b.Fatal("exact selection cost path lost its weighted population")
	}
}
