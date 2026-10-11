package cluster

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/spot"
)

func BenchmarkP4FTObserveTypicalNoDue(b *testing.B) {
	base := time.Now().UTC().Add(-6 * time.Second)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := newFTConfidenceController(config.CallCorrectionConfig{})
		p4FTSeed(b, c, base, p4FTContext("K1ABC", "N0AAA", "FT8", base))
		p4FTSeed(b, c, base.Add(time.Second), p4FTContext("K1ABC", "N0BBB", "FT8", base))
		ctx := p4FTContext("K1ABC", "N0CCC", "FT8", base)
		b.StartTimer()
		held, unique, retired := c.Observe(base.Add(2*time.Second), ctx)
		b.StopTimer()
		if !held || unique != 3 || len(retired) != 0 || len(c.pending) != 1 || c.pendingSpots != 3 {
			b.Fatal("typical no-due benchmark did not hold only its live burst")
		}
		b.StartTimer()
	}
}

func p4FTCappedBenchmarkPipeline(b testing.TB) (*outputPipeline, outputSpotContext, time.Time, *atomic.Uint64) {
	b.Helper()
	p := newTestFTOutputPipeline(config.CallCorrectionConfig{}, buffer.NewRingBuffer(16))
	count := new(atomic.Uint64)
	p.secondaryStage = count
	base := time.Unix(1_700_000_000, 0).UTC()
	c := p.ftConfidence
	// Fixture construction is excluded from retirement timing. Exactly 6,000
	// groups with three contexts plus 6,000 with two reach both unchanged caps.
	for i := 0; i < 12000; i++ {
		n := 2
		if i < 6000 {
			n = 3
		}
		frequency := 14074.1 + float64(i)/100 // Distinct canonical 10-Hz burst keys.
		group := &ftConfidencePendingGroup{
			firstSeen: base, lastSeen: base, hardDeadline: base.Add(12 * time.Second), due: base.Add(6 * time.Second), seq: uint64(i),
			contexts: make([]ftConfidencePendingContext, n), spotters: make(map[string]struct{}, n),
		}
		for receiver := 0; receiver < n; receiver++ {
			de := fmt.Sprintf("N0%c%c%c", 'A'+rune(receiver), 'A'+rune(receiver), 'A'+rune(receiver))
			s := spot.NewSpot("K1ABC", de, frequency, "FT8")
			s.Time = base
			ctx := outputSpotContext{spot: s, modeUpper: "FT8"}
			group.contexts[receiver] = newFTConfidencePendingContext(ctx)
			group.spotters[de] = struct{}{}
			if receiver == 0 {
				var ok bool
				group.key, _, ok = buildFTConfidenceKey(s)
				if !ok {
					b.Fatal("capped fixture has no FT group key")
				}
			}
		}
		if c.pending[group.key] != nil {
			b.Fatal("capped fixture collapsed distinct frequency keys")
		}
		c.pending[group.key] = group
		c.pushDueItem(group)
		c.pendingSpots += n
	}
	c.seq = 12000
	if len(c.pending) != 12000 || c.pendingSpots != 30000 || c.queue.Len() != 12000 {
		b.Fatal("capped fixture did not reach literal production caps")
	}
	return p, p4FTContext("K1NEXT", "N0DDD", "FT8", base), base.Add(6 * time.Second), count
}

func BenchmarkP4FTObserveCappedRetirementAndRelease(b *testing.B) {
	b.ReportAllocs()
	b.ReportMetric(12000, "groups/op")
	b.ReportMetric(30000, "contexts/op")
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p, ctx, clock, deliveries := p4FTCappedBenchmarkPipeline(b)
		b.StartTimer()
		continued := p.applyFTConfidenceStage(&ctx, clock)
		b.StopTimer()
		if continued || deliveries.Load() != 30000 || p.buf.GetCount() != 30000 || len(p.buf.GetRecent(17)) != 16 || len(p.ftConfidence.pending) != 1 || p.ftConfidence.pendingSpots != 1 {
			b.Fatalf("capped synchronous retirement skipped real release/admission: continued=%t released=%d ringWrites=%d groups=%d pending=%d", continued, deliveries.Load(), p.buf.GetCount(), len(p.ftConfidence.pending), p.ftConfidence.pendingSpots)
		}
		b.StartTimer()
	}
}

func BenchmarkP4FTObserveChurn(b *testing.B) {
	base := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ReportMetric(256, "bursts/op")
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := newFTConfidenceController(config.CallCorrectionConfig{})
		first := p4FTContext("K1ABC", "N0AAA", "FT8", base)
		second := p4FTContext("K1ABC", "N0BBB", "FT8", base)
		b.StartTimer()
		released := 0
		for burst := 0; burst < 256; burst++ {
			at := base.Add(time.Duration(burst) * (6*time.Second + 2*time.Nanosecond))
			held, unique, retired := c.Observe(at, first)
			if !held || unique != 1 || len(retired) != min(burst, 1) {
				b.Fatal("churn benchmark retained an expired generation")
			}
			released += len(retired)
			held, unique, retired = c.Observe(at.Add(time.Nanosecond), second)
			if !held || unique != 2 || len(retired) != 0 {
				b.Fatal("churn benchmark split predeadline evidence")
			}
		}
		released += len(c.Drain(base.Add(time.Hour), true))
		b.StopTimer()
		if released != 256 || len(c.pending) != 0 || c.pendingSpots != 0 || c.queue.Len() != 0 {
			b.Fatal("churn benchmark failed retained-state convergence")
		}
		b.StartTimer()
	}
}

// This also supplies the smallest fixture-only correction for the existing
// BenchmarkOutputPipelineFTHoldAndRelease: derive Drain's clock from NextDue,
// rather than historical Spot.Time+30s while Observe used actual wall arrival.
func BenchmarkP4OutputPipelineFTHoldAndRealRelease(b *testing.B) {
	cfg := config.CallCorrectionConfig{RecentBandBonusEnabled: true, RecentBandWindowSeconds: 12 * 60 * 60, RecentBandRecordMinUniqueSpotters: 2}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rb := buffer.NewRingBuffer(8)
		p := newTestFTOutputPipeline(cfg, rb)
		base := time.Now().UTC().Truncate(time.Second)
		for j, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
			s := spot.NewSpot("K1BENCH", receiver, 14074.1, "FT8")
			s.Time = base.Add(time.Duration(j) * 400 * time.Millisecond)
			p.processSpotBody(s, nil)
		}
		due, ok := p.ftConfidence.NextDue()
		if !ok || rb.GetCount() != 0 {
			b.Fatal("real-release benchmark never positively held three FT reports")
		}
		p.releaseDueFT(due, false)
		if rb.GetCount() != 3 || p.ftConfidence.pendingSpots != 0 || len(p.ftConfidence.pending) != 0 {
			b.Fatal("real-release benchmark measured holding without release")
		}
		for _, s := range rb.GetRecent(3) {
			if s.Confidence != "V" {
				b.Fatal("three-receiver FT benchmark changed its V glyph")
			}
		}
	}
}
