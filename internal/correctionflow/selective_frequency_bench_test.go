package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func selectiveMaximumFixture(t testing.TB, receivers int) (spot.ResolverSignalKey, ResolverPrimarySelection, [5]FrequencySource, config.CallCorrectionConfig) {
	t.Helper()
	var rows []spot.ResolverEvidenceRow
	for candidate := 0; candidate < 16; candidate++ {
		for receiver := 0; receiver < receivers; receiver++ {
			rows = append(rows, f0Row(fmt.Sprintf("K%dABC", candidate), fmt.Sprintf("R%d", receiver), 7021, 0))
		}
	}
	k, ordinary, sources, cfg := f0Fixture(t, rows)
	for i := range sources {
		sources[i] = sources[2]
		sources[i].RequestedKey.Bucket = k.Bucket + int64(i-2)
		sources[i].Snapshot.Key = sources[i].RequestedKey
	}
	return k, ordinary, sources, cfg
}

func BenchmarkSelectiveFrequency(b *testing.B) {
	for _, maximum := range []bool{false, true} {
		name := "typical"
		if maximum {
			name = "maximum_5120_incidences"
		}
		b.Run(name, func(b *testing.B) {
			k, ordinary, sources, cfg := f0Fixture(b, selectiveRows())
			want := 1
			if maximum {
				k, ordinary, sources, cfg = selectiveMaximumFixture(b, 64)
				want = 16
			}
			options := FrequencyProposalOptions{150, time.Minute, 3}
			control := AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, options)
			if control.Reason != "assessed" || len(control.Proposals) != want || len(control.Declines) != 0 || control.BeamLimited != maximum {
				b.Fatalf("benchmark must assess a useful population, not refuse early: %+v", control)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = AssessSelectiveFrequencyProposals(k, "K1ABD", 7021, ordinary, sources, cfg, options)
			}
		})
	}
}
