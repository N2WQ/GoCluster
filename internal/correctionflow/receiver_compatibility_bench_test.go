package correctionflow

import (
	"fmt"
	"testing"

	"dxcluster/config"
	"dxcluster/spot"
)

// Six populated bins for every identity reach the actual 9*8*6 Score ceiling.
// Sixteen raw alias spellings by 64 receivers by five sources also exercise
// the inherited 5,120-incidence admission bound before canonical joining.
func compatibilityMaximumFixture(t testing.TB) (spot.ResolverSignalKey, ResolverPrimarySelection, [5]FrequencySource, config.CallCorrectionConfig) {
	t.Helper()
	var rows []spot.ResolverEvidenceRow
	for identity := range 8 {
		call := fmt.Sprintf("K1AB%c", 'A'+identity)
		for _, alias := range []string{"KH6/" + call, call + "/KH6"} {
			for receiver := range 64 {
				row := compatibilityRow(alias, fmt.Sprintf("R%02d", receiver), 1000, [6]int{0, 6, 12, 18, 24, 999}[receiver%6])
				rows = append(rows, row)
			}
		}
	}
	key, ordinary, sources, cfg := f0Fixture(t, rows)
	for i := range sources {
		sources[i] = sources[2]
		sources[i].RequestedKey.Bucket = key.Bucket + int64(i-2)
		sources[i].Snapshot.Key = sources[i].RequestedKey
	}
	return key, ordinary, sources, cfg
}

func TestCCV2BScoreCallBound(t *testing.T) {
	key, ordinary, sources, cfg := compatibilityMaximumFixture(t)
	got := AssessReceiverCompatibilityProposals(key, "K1ABZ/KH6", 7021, ordinary, sources, cfg, compatibilityOptions, compatibilityToyModel())
	if !got.Available || len(got.Hypotheses) != 9 || got.ScoreCalls != 432 || got.Denominator != 64000 {
		t.Fatalf("maximum must score the full admitted population: %+v", got)
	}
	got = AssessReceiverCompatibilityProposals(key, "KH6/K1ABA", 7021, ordinary, sources, cfg, compatibilityOptions, compatibilityToyModel())
	if !got.Available || len(got.Hypotheses) != 8 || got.ScoreCalls != 384 {
		t.Fatalf("canonical KEEP did not reduce actual Score calls: %+v", got)
	}
}

func BenchmarkReceiverCompatibility(b *testing.B) {
	for _, maximum := range []bool{false, true} {
		name := "typical"
		if maximum {
			name = "maximum_5120_incidences_432_scores"
		}
		b.Run(name, func(b *testing.B) {
			rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 2, 0), compatibilityRow("B", "R1", 1, 0), compatibilityRow("B", "R2", 1, 0)}
			key, ordinary, sources, cfg := f0Fixture(b, rows)
			subject, calls := "C", 6
			if maximum {
				key, ordinary, sources, cfg = compatibilityMaximumFixture(b)
				subject, calls = "K1ABZ/KH6", 432
			}
			model := compatibilityToyModel()
			control := AssessReceiverCompatibilityProposals(key, subject, 7021, ordinary, sources, cfg, compatibilityOptions, model)
			if !control.Available || control.ScoreCalls != calls || !maximum && (!control.SelectedOK || control.Selected.Identity != "B") {
				b.Fatalf("benchmark must execute B, not unavailable refusal: %+v", control)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = AssessReceiverCompatibilityProposals(key, subject, 7021, ordinary, sources, cfg, compatibilityOptions, model)
			}
		})
	}
}
