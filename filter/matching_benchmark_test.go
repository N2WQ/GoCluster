package filter

import (
	"testing"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// Prediction is deliberately excluded: every case receives the same path class.
// Keep these inputs and outcomes stable across the engine replacement.
func BenchmarkMatchesWithPath(b *testing.B) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(b)); err != nil {
		b.Fatal(err)
	}
	fine := pathreliability.EncodeCell("FN31")
	coarse := pathreliability.EncodeCoarseCell("FN31")
	if fine == pathreliability.InvalidCell || coarse == pathreliability.InvalidCell {
		b.Fatal("benchmark requires valid FN31 cells")
	}
	for _, test := range []struct {
		name      string
		mode      string
		frequency float64
		want      bool
	}{
		{"constructor-default", "CW", 14025, true},
		{"unrestricted", "CW", 14025, true},
		{"narrow-pass", "CW", 14025, true},
		{"narrow-mode-reject", "FT8", 14025, false},
		{"nearby-fine", "CW", 14025, true},
		{"nearby-coarse", "CW", 3520, true},
		{"literal-comment", "CW", 14025, true},
	} {
		b.Run(test.name, func(b *testing.B) {
			f := NewFilter()
			if test.name != "constructor-default" {
				f.Reset()
			}
			switch test.name {
			case "narrow-pass", "narrow-mode-reject":
				f.preparedRules = mustPrepareRuleSet(b, RuleDefinition{0, RulePass, "BAND=20m AND MODE=CW AND SOURCE=HUMAN"})
			case "nearby-fine", "nearby-coarse":
				f.preparedRules = mustPrepareRuleSet(b, RuleDefinition{0, RulePass, "DXCONT=EU"})
				if err := f.EnableNearby(fine, coarse); err != nil {
					b.Fatal(err)
				}
			case "literal-comment":
				f.preparedRules = mustPrepareRuleSet(b,
					RuleDefinition{0, RulePass, `COMMENT="CQ POTA"`},
					RuleDefinition{1, RuleReject, `COMMENT="QRT"`})
			}
			s := spot.NewSpotNormalized("K1ABC", "N2ABC", test.frequency, test.mode)
			s.IsHuman = true
			s.Comment = "CQ POTA UP 5"
			s.DXMetadata.Grid = "FN31"
			s.DEMetadata.Grid = "EM10"
			s.DXMetadata.Continent = "NA"
			s.DEMetadata.Continent = "NA"
			s.EnsureNormalized()
			if got := f.MatchesWithPath(s, PathClassHigh); got != test.want {
				b.Fatalf("fixture got %v, want %v", got, test.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if f.MatchesWithPath(s, PathClassHigh) != test.want {
					b.Fatal("matching outcome changed")
				}
			}
		})
	}
}
