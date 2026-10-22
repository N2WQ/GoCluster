package filter

import (
	"fmt"
	"testing"

	"dxcluster/spot"
)

func minSNRTestSpot(mode string, report int, hasReport, human bool) *spot.Spot {
	s := spot.NewSpotNormalized("K1ABC", "W1XYZ", 14025, mode)
	s.Report, s.HasReport, s.IsHuman = report, hasReport, human
	return s
}

func TestNativeSNRRulesBoundariesAndExplicitExemptions(t *testing.T) {
	for _, minimum := range []int{-10, 0, 10} {
		f := nativeRuleFilter(t, RuleDefinition{0, RulePass, fmt.Sprintf("MODE=CW AND SNR>=%d", minimum)})
		for _, human := range []bool{false, true} {
			for _, present := range []bool{false, true} {
				for _, report := range []int{minimum - 1, minimum, minimum + 1} {
					s := minSNRTestSpot("CW", report, present, human)
					if f.Matches(s) != (human && present && report >= minimum) {
						t.Fatal("source-less PASS or SNR boundary truth changed")
					}
				}
			}
		}
	}
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "SOURCE=HUMAN OR SNR>=10"})
	if !f.Matches(minSNRTestSpot("CW", -100, false, true)) ||
		f.Matches(minSNRTestSpot("CW", 0, false, false)) {
		t.Fatal("explicit exemption or missing report truth changed")
	}
}

func BenchmarkMinSNRMatches(b *testing.B) {
	f := nativeRuleFilter(b, RuleDefinition{0, RulePass, "MODE=CW AND SNR>=0"})
	s := minSNRTestSpot("CW", 1, true, true)
	b.ReportAllocs()
	for b.Loop() {
		if !f.Matches(s) {
			b.Fatal("SNR fixture rejected")
		}
	}
}
