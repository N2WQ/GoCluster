package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestNativeModeRulesAreExplicitReplacements(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW"})
	before := f.MatchSnapshot()
	next, err := f.PreparedRules().WithRule(RuleDefinition{0, RulePass, "MODE=FT8"})
	if err != nil {
		t.Fatal(err)
	}
	f.preparedRules = next
	for _, mode := range []string{"CW", "FT8", "USB", ""} {
		value := &spot.Spot{Mode: mode, IsHuman: true}
		if before.MatchesWithPath(value, "") != (mode == "CW") ||
			f.Matches(value) != (mode == "FT8") {
			t.Fatal("replacement retained hidden category delta state")
		}
	}
}
