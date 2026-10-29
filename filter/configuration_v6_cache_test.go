package filter

import (
	"slices"
	"testing"

	"dxcluster/spot"
)

func TestV6ConfigurationPreparedCacheReuse(t *testing.T) {
	set := mustPrepareRuleSet(t,
		RuleDefinition{0, RulePass, "MODE=CW"}, RuleDefinition{9, RuleReject, "MODE=FT8"})
	input := V6Configuration{Filters: V6FilterConfiguration{AllowSelf: DefaultBoolFalse}.WithRuleSet(set)}
	c, runtime := mustPrepareV6Configuration(t, input)
	if c.Filters.preparedRules != set || runtime.PreparedRules() != set {
		t.Fatal("attested definitions were recompiled")
	}
	clone := c.Clone()
	readback := V6ConfigurationFromFilter(&runtime, c.Settings)
	for _, snapshot := range []V6Configuration{clone, readback} {
		if snapshot.Filters.preparedRules != set || &snapshot.Filters.Rules[0] == &c.Filters.Rules[0] {
			t.Fatal("clone/readback recompiled programs or borrowed definitions")
		}
		prepared, f := mustPrepareV6Configuration(t, snapshot)
		if f.PreparedRules() != set || prepared.Filters.preparedRules != set {
			t.Fatal("unchanged snapshot preparation lost its attestation")
		}
	}
	clone.Filters.Rules[0].Expr = "MODE=USB"
	readback.Filters.Rules[0].Expr = "MODE=RTTY"
	if c.Filters.Rules[0].Expr != "MODE=CW" ||
		!runtime.PreparedRules().Matches(&spot.Spot{Mode: "CW", IsHuman: true}, EvaluationContext{}) {
		t.Fatal("clone/readback definitions changed another snapshot")
	}

	edited, err := set.WithRule(RuleDefinition{9, RuleReject, "MODE=RTTY"})
	if err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.Filters = changed.Filters.WithRuleSet(edited)
	prepared, f := mustPrepareV6Configuration(t, changed)
	if f.PreparedRules() != edited || prepared.Filters.preparedRules != edited ||
		edited.rules[0].program != set.rules[0].program {
		t.Fatal("changed-slot transfer prepared rules a second time")
	}
	changed.Filters.Rules[0].Expr = "MODE=FT8"
	if prepared.Filters.Rules[0].Expr != "MODE=CW" || c.Filters.Rules[1].Expr != "MODE=FT8" {
		t.Fatal("WithRuleSet or preparation borrowed a public definition slice")
	}
	reset := c.Filters.WithRuleSet(nil)
	if len(reset.Rules) != 0 || reset.preparedRules != nil || reset.AllowSelf != DefaultBoolFalse {
		t.Fatal("WithRuleSet(nil) lost controls or retained old rules")
	}
}

func TestV6ConfigurationStaleCacheCorrespondence(t *testing.T) {
	base, original := mustPrepareV6Configuration(t, V6Configuration{
		Filters: V6FilterConfiguration{Rules: []RuleDefinition{
			{0, RulePass, "MODE=CW"}, {9, RuleReject, "MODE=FT8"},
		}},
	})
	for _, test := range []struct {
		name   string
		mutate func(*V6Configuration)
		want   []RuleDefinition
	}{
		{"slot", func(c *V6Configuration) { c.Filters.Rules[1].Slot = 8 },
			[]RuleDefinition{{0, RulePass, "MODE=CW"}, {8, RuleReject, "MODE=FT8"}}},
		{"action", func(c *V6Configuration) { c.Filters.Rules[0].Action = RuleReject },
			[]RuleDefinition{{0, RuleReject, "MODE=CW"}, {9, RuleReject, "MODE=FT8"}}},
		{"expression", func(c *V6Configuration) { c.Filters.Rules[0].Expr = "MODE=USB" },
			[]RuleDefinition{{0, RulePass, "MODE=USB"}, {9, RuleReject, "MODE=FT8"}}},
		{"equivalent-alias", func(c *V6Configuration) { c.Filters.Rules[0].Expr = " ( mode=cw ) " },
			[]RuleDefinition{{0, RulePass, "MODE=CW"}, {9, RuleReject, "MODE=FT8"}}},
		{"order", func(c *V6Configuration) { slices.Reverse(c.Filters.Rules) },
			[]RuleDefinition{{0, RulePass, "MODE=CW"}, {9, RuleReject, "MODE=FT8"}}},
		{"fewer", func(c *V6Configuration) { c.Filters.Rules = c.Filters.Rules[:1] },
			[]RuleDefinition{{0, RulePass, "MODE=CW"}}},
		{"more", func(c *V6Configuration) {
			c.Filters.Rules = append(c.Filters.Rules, RuleDefinition{4, RulePass, "MODE=USB"})
		}, []RuleDefinition{{0, RulePass, "MODE=CW"}, {4, RulePass, "MODE=USB"}, {9, RuleReject, "MODE=FT8"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base.Clone()
			test.mutate(&input)
			c, f := mustPrepareV6Configuration(t, input)
			if f.PreparedRules() == original.PreparedRules() || c.Filters.preparedRules != f.PreparedRules() {
				t.Fatal("edited definitions reused stale programs")
			}
			if !slices.Equal(c.Filters.Rules, test.want) || !slices.Equal(f.PreparedRules().Definitions(), test.want) {
				t.Fatalf("edited definitions were not prepared: %#v", c.Filters.Rules)
			}
			if test.name == "expression" &&
				(f.PreparedRules().Matches(&spot.Spot{Mode: "CW", IsHuman: true}, EvaluationContext{}) ||
					!f.PreparedRules().Matches(&spot.Spot{Mode: "USB", IsHuman: true}, EvaluationContext{})) {
				t.Fatal("runtime evaluated stale expression")
			}
		})
	}
	if base.Filters.Rules[0].Expr != "MODE=CW" ||
		!original.PreparedRules().Matches(&spot.Spot{Mode: "CW", IsHuman: true}, EvaluationContext{}) {
		t.Fatal("edited snapshot changed its source")
	}
}
