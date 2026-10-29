package filter

import (
	"strings"
	"testing"

	"dxcluster/filter/expr"
)

func fullBudgetRuleDefinitions() []RuleDefinition {
	definitions := make([]RuleDefinition, MaxRuleSlots)
	for i := range definitions {
		atoms := 13
		if i >= 8 {
			atoms = 12
		}
		definitions[i] = RuleDefinition{i, RulePass, strings.Repeat("SNR=1,2,3,4 OR ", atoms-1) + "SNR=1,2,3,4"}
	}
	return definitions
}

func typicalRuleDefinitions() []RuleDefinition {
	return []RuleDefinition{
		{0, RulePass, "MODE=CW OR MODE=FT8 AND DXCONT=EU"},
		{4, RuleReject, `COMMENT="not present * ?" OR MODE=USB`},
		{9, RulePass, `PATH=HIGH AND COMMENT="POTA * ?"`},
	}
}

func TestRuleSetMatchesAllocs(t *testing.T) {
	for _, tc := range []struct {
		name        string
		definitions []RuleDefinition
		want        bool
	}{
		{"empty", nil, true}, {"mixed-three-rules", typicalRuleDefinitions(), true},
		{"mixed-geography", []RuleDefinition{{0, RulePass, "MODE=CW OR DXCONT=NA"}}, true},
		{"comment-literal", []RuleDefinition{{0, RulePass, `COMMENT="POTA * ?"`}}, true},
		{"comment-miss", []RuleDefinition{{0, RulePass, `COMMENT="POTA.*"`}}, false},
		{"full-scan", fullBudgetRuleDefinitions(), false},
		{"nearby-cached", []RuleDefinition{{0, RulePass, "NEARBY OR NEARBY"}}, true},
		{"nearby-short-circuit", []RuleDefinition{{0, RulePass, "MODE=FT8 OR NEARBY"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := mustPrepareRuleSet(t, tc.definitions...)
			sp := normalizedRuleTestSpot()
			sp.IsHuman = true
			sp.DXCellID = 10
			sp.DECellID = 11
			context := EvaluationContext{PathClass: PathClassHigh, FineCell: 10, CoarseCell: 12}
			if set.Matches(sp, context) != tc.want {
				t.Fatal("allocation control truth wrong")
			}
			allocs := testing.AllocsPerRun(1000, func() { ruleEvaluationSink = set.Matches(sp, context) })
			if allocs != 0 || ruleEvaluationSink != tc.want {
				t.Fatalf("allocations=%g result=%v", allocs, ruleEvaluationSink)
			}
		})
	}
}

func BenchmarkRuleSetMatches(b *testing.B) {
	for _, tc := range []struct {
		name        string
		definitions []RuleDefinition
		report      int
		want        bool
	}{
		{"typical-three/normal", typicalRuleDefinitions(), 0, true},
		{"full-budget-full-scan/normal", fullBudgetRuleDefinitions(), 0, false},
		{"full-budget-common-pass/normal", fullBudgetRuleDefinitions(), 1, true},
		{"nearby-cached-single", []RuleDefinition{{0, RulePass, "NEARBY"}}, 0, true},
		{"nearby-cached-repeated", []RuleDefinition{{0, RulePass, "NEARBY AND NEARBY OR NEARBY"}}, 0, true},
		{"nearby-short-circuit", []RuleDefinition{{0, RulePass, "MODE=FT8 OR NEARBY"}}, 0, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := mustPrepareRuleSet(b, tc.definitions...)
			if strings.HasPrefix(tc.name, "full-budget") {
				want := RuleProgramCounts{Nodes: 246, Atoms: 128, Values: 512}
				if got := set.Counts(); got.Rules != 10 || got.Program != want {
					b.Fatalf("not full budget: %+v", got)
				}
				for i := range set.rules {
					rule := &set.rules[i]
					wantAtoms := 13
					if i >= 8 {
						wantAtoms = 12
					}
					if rule.definition.Action != RulePass || rule.program.atomCount != wantAtoms {
						b.Fatal("invalid scan fixture")
					}
					for _, node := range rule.program.nodes {
						if node.kind == expr.AtomKind && (len(node.predicate.integers) != 4 || node.predicate.field != ruleFieldSNR) {
							b.Fatal("full-budget atom must test four integer values")
						}
					}
				}
			}
			sp := normalizedRuleTestSpot()
			sp.IsHuman = true
			sp.Report = tc.report
			sp.DXCellID = 10
			sp.DECellID = 11
			context := EvaluationContext{PathClass: PathClassHigh, FineCell: 10, CoarseCell: 12}
			if set.Matches(sp, context) != tc.want {
				b.Fatal("benchmark fixture truth wrong")
			}
			b.ReportAllocs()
			b.ResetTimer()
			var got bool
			for b.Loop() {
				got = set.Matches(sp, context)
			}
			ruleEvaluationSink = got
			if got != tc.want {
				b.Fatal("timed truth changed")
			}
		})
	}
}
