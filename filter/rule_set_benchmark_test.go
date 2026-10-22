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
	for _, test := range []struct {
		name         string
		definitions  []RuleDefinition
		normal, near bool
	}{
		{"empty", nil, true, true},
		{"mixed-three-rules", typicalRuleDefinitions(), true, true},
		{"mixed-geo-removal", []RuleDefinition{{0, RulePass, "MODE=CW OR DXCONT=NA"}}, true, false},
		{"comment-literal", []RuleDefinition{{0, RulePass, `COMMENT="POTA * ?"`}}, true, true},
		{"comment-miss", []RuleDefinition{{0, RulePass, `COMMENT="POTA.*"`}}, false, false},
		{"full-scan", fullBudgetRuleDefinitions(), false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			set := mustPrepareRuleSet(t, test.definitions...)
			sp := normalizedRuleTestSpot()
			sp.IsHuman = true
			for _, nearby := range []bool{false, true} {
				want := test.normal
				if nearby {
					want = test.near
				}
				if got := set.Matches(sp, PathClassHigh, nearby); got != want {
					t.Fatalf("allocation fixture nearby=%v got %v, want %v", nearby, got, want)
				}
				allocations := testing.AllocsPerRun(1000, func() {
					// Matches must create a fresh lazy view on every invocation.
					ruleEvaluationSink = set.Matches(sp, PathClassHigh, nearby)
				})
				if allocations != 0 || ruleEvaluationSink != want {
					t.Fatalf("nearby=%v allocations=%g result=%v, want 0/%v", nearby, allocations, ruleEvaluationSink, want)
				}
			}
		})
	}
}

func BenchmarkRuleSetMatches(b *testing.B) {
	for _, test := range []struct {
		name        string
		definitions []RuleDefinition
		report      int
		want        bool
	}{
		{"typical-three", typicalRuleDefinitions(), 0, true},
		{"full-budget-full-scan", fullBudgetRuleDefinitions(), 0, false},
		{"full-budget-common-pass", fullBudgetRuleDefinitions(), 1, true},
	} {
		for _, nearby := range []bool{false, true} {
			suffix := "/normal"
			if nearby {
				suffix = "/nearby"
			}
			b.Run(test.name+suffix, func(b *testing.B) {
				set := mustPrepareRuleSet(b, test.definitions...)
				if strings.HasPrefix(test.name, "full-budget") {
					want := RuleProgramCounts{Nodes: 246, Atoms: 128, Values: 512}
					counts := set.Counts()
					if counts.Rules != 10 || counts.Normal != want || counts.Nearby != want || counts.Retained != want {
						b.Fatalf("benchmark is not the real full budget: %+v", counts)
					}
					for i := range set.rules {
						rule := &set.rules[i]
						wantAtoms := 13
						if i >= 8 {
							wantAtoms = 12
						}
						if rule.definition.Action != RulePass || rule.normal.atomCount != wantAtoms {
							b.Fatal("full-scan fixture cannot use a reject shortcut or uneven budget")
						}
						for j := range rule.normal.nodes {
							node := &rule.normal.nodes[j]
							if node.kind == expr.AtomKind && (len(node.predicate.integers) != 4 || node.predicate.field != ruleFieldSNR) {
								b.Fatal("full-budget atom must test all four integer values")
							}
						}
					}
				} else {
					want := RuleSetCounts{
						Rules:    3,
						Normal:   RuleProgramCounts{Nodes: 11, Atoms: 7, Values: 7},
						Nearby:   RuleProgramCounts{Nodes: 9, Atoms: 6, Values: 6},
						Retained: RuleProgramCounts{Nodes: 14, Atoms: 9, Values: 7},
					}
					if got := set.Counts(); got != want {
						b.Fatalf("typical fixture counts got %+v, want %+v", got, want)
					}
				}
				sp := normalizedRuleTestSpot()
				sp.IsHuman = true
				sp.Report = test.report
				if got := set.Matches(sp, PathClassHigh, nearby); got != test.want {
					b.Fatalf("fixture got %v, want %v", got, test.want)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var got bool
				for b.Loop() {
					got = set.Matches(sp, PathClassHigh, nearby)
				}
				ruleEvaluationSink = got
				if got != test.want {
					b.Fatalf("timed fixture got %v, want %v", got, test.want)
				}
			})
		}
	}
}
