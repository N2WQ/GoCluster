package filter

import (
	"reflect"
	"testing"

	"dxcluster/spot"
)

func TestRuleSetWholeExpressionSourcePolicy(t *testing.T) {
	for _, test := range []struct {
		source         string
		human, skimmer bool
	}{
		{`MODE=CW`, true, true},
		{`MODE=CW OR MODE=FT8`, true, true},
		{`NOT (MODE=USB OR NOT MODE=CW)`, true, true},
		{`COMMENT="SOURCE=SKIMMER"`, true, true},
		{`MODE=CW AND SOURCE=HUMAN`, true, false},
		{`MODE=CW AND SOURCE=HUMAN,SKIMMER`, true, true},
		{`SOURCE=HUMAN OR MODE=CW`, true, true},
		{`MODE=CW OR (MODE=USB AND NOT SOURCE=SKIMMER)`, true, true},
		{`NOT SOURCE=HUMAN`, false, true},
		{`SOURCE<>HUMAN`, false, true},
		{`SOURCE=SKIMMER AND MODE=CW`, false, true},
		{`MODE=CW AND SOURCE=HUMAN AND SOURCE=SKIMMER`, false, false},
		{`NOT (SOURCE=HUMAN OR MODE=CW)`, false, false},
	} {
		t.Run(test.source, func(t *testing.T) {
			set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, test.source})
			saved := set.Definitions()
			for _, nearby := range []bool{false, true} {
				for _, human := range []bool{false, true} {
					want := test.skimmer
					if human {
						want = test.human
					}
					value := &spot.Spot{Mode: "CW", IsHuman: human, Comment: "SOURCE=SKIMMER"}
					if got := set.Matches(value, "", nearby); got != want {
						t.Fatalf("human=%v nearby=%v got %v, want %v", human, nearby, got, want)
					}
					allocs := testing.AllocsPerRun(100, func() {
						ruleEvaluationSink = set.Matches(value, "", nearby)
					})
					if allocs != 0 || ruleEvaluationSink != want {
						t.Fatalf("human=%v nearby=%v allocations=%g result=%v, want 0/%v",
							human, nearby, allocs, ruleEvaluationSink, want)
					}
				}
				row := set.Readback(nearby)[0]
				if row.EffectiveExpr != row.Expr || row.Suspended {
					t.Fatalf("incorrect effective source disclosure: %+v", row)
				}
			}
			if !reflect.DeepEqual(saved, set.Definitions()) {
				t.Fatal("source evaluation changed saved syntax")
			}
		})
	}
}

func TestRuleSetSourcePolicyAbsentPassAndReject(t *testing.T) {
	for _, definitions := range [][]RuleDefinition{
		nil,
		{{0, RuleReject, "MODE=USB"}},
		{{0, RulePass, "DXCONT=EU"}, {1, RuleReject, "MODE=USB"}},
	} {
		set := mustPrepareRuleSet(t, definitions...)
		for _, human := range []bool{false, true} {
			if !set.Matches(&spot.Spot{Mode: "CW", IsHuman: human}, "", true) {
				t.Fatal("absent PASS acquired a source restriction")
			}
			if len(definitions) > 0 && set.Matches(&spot.Spot{Mode: "USB", IsHuman: human}, "", true) {
				t.Fatal("source-less REJECT stopped vetoing a source")
			}
		}
		if len(definitions) == 2 {
			row := set.Readback(true)[0]
			if !row.Suspended || row.EffectiveExpr != "" {
				t.Fatalf("suspended PASS acquired effective source condition: %+v", row)
			}
		}
	}
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=CW"},
		RuleDefinition{1, RulePass, "SOURCE=SKIMMER AND MODE=FT8"},
		RuleDefinition{2, RuleReject, "COMMENT=blocked"})
	for _, human := range []bool{false, true} {
		for _, mode := range []string{"CW", "FT8"} {
			want := mode == "CW" || !human && mode == "FT8"
			value := &spot.Spot{Mode: mode, IsHuman: human}
			if got := set.Matches(value, "", false); got != want {
				t.Fatalf("source selection changed PASS union: human=%v mode=%s", human, mode)
			}
			value.Comment = "blocked"
			if set.Matches(value, "", false) {
				t.Fatal("REJECT did not veto both sources")
			}
		}
	}
}

func TestRuleSetSourcePolicyFullBudget(t *testing.T) {
	set := mustPrepareRuleSet(t, fullBudgetRuleDefinitions()...)
	want := RuleProgramCounts{Nodes: 246, Atoms: 128, Values: 512}
	before := set.Counts()
	for _, nearby := range []bool{false, true} {
		for _, human := range []bool{false, true} {
			if got := set.Matches(&spot.Spot{IsHuman: human, HasReport: true, Report: 1}, "", nearby); !got {
				t.Fatalf("full budget human=%v nearby=%v got %v", human, nearby, got)
			}
		}
		for _, row := range set.Readback(nearby) {
			if row.EffectiveExpr != row.Expr {
				t.Fatal("full-budget expression acquired a hidden condition")
			}
		}
	}
	if before.Normal != want || before.Nearby != want || before.Retained != want || set.Counts() != before {
		t.Fatalf("source evaluation changed syntax budget: %+v", set.Counts())
	}
	assertPreparedRulePrograms(t, set)
}
