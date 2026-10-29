package filter

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

func TestRuleSetBudgets(t *testing.T) {
	full := fullBudgetRuleDefinitions()
	set := mustPrepareRuleSet(t, full...)
	want := RuleProgramCounts{Nodes: 246, Atoms: 128, Values: 512}
	if counts := set.Counts(); counts.Rules != 10 || counts.Program != want {
		t.Fatalf("full-budget counts got %+v, want ten programs %+v", counts, want)
	}
	if set.Matches(&spot.Spot{IsHuman: true, HasReport: true, Report: 0}, EvaluationContext{PathClass: ""}) {
		t.Fatal("full-scan budget fixture unexpectedly passed")
	}
	before := set.Definitions()
	for _, definition := range []RuleDefinition{
		{9, RulePass, strings.Repeat("SNR=1,2,3,4 OR ", 12) + "SNR=1,2,3,4"},
		{9, RulePass, strings.Replace(full[9].Expr, "SNR=1,2,3,4", "SNR=1,2,3,4,5", 1)},
	} {
		changed := append([]RuleDefinition(nil), full...)
		changed[9] = definition
		if got, err := PrepareRuleSet(changed); err == nil || got != nil {
			t.Fatal("over-budget replacement admitted")
		}
		if got, err := set.WithRule(definition); err == nil || got != nil {
			t.Fatal("over-budget edit admitted")
		}
		if !reflect.DeepEqual(before, set.Definitions()) || set.Counts().Program != want {
			t.Fatal("refused budget edit changed original")
		}
	}
	shorter, err := set.WithRule(RuleDefinition{9, RuleReject, "MODE=CW"})
	if err != nil || shorter.Counts().Program.Atoms != 117 || shorter.Counts().Program.Values != 465 {
		t.Fatalf("replacement did not subtract old slot budget: %+v, %v", shorter.Counts(), err)
	}
	restored, err := shorter.WithRule(full[9])
	if err != nil || !restored.Equal(set) || restored.Counts().Program != want {
		t.Fatalf("budget was not reclaimed on replacement: %v", err)
	}

	// Repeated operands consume budget. Two rules have 16 * 32 = 512 values.
	list := strings.Repeat("1,", 31) + "1"
	source := strings.Repeat("SNR="+list+" OR ", 7) + "SNR=" + list
	duplicates := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, source}, RuleDefinition{9, RulePass, source})
	if got := duplicates.Counts().Program; got.Atoms != 16 || got.Values != 512 {
		t.Fatalf("duplicate values were deduplicated: %+v", got)
	}
	if got, err := duplicates.WithRule(RuleDefinition{4, RulePass, "SNR=1"}); err == nil || got != nil {
		t.Fatal("513th duplicate/list value admitted")
	}
}

func TestRuleSetPerRuleLimits(t *testing.T) {
	exactBytes := "MODE=CW" + strings.Repeat(" ", expr.MaxBytes-len("MODE=CW"))
	if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, exactBytes}}); err != nil {
		t.Fatalf("exact raw byte limit rejected: %v", err)
	}
	if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, exactBytes + " "}}); err == nil {
		t.Fatal("overlong raw expression admitted")
	}
	if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, "FREQ=1e300,1e300,1e300,1e300"}}); err == nil {
		t.Fatal("overlong canonical expression admitted")
	}
	maxAtoms := strings.Repeat("SNR=1 OR ", expr.MaxAtoms-1) + "SNR=1"
	maxValues := "SNR=" + strings.Repeat("1,", expr.MaxValuesPerAtom-1) + "1"
	for _, source := range []string{maxAtoms, maxValues, strings.Repeat("NOT ", expr.MaxDepth) + "SNR=1"} {
		set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, source})
		assertPreparedRulePrograms(t, set)
	}
	for _, source := range []string{maxAtoms + " OR SNR=1", maxValues + ",1", strings.Repeat("NOT ", expr.MaxDepth+1) + "SNR=1"} {
		if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, source}}); err == nil {
			t.Fatal("per-rule atom/value/depth limit exceeded")
		}
	}
	// 32 atoms + 31 ORs + 65 NOTs reaches exactly 128 nodes.
	nodes := strings.Repeat("NOT NOT SNR=1 OR ", 31) + "NOT NOT NOT SNR=1"
	set := mustPrepareRuleSet(t, RuleDefinition{9, RulePass, nodes})
	if set.Counts().Program.Nodes != 128 {
		t.Fatal("node-bound fixture is not at the limit")
	}
	if _, err := set.WithRule(RuleDefinition{9, RulePass, "NOT " + nodes}); err == nil {
		t.Fatal("129-node expression admitted")
	}
}

func TestRuleSetSingleProgramOwnership(t *testing.T) {
	set := mustPrepareRuleSet(t,
		RuleDefinition{0, RulePass, "MODE=CW,FT8 AND NEARBY OR DXCONT=EU"},
		RuleDefinition{4, RuleReject, "DXZONE=5,5"}, RuleDefinition{9, RulePass, "PATH=HIGH"})
	want := RuleSetCounts{Rules: 3, Program: RuleProgramCounts{Nodes: 7, Atoms: 5, Values: 6}}
	if got := set.Counts(); got != want {
		t.Fatalf("counts=%+v want %+v", got, want)
	}
	assertPreparedRulePrograms(t, set)
	original := set.rules[0].program
	changed, err := set.WithRule(RuleDefinition{4, RuleReject, "MODE=USB"})
	if err != nil || changed.rules[0].program != original {
		t.Fatal("unchanged immutable program not shared")
	}
	compiled, err := compileRule("NOT MODE=CW OR NEARBY")
	if err != nil || compiled.root != compiled.syntax.Root {
		t.Fatalf("compiler root lost: %v", err)
	}
}

func TestRuleSetChurnBoundedSnapshots(t *testing.T) {
	type snapshot struct {
		set          *PreparedRuleSet
		definitions  []RuleDefinition
		readback     []RuleReadback
		counts       RuleSetCounts
		normal, near bool
	}
	sp := &spot.Spot{Mode: "CW", HasReport: true, Report: 3, IsHuman: true}
	var set *PreparedRuleSet
	snapshots := []snapshot{}
	for i := range 240 {
		var err error
		action := RulePass
		if i%2 == 0 {
			action = RuleReject
		}
		set, err = set.WithRule(RuleDefinition{i % 10, action, fmt.Sprintf("DXCONT=EU OR (SNR=%d AND MODE=CW)", i)})
		if err != nil {
			t.Fatal(err)
		}
		if i%30 == 0 {
			snapshots = append(snapshots, snapshot{set, set.Definitions(), set.Readback(), set.Counts(),
				set.Matches(sp, EvaluationContext{PathClass: ""}), set.Matches(sp, EvaluationContext{PathClass: ""})})
		}
		set, err = set.WithoutRule((i + 3) % 10)
		if err != nil {
			t.Fatal(err)
		}
		counts := set.Counts()
		if counts.Rules > 10 || counts.Program.Nodes > counts.Rules*5 ||
			counts.Program.Atoms > counts.Rules*3 || counts.Program.Values > counts.Rules*3 {
			t.Fatalf("historical expressions accumulated at iteration %d: %+v", i, counts)
		}
		assertPreparedRulePrograms(t, set)
	}
	for _, saved := range snapshots {
		if !reflect.DeepEqual(saved.set.Definitions(), saved.definitions) ||
			!reflect.DeepEqual(saved.set.Readback(), saved.readback) || saved.set.Counts() != saved.counts ||
			saved.set.Matches(sp, EvaluationContext{PathClass: ""}) != saved.normal || saved.set.Matches(sp, EvaluationContext{PathClass: ""}) != saved.near {
			t.Fatal("churn changed a prior immutable snapshot")
		}
	}
	for slot := range MaxRuleSlots {
		var err error
		set, err = set.WithoutRule(slot)
		if err != nil {
			t.Fatal(err)
		}
	}
	if set != nil || set.Counts() != (RuleSetCounts{}) {
		t.Fatal("deletion did not release all owned state")
	}
}

func TestRuleSetNearbyAggregateAtomBudget(t *testing.T) {
	source := strings.Repeat("NEARBY OR ", expr.MaxAtoms-1) + "NEARBY"
	definitions := make([]RuleDefinition, 4)
	for i := range definitions {
		definitions[i] = RuleDefinition{i, RulePass, source}
	}
	set := mustPrepareRuleSet(t, definitions...)
	if got := set.Counts().Program; got.Atoms != MaxTotalRuleAtoms || got.Values != 0 {
		t.Fatalf("zero-value predicate counts wrong: %+v", got)
	}
	before := set.Definitions()
	if changed, err := set.WithRule(RuleDefinition{4, RulePass, "NEARBY"}); err == nil || changed != nil {
		t.Fatal("129th predicate atom admitted")
	}
	if !reflect.DeepEqual(set.Definitions(), before) {
		t.Fatal("rejected predicate edit changed original")
	}
}
