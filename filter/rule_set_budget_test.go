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
	if counts := set.Counts(); counts.Rules != 10 || counts.Normal != want || counts.Nearby != want || counts.Retained != want {
		t.Fatalf("full-budget counts got %+v, want ten programs %+v", counts, want)
	}
	if set.Matches(&spot.Spot{IsHuman: true, HasReport: true, Report: 0}, "", false) {
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
		if !reflect.DeepEqual(before, set.Definitions()) || set.Counts().Normal != want {
			t.Fatal("refused budget edit changed original")
		}
	}
	shorter, err := set.WithRule(RuleDefinition{9, RuleReject, "MODE=CW"})
	if err != nil || shorter.Counts().Normal.Atoms != 117 || shorter.Counts().Normal.Values != 465 {
		t.Fatalf("replacement did not subtract old slot budget: %+v, %v", shorter.Counts(), err)
	}
	restored, err := shorter.WithRule(full[9])
	if err != nil || !restored.Equal(set) || restored.Counts().Normal != want {
		t.Fatalf("budget was not reclaimed on replacement: %v", err)
	}

	// Repeated operands consume budget even though the evaluator can share
	// projected operand storage. Two rules have 16 * 32 = 512 values.
	list := strings.Repeat("1,", 31) + "1"
	source := strings.Repeat("SNR="+list+" OR ", 7) + "SNR=" + list
	duplicates := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, source}, RuleDefinition{9, RulePass, source})
	if got := duplicates.Counts().Normal; got.Atoms != 16 || got.Values != 512 {
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
	if set.Counts().Normal.Nodes != 128 {
		t.Fatal("node-bound fixture is not at the limit")
	}
	if _, err := set.WithRule(RuleDefinition{9, RulePass, "NOT " + nodes}); err == nil {
		t.Fatal("129-node expression admitted")
	}
}

func TestRuleSetProjectionCountsAndOperandSharing(t *testing.T) {
	set := mustPrepareRuleSet(t,
		RuleDefinition{0, RulePass, "MODE=CW,FT8 AND SNR=1,1 OR DXCONT=EU"},
		RuleDefinition{4, RuleReject, "DXZONE=5,5"},
		RuleDefinition{9, RulePass, "PATH=HIGH"})
	want := RuleSetCounts{
		Rules:    3,
		Normal:   RuleProgramCounts{Nodes: 7, Atoms: 5, Values: 8},
		Nearby:   RuleProgramCounts{Nodes: 4, Atoms: 3, Values: 5},
		Retained: RuleProgramCounts{Nodes: 10, Atoms: 7, Values: 8},
	}
	if got := set.Counts(); got != want {
		t.Fatalf("projection/shared-vector counts got %+v, want %+v", got, want)
	}
	if set.rules[2].normal != set.rules[2].nearby {
		t.Fatal("non-geographic rule did not reuse the program")
	}
	full, pruned := set.rules[0].normal, set.rules[0].nearby
	if full == pruned || pruned == nil || set.rules[1].nearby != nil {
		t.Fatal("mixed/suspended projections were not independently represented")
	}
	for i := range pruned.nodes {
		predicate := &pruned.nodes[i].predicate
		switch pruned.nodes[i].kind {
		case expr.AtomKind:
			shared := false
			for j := range full.nodes {
				original := &full.nodes[j].predicate
				if full.nodes[j].kind != expr.AtomKind || original.field != predicate.field {
					continue
				}
				if len(predicate.text) > 0 {
					shared = &predicate.text[0] == &original.text[0]
				} else if len(predicate.integers) > 0 {
					shared = &predicate.integers[0] == &original.integers[0]
				}
			}
			if !shared {
				t.Fatal("projection recompiled/copied immutable operands")
			}
		}
	}
	assertPreparedRulePrograms(t, set)
	compiled, err := compileRule("NOT MODE=CW OR SNR=1")
	if err != nil || compiled.root != compiled.syntax.Root {
		t.Fatalf("compiler root did not survive independently: %v", err)
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
			snapshots = append(snapshots, snapshot{set, set.Definitions(), set.Readback(true), set.Counts(),
				set.Matches(sp, "", false), set.Matches(sp, "", true)})
		}
		set, err = set.WithoutRule((i + 3) % 10)
		if err != nil {
			t.Fatal(err)
		}
		counts := set.Counts()
		if counts.Rules > 10 || counts.Normal.Nodes > counts.Rules*5 ||
			counts.Nearby.Nodes > counts.Rules*3 || counts.Retained.Nodes > counts.Rules*8 ||
			counts.Retained.Atoms > counts.Rules*5 || counts.Retained.Values > counts.Rules*3 {
			t.Fatalf("historical expressions accumulated at iteration %d: %+v", i, counts)
		}
		assertPreparedRulePrograms(t, set)
	}
	for _, saved := range snapshots {
		if !reflect.DeepEqual(saved.set.Definitions(), saved.definitions) ||
			!reflect.DeepEqual(saved.set.Readback(true), saved.readback) || saved.set.Counts() != saved.counts ||
			saved.set.Matches(sp, "", false) != saved.normal || saved.set.Matches(sp, "", true) != saved.near {
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
