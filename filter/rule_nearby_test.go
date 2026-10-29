package filter

import (
	"sync"
	"testing"

	"dxcluster/filter/expr"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestRuleNearbyBooleanTruth(t *testing.T) {
	context := EvaluationContext{FineCell: 10, CoarseCell: 11}
	for _, tc := range []struct {
		source             string
		near, far, unknown bool
	}{
		{"NEARBY", true, false, false}, {"NOT NEARBY", false, true, true},
		{"NEARBY AND DXCONT=EU", false, false, false},
		{"NEARBY OR DXCONT=EU", true, true, true},
		{"MODE=CW AND (DXCONT=EU OR NEARBY)", true, true, true},
		{"NEARBY AND SOURCE=HUMAN", false, false, false},
		{`COMMENT="NEARBY"`, false, false, false},
	} {
		t.Run(tc.source, func(t *testing.T) {
			set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, tc.source})
			reject := mustPrepareRuleSet(t, RuleDefinition{9, RuleReject, tc.source})
			near := &spot.Spot{Mode: "CW", BandNorm: "20m", DXCellID: 10, DXMetadata: spot.CallMetadata{Continent: "NA"}}
			far := &spot.Spot{Mode: "CW", BandNorm: "20m", DXCellID: 12, DXMetadata: spot.CallMetadata{Continent: "EU"}}
			unknown := &spot.Spot{Mode: "CW", DXMetadata: spot.CallMetadata{Continent: "EU"}}
			for i, sp := range []*spot.Spot{near, far, unknown} {
				want := []bool{tc.near, tc.far, tc.unknown}[i]
				if set.Matches(sp, context) != want || reject.Matches(sp, context) == want {
					t.Fatalf("probe %d truth/veto wrong", i)
				}
			}
			if set.Matches(nil, context) {
				t.Fatal("nil spot accepted")
			}
			assertPreparedRulePrograms(t, set)
		})
	}
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=FT8"}, RuleDefinition{9, RulePass, "NEARBY"})
	if !set.Matches(&spot.Spot{Mode: "FT8"}, EvaluationContext{}) {
		t.Fatal("NEARBY added a global geometry gate")
	}
	if !set.Matches(&spot.Spot{Mode: "CW", BandNorm: "20m", DXCellID: 10}, context) {
		t.Fatal("separate PASS did not broaden")
	}
	set = mustPrepareRuleSet(t, RuleDefinition{9, RuleReject, "NEARBY"})
	if set.Matches(&spot.Spot{BandNorm: "20m", DXCellID: 10}, context) || !set.Matches(&spot.Spot{}, context) {
		t.Fatal("reject-only semantics changed")
	}
}

func TestRuleNearbyLazyEvaluation(t *testing.T) {
	for _, source := range []string{"MODE=CW OR NEARBY", "MODE=FT8 AND NEARBY"} {
		program, err := compileRule(source)
		if err != nil {
			t.Fatal(err)
		}
		view := newRuleView(&spot.Spot{Mode: "CW", BandNorm: "20m", DXCellID: 10}, "")
		view.fine = 10
		program.evaluate(&view, program.root)
		if view.nearbyLoaded {
			t.Fatalf("unreached proximity calculated for %s", source)
		}
	}
	for _, want := range []bool{false, true} {
		s := &spot.Spot{BandNorm: "20m", DXCellID: 10}
		view := newRuleView(s, "")
		view.fine = 11
		if want {
			view.fine = 10
		}
		if view.nearbyLoaded || view.nearbyValue() != want || !view.nearbyLoaded {
			t.Fatal("first result not computed lazily")
		}
		// Alter the test-only view inputs after the first read to distinguish reuse
		// of both true and false from a second conversion/evaluation.
		view.fine = 10
		if want {
			view.fine = 11
		}
		if view.nearbyValue() != want {
			t.Fatal("cached true/false was recomputed")
		}
	}
	comment := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, `COMMENT="NEARBY"`})
	if comment.NeedsNearby() {
		t.Fatal("literal text introduced a geometry dependency")
	}
}

func TestRuleNearbySharedContexts(t *testing.T) {
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "NEARBY OR NEARBY"}, RuleDefinition{9, RuleReject, "NOT NEARBY"})
	s := &spot.Spot{BandNorm: "20m", DXCellID: 10}
	var workers sync.WaitGroup
	failures := make(chan bool, 4)
	for worker := range 4 {
		workers.Go(func() {
			for i := range 1000 {
				cell := pathreliability.CellID(10 + (worker+i)%2)
				if set.Matches(s, EvaluationContext{FineCell: cell}) != (cell == 10) {
					failures <- true
					return
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for range failures {
		t.Error("shared program retained another user's result")
	}
	if !set.NeedsNearby() || set.NeedsPath() {
		t.Fatal("prepared dependencies wrong")
	}
	if set.Matches(s, EvaluationContext{}) {
		t.Fatal("unavailable context bypassed predicate")
	}
}

func assertPreparedRulePrograms(t testing.TB, set *PreparedRuleSet) {
	t.Helper()
	if set == nil {
		return
	}
	for i := range set.rules {
		program := set.rules[i].program
		if program == nil || program.syntax != nil || len(program.nodes) > expr.MaxNodes || cap(program.nodes) != len(program.nodes) {
			t.Fatal("prepared program retained syntax or oversized vector")
		}
		var seen [expr.MaxNodes]bool
		atoms, nodes := 0, 0
		var walk func(int)
		walk = func(index int) {
			if index < 0 || index >= len(program.nodes) || seen[index] {
				t.Fatal("prepared root/child missing, shared or cyclic")
			}
			seen[index] = true
			nodes++
			node := &program.nodes[index]
			switch node.kind {
			case expr.AtomKind, expr.PredicateKind:
				atoms++
			case expr.NotKind:
				walk(node.left)
			case expr.AndKind, expr.OrKind:
				walk(node.left)
				walk(node.right)
			default:
				t.Fatal("invalid prepared node kind")
			}
		}
		walk(program.root)
		if atoms != program.atomCount || nodes != len(program.nodes) {
			t.Fatal("unreachable program nodes or wrong count")
		}
		again, err := compileRule(program.canonical)
		if err != nil || again.atomCount != program.atomCount || again.valueCount != program.valueCount {
			t.Fatalf("canonical disagrees: %v", err)
		}
	}
}
