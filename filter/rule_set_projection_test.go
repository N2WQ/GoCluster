package filter

import (
	"reflect"
	"testing"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

func TestRuleSetNearbyProjectionTruth(t *testing.T) {
	probes := []spot.Spot{
		{Mode: "CW", Band: "40m", DXMetadata: spot.CallMetadata{Continent: "NA"}, DEMetadata: spot.CallMetadata{Continent: "NA"}},
		{Mode: "FT8", Band: "20m", DXMetadata: spot.CallMetadata{Continent: "EU"}, DEMetadata: spot.CallMetadata{Continent: "NA"}},
		{Mode: "FT8", Band: "40m", DXMetadata: spot.CallMetadata{Continent: "NA"}, DEMetadata: spot.CallMetadata{Continent: "EU"}},
	}
	for i := range probes {
		probes[i].IsHuman = true
	}
	for _, test := range []struct {
		source, effective string
		normal, nearby    [3]bool
	}{
		{`MODE=CW OR DXCONT=EU`, `MODE=CW`, [3]bool{true, true, false}, [3]bool{true, false, false}},
		{`DXCONT=EU AND MODE=CW`, `MODE=CW`, [3]bool{false, false, false}, [3]bool{true, false, false}},
		{`NOT (DXCONT=EU AND MODE=CW)`, `NOT MODE=CW`, [3]bool{true, true, true}, [3]bool{false, true, true}},
		{`NOT (DXCONT=EU OR MODE=CW)`, `NOT MODE=CW`, [3]bool{false, false, true}, [3]bool{false, true, true}},
		{`(DXCONT=EU AND DECONT=NA) OR MODE=CW`, `MODE=CW`, [3]bool{true, true, false}, [3]bool{true, false, false}},
		{`(NOT DXCONT=EU) AND MODE=CW`, `MODE=CW`, [3]bool{true, false, false}, [3]bool{true, false, false}},
		{`(MODE=CW AND DXCONT=EU) OR (BAND=20m AND NOT DECONT=NA)`, `MODE=CW OR BAND=20M`,
			[3]bool{false, false, false}, [3]bool{true, true, false}},
		{`NOT ((DXCONT=EU OR DEZONE=5) AND (MODE=CW OR DEGRID2=FN))`, `NOT MODE=CW`,
			[3]bool{true, true, true}, [3]bool{false, true, true}},
		{`NOT (NOT DXCONT=EU OR NOT (DXZONE=14 AND MODE=CW))`, `NOT NOT MODE=CW`,
			[3]bool{false, false, false}, [3]bool{true, false, false}},
		{`DXCONT=EU OR (MODE=CW AND (DECONT=NA OR DEZONE=5))`, `MODE=CW`,
			[3]bool{true, true, false}, [3]bool{true, false, false}},
	} {
		t.Run(test.source, func(t *testing.T) {
			set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, test.source})
			reject := mustPrepareRuleSet(t, RuleDefinition{9, RuleReject, test.source})
			saved := set.Definitions()
			readback := set.Readback(true)
			if len(readback) != 1 || readback[0].EffectiveExpr != test.effective || readback[0].Suspended {
				t.Fatalf("effective got %#v, want %q", readback, test.effective)
			}
			for i := range probes {
				if got := set.Matches(&probes[i], "", false); got != test.normal[i] {
					t.Fatalf("normal probe %d got %v, want %v", i, got, test.normal[i])
				}
				if got := set.Matches(&probes[i], "", true); got != test.nearby[i] {
					t.Fatalf("nearby probe %d got %v, want %v", i, got, test.nearby[i])
				}
				if got := reject.Matches(&probes[i], "", false); got == test.normal[i] {
					t.Fatalf("ordinary reject probe %d failed to veto exactly its matching program", i)
				}
				if got := reject.Matches(&probes[i], "", true); got == test.nearby[i] {
					t.Fatalf("projected reject probe %d failed to veto exactly its matching program", i)
				}
			}
			if !reflect.DeepEqual(set.Definitions(), saved) || readback[0].Expr != saved[0].Expr {
				t.Fatal("projection lost original definition")
			}
			assertPreparedRulePrograms(t, set)
		})
	}
}

func TestRuleSetNearbySuspensionAndMultiplePasses(t *testing.T) {
	for _, action := range []RuleAction{RulePass, RuleReject} {
		for _, source := range []string{
			"DXCONT=EU", "NOT (DXCONT=EU OR DEZONE=5)",
			"DXSTATE=CT AND DESTATE=ON OR DXDXCC=291 AND DEDXCC=1 OR DXGRID2=FN AND DEGRID2=EM",
		} {
			set := mustPrepareRuleSet(t, RuleDefinition{0, action, source})
			if source == "DXCONT=EU" {
				for _, continent := range []string{"EU", "NA"} {
					want := continent == "EU"
					if action == RuleReject {
						want = !want
					}
					if got := set.Matches(&spot.Spot{IsHuman: true, DXMetadata: spot.CallMetadata{Continent: continent}}, "", false); got != want {
						t.Fatal("geography-only rule did not retain its ordinary meaning")
					}
				}
			}
			for _, mode := range []string{"CW", "FT8"} {
				if !set.Matches(&spot.Spot{Mode: mode}, "", true) {
					t.Fatalf("suspended %s rule restricted mode %s: %s", action, mode, source)
				}
			}
			readback := set.Readback(true)[0]
			if !readback.Suspended || readback.EffectiveExpr != "" || readback.Expr == "" || set.rules[0].nearby != nil {
				t.Fatal("geography-only program is not absent")
			}
			if set.Readback(false)[0].Suspended || set.Counts().Nearby != (RuleProgramCounts{}) {
				t.Fatal("suspension/counts leaked into ordinary mode")
			}
		}
	}
	set := mustPrepareRuleSet(t,
		RuleDefinition{0, RulePass, "DXCONT=EU"},
		RuleDefinition{4, RuleReject, "DECONT=NA"},
		RuleDefinition{9, RulePass, "MODE=CW"})
	for _, probe := range []struct {
		mode string
		want bool
	}{{"CW", true}, {"FT8", false}} {
		if got := set.Matches(&spot.Spot{Mode: probe.mode, IsHuman: true}, "", true); got != probe.want {
			t.Fatalf("geo-only pass broadened remaining pass: %s got %v", probe.mode, got)
		}
	}
	// Editing while NEARBY is in use must replace the full saved rule too.
	changed, err := set.WithRule(RuleDefinition{0, RulePass, "MODE=FT8 AND DXCONT=NA"})
	if err != nil {
		t.Fatal(err)
	}
	if !changed.Matches(&spot.Spot{Mode: "FT8", IsHuman: true}, "", true) ||
		changed.Matches(&spot.Spot{Mode: "FT8", IsHuman: true, DXMetadata: spot.CallMetadata{Continent: "EU"}}, "", false) ||
		!changed.Matches(&spot.Spot{Mode: "FT8", IsHuman: true, DXMetadata: spot.CallMetadata{Continent: "NA"}}, "", false) {
		t.Fatal("NEARBY edit failed to retain latest full rule")
	}
	if set.Matches(&spot.Spot{Mode: "FT8"}, "", true) {
		t.Fatal("NEARBY edit mutated original snapshot")
	}
}

func TestRuleSetNearbyNonGeographicAndPath(t *testing.T) {
	source := `DXCONT=EU OR (PATH=HIGH AND COMMENT="POTA * ?" AND DXCALL=K1* AND DECALL=N2ABC AND ` +
		`MODE=FT8 AND BAND=20m AND SOURCE=SKIMMER AND SNR=0)`
	set := mustPrepareRuleSet(t, RuleDefinition{9, RulePass, source})
	want := `PATH=HIGH AND COMMENT="POTA * ?" AND DXCALL=K1* AND DECALL=N2ABC AND MODE=FT8 AND BAND=20M AND SOURCE=SKIMMER AND SNR=0`
	if got := set.Readback(true)[0].EffectiveExpr; got != want {
		t.Fatalf("non-geographic fields changed: %s", got)
	}
	if !set.NeedsPath(false) || !set.NeedsPath(true) {
		t.Fatal("PATH dependency was lost")
	}
	sp := normalizedRuleTestSpot()
	if !set.Matches(sp, PathClassHigh, true) || set.Matches(sp, PathClassClosed, true) {
		t.Fatal("projected PATH no longer applies")
	}
	sp.Comment = "POTA without literal punctuation"
	if set.Matches(sp, PathClassHigh, true) {
		t.Fatal("COMMENT was removed or interpreted as a wildcard")
	}
	noPath := mustPrepareRuleSet(t, RuleDefinition{0, RuleReject, "DXCONT=EU OR MODE=CW"})
	if noPath.NeedsPath(false) || noPath.NeedsPath(true) {
		t.Fatal("unrelated geographic projection introduced PATH")
	}
	onlyRejectPath := mustPrepareRuleSet(t, RuleDefinition{0, RuleReject, "PATH=CLOSED"})
	if !onlyRejectPath.NeedsPath(false) || !onlyRejectPath.NeedsPath(true) {
		t.Fatal("reject PATH dependency was omitted")
	}
	assertPreparedRulePrograms(t, set)
}

func assertPreparedRulePrograms(t testing.TB, set *PreparedRuleSet) {
	t.Helper()
	for i := range set.rules {
		rule := &set.rules[i]
		for _, program := range []*compiledRule{rule.normal, rule.nearby} {
			if program == nil {
				continue
			}
			if program.syntax != nil || len(program.nodes) > expr.MaxNodes || cap(program.nodes) != len(program.nodes) {
				t.Fatal("prepared program retained syntax or oversized vector")
			}
			var seen [expr.MaxNodes]bool
			atoms, nodes := 0, 0
			var walk func(int)
			walk = func(index int) {
				if index < 0 || index >= len(program.nodes) || seen[index] {
					t.Fatal("prepared root/child is missing, shared, or cyclic")
				}
				seen[index] = true
				nodes++
				node := &program.nodes[index]
				switch node.kind {
				case expr.AtomKind:
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
				t.Fatal("prepared vector has unreachable nodes or wrong atom count")
			}
			recompiled, err := compileRule(program.canonical)
			if err != nil || recompiled.atomCount != program.atomCount || recompiled.valueCount != program.valueCount {
				t.Fatalf("effective canonical text disagrees with typed program: %v", err)
			}
		}
	}
}
