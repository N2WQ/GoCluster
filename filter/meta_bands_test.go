package filter

import (
	"reflect"
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestMetaBandGOExpansion(t *testing.T) {
	for _, test := range []struct{ source, canonical string }{
		{`BAND=warc`, `BAND=30M,17M,12M`},
		{`BAND=HFCONTEST`, `BAND=160M,80M,40M,20M,15M,10M`},
		{`BAND=HF`, `BAND=160M,80M,60M,40M,30M,20M,17M,15M,12M,10M`},
		{`BAND=VHF`, `BAND=6M,4M,2M,1.25M`},
		{`BAND=UHF`, `BAND=70CM,33CM,23CM,13CM`},
		{`BAND=VHFUP`, `BAND=6M,4M,2M,1.25M,70CM,33CM,23CM,13CM`},
		{`BAND=20M,WARC,30m,WARC,6`, `BAND=20M,30M,17M,12M,6M`},
		{`BAND<>[WARC,20M] AND NOT (MODE=CW OR BAND=VHF)`, `BAND<>30M,17M,12M,20M AND NOT (MODE=CW OR BAND=6M,4M,2M,1.25M)`},
		{`BAND=VHF AND COMMENT="WARC,HF AND BAND=UHF ""literal"""`, `BAND=6M,4M,2M,1.25M AND COMMENT="WARC,HF AND BAND=UHF ""literal"""`},
		{`BAND=20M,20m`, `BAND=20M,20M`},
		{`BAND=[warc,all,20M] AND COMMENT="BAND=ALL"`, `BAND=30M,17M,12M,ALL,20M AND COMMENT="BAND=ALL"`},
	} {
		program, err := compileRule(test.source)
		if err != nil || program.canonical != test.canonical {
			t.Fatalf("%s canonical = %v, %v; want %s", test.source, program, err, test.canonical)
		}
		again, err := compileRule(program.canonical)
		if err != nil || again.canonical != test.canonical || again.valueCount != program.valueCount {
			t.Fatalf("expanded canonical is not stable: %v, %v", again, err)
		}
	}
	for _, test := range []struct {
		source, band, mode string
		want               bool
	}{
		{`BAND=VHF`, "4m", "CW", true}, {`BAND=VHF`, "20m", "CW", false},
		{`BAND=UHF`, "13cm", "FT8", true}, {`BAND=UHF`, "6m", "CW", false},
		{`BAND<>WARC`, "20m", "CW", true}, {`BAND<>WARC`, "30m", "CW", false},
		{`BAND<>WARC`, "", "CW", false}, {`NOT BAND=WARC`, "", "CW", true},
		{`BAND=WARC,ALL`, "", "FT8", true},
		{`BAND<>ALL,WARC`, "20m", "FT8", false},
		{`BAND=WARC AND MODE=CW OR BAND=VHF`, "30m", "FT8", false},
		{`BAND=WARC AND MODE=CW OR BAND=VHF`, "4m", "FT8", true},
	} {
		program, err := compileRule(test.source)
		if err != nil {
			t.Fatal(err)
		}
		view := newRuleView(&spot.Spot{Band: test.band, Mode: test.mode}, "")
		if got := program.evaluate(&view, program.root); got != test.want {
			t.Fatalf("%s at %s/%s = %v, want %v", test.source, test.band, test.mode, got, test.want)
		}
	}
}

func TestMetaBandGOBudgetsAndAtomicPreparation(t *testing.T) {
	for _, source := range []string{
		`BAND=` + strings.Repeat("HF,", 32) + "HF",     // raw per-comparison limit still applies before deduplication.
		strings.Repeat("BAND=HF AND ", 20) + "BAND=HF", // short input expands past canonical byte limit.
		`BAND=WARC,BOGUS`, `BAND>WARC`,
	} {
		if program, err := compileRule(source); err == nil || program != nil {
			t.Fatalf("invalid expansion accepted: %s", source)
		}
	}
	physical, err := compileRule("BAND=" + strings.Repeat("20m,", 31) + "20m")
	if err != nil || physical.valueCount != 32 {
		t.Fatalf("physical-only duplicate accounting changed: %v %v", physical, err)
	}
	meta, err := compileRule("BAND=" + strings.Repeat("HF,", 31) + "HF")
	if err != nil || meta.rawValueCount != 32 || meta.valueCount != 10 {
		t.Fatalf("meta accounting = %v %v", meta, err)
	}

	// Two raw 256-operand rules are admissible even though canonical members
	// deduplicate to 160 operands. One more raw operand must still be refused.
	rawAtom := "BAND=" + strings.Repeat("HF,", 31) + "HF"
	rawRule := strings.Repeat(rawAtom+" AND ", 7) + rawAtom
	raw := []RuleDefinition{{Slot: 0, Action: RulePass, Expr: rawRule}, {Slot: 1, Action: RulePass, Expr: rawRule}}
	if _, err := PrepareRuleSet(raw); err != nil {
		t.Fatalf("raw total512 refused: %v", err)
	}
	if got, err := PrepareRuleSet(append(raw, RuleDefinition{Slot: 2, Action: RulePass, Expr: "BAND=HF"})); err == nil || got != nil {
		t.Fatal("raw total513 accepted")
	}

	definitions := make([]RuleDefinition, 5)
	for i := range definitions {
		definitions[i] = RuleDefinition{Slot: i, Action: RulePass, Expr: strings.Repeat("BAND=HF AND ", 9) + "BAND=HF"}
	}
	definitions = append(definitions, RuleDefinition{Slot: 5, Action: RulePass, Expr: "BAND=WARC,VHF,UHF,630m"})
	set, err := PrepareRuleSet(definitions)
	if err != nil {
		t.Fatalf("expanded total512 refused: %v", err)
	}
	before := set.Definitions()
	for _, source := range []string{"BAND=WARC,VHF,UHF,630m,2200m", "BAND=WARC,BOGUS"} {
		if got, err := set.WithRule(RuleDefinition{Slot: 5, Action: RulePass, Expr: source}); err == nil || got != nil {
			t.Fatalf("invalid replacement accepted: %s", source)
		}
		if !reflect.DeepEqual(set.Definitions(), before) {
			t.Fatal("failed preparation mutated existing set")
		}
	}
}

func TestMetaBandGORuleCombination(t *testing.T) {
	set, err := PrepareRuleSet([]RuleDefinition{
		{Slot: 0, Action: RulePass, Expr: "BAND=HF OR BAND=VHF"},
		{Slot: 9, Action: RuleReject, Expr: "BAND=WARC"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		band string
		want bool
	}{{"160m", true}, {"60m", true}, {"4m", true}, {"30m", false}, {"70cm", false}, {"", false}} {
		if got := set.Matches(&spot.Spot{Band: test.band}, EvaluationContext{}); got != test.want {
			t.Fatalf("combined rules at %s = %v, want %v", test.band, got, test.want)
		}
	}
}

func FuzzMetaBandGOExpansion(f *testing.F) {
	for _, source := range []string{
		"BAND=WARC", "BAND=HFCONTEST", "BAND=HF", "BAND=VHF", "BAND=UHF", "BAND=VHFUP",
		"BAND=WARC,20m,WARC", "BAND<>HF AND NOT BAND=VHF",
		"BAND=[WARC,ALL,20M]", `BAND=ALL,HF,"ALL"`, "NOT BAND<>ALL",
		`BAND=UHF AND COMMENT="WARC AND BAND=HF ""literal"""`,
		"BAND=VHF,BOGUS", "BAND=" + strings.Repeat("HF,", 32) + "HF",
	} {
		f.Add(source)
	}
	f.Fuzz(func(t *testing.T, source string) {
		program, err := compileRule(source)
		if err != nil {
			return
		}
		again, err := compileRule(program.canonical)
		if err != nil || again.canonical != program.canonical || again.valueCount != program.valueCount {
			t.Fatalf("canonical round trip failed: %q -> %q: %v", source, program.canonical, err)
		}
		// Matching the incoming prepared program and its saved/reloaded form
		// must agree, including unknown operands and Boolean negation. Literal
		// membership expectations live in TestMetaBandGOExpansion above.
		for _, band := range []string{"", "160m", "60m", "30m", "4m", "70cm", "13cm"} {
			sp := &spot.Spot{Band: band, Mode: "CW", Comment: `WARC AND BAND=HF "literal"`}
			left, right := newRuleView(sp, ""), newRuleView(sp, "")
			if program.evaluate(&left, program.root) != again.evaluate(&right, again.root) {
				t.Fatalf("canonical reload changed matching: %q at %s", source, band)
			}
		}
	})
}
