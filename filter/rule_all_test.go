package filter

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

func TestRuleAllValuesByField(t *testing.T) {
	fields := strings.Fields("BAND MODE SOURCE EVENT DXCONT DECONT DXZONE DEZONE DXDXCC DEDXCC DXSTATE DESTATE DXGRID2 DEGRID2 FREQ SNR CONF PATH")
	known := normalizedRuleTestSpot()
	missing := &spot.Spot{Mode: "MSK144"} // Confidence-exempt and missing the other optional fields.
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			for _, test := range []struct {
				expression, canonical string
				want                  bool
			}{
				{field + "=[aLl]", field + "=ALL", true},
				{field + "<>ALL", field + "<>ALL", false},
				{"NOT " + field + "=ALL", "NOT " + field + "=ALL", false},
			} {
				set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, test.expression})
				if set.Definitions()[0].Expr != test.canonical || set.Counts().Program.Values != 1 {
					t.Fatalf("unexpected canonical definition or count for %q: %+v %+v", test.expression, set.Definitions(), set.Counts())
				}
				for _, value := range []*spot.Spot{known, missing} {
					if got := set.Matches(value, EvaluationContext{}); got != test.want {
						t.Fatalf("%q matched=%t, want %t, spot %+v", test.expression, got, test.want, value)
					}
				}
				if set.Matches(nil, EvaluationContext{}) || set.NeedsPath() {
					t.Fatalf("%q matched a nil spot or requested path prediction", test.expression)
				}
			}
		})
	}
}

func TestRuleAllRejectVeto(t *testing.T) {
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=CW"}, RuleDefinition{9, RuleReject, "BAND=ALL"})
	for _, value := range []*spot.Spot{{Mode: "CW", Band: "20m"}, {Mode: "CW"}} {
		if set.Matches(value, EvaluationContext{}) {
			t.Fatalf("BAND=ALL reject did not veto the passing CW spot: %+v", value)
		}
	}
}

func TestRuleAllPreservesTextMatching(t *testing.T) {
	for _, test := range []struct {
		expression string
		hit, miss  *spot.Spot
	}{
		{"DXCALL=ALL,K1*", &spot.Spot{DXCall: "ALL"}, &spot.Spot{DXCall: "ALL1A"}},
		{"DECALL=ALL", &spot.Spot{DECallStripped: "all"}, &spot.Spot{DECall: "ALL1A"}},
		{"COMMENT=AlL", &spot.Spot{Comment: "calling CQ"}, &spot.Spot{Comment: "CQ DX"}},
	} {
		set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, test.expression})
		if !set.Matches(test.hit, EvaluationContext{}) || set.Matches(test.miss, EvaluationContext{}) ||
			set.Matches(&spot.Spot{}, EvaluationContext{}) {
			t.Fatalf("%q did not retain literal matching", test.expression)
		}
	}
}

func TestRuleAllQuotedCatalogValuesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	const source = `modes:
  - name: ALL
    filter_visible: true
    variants: [NAMEDMODE]
  - name: CW
    filter_visible: true
events:
  - name: ALL
    filter_visible: true
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })

	selected := spot.Spot{Mode: "ALL", Events: taxonomy.EventMaskForName("ALL")}
	other := spot.Spot{Mode: "CW"}
	for _, test := range []struct{ input, canonical string }{
		{`MODE="all"`, `MODE="ALL"`},
		{`MODE=NAMEDMODE`, `MODE="ALL"`},
		{`EVENT="all"`, `EVENT="ALL"`},
	} {
		set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, test.input})
		if got := set.Definitions()[0].Expr; got != test.canonical {
			t.Fatalf("%q canonicalized to %q, want %q", test.input, got, test.canonical)
		}
		for _, rules := range []*PreparedRuleSet{set, mustPrepareRuleSet(t, set.Definitions()...)} {
			if !rules.Matches(&selected, EvaluationContext{}) || rules.Matches(&other, EvaluationContext{}) {
				t.Fatalf("%q widened on canonicalization", test.input)
			}
		}
	}
	for _, source := range []string{"MODE=ALL", "EVENT=ALL"} {
		rules := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, source})
		if !rules.Matches(&other, EvaluationContext{}) || rules.Definitions()[0].Expr != source {
			t.Fatalf("%q did not retain wildcard meaning with a same-named literal", source)
		}
	}
	defaults, err := PrepareDefaultRuleSet([]string{"ALL", "NAMEDMODE"}, []string{"SKIMMER"})
	if err != nil || defaults.Definitions()[0].Expr != `MODE="ALL" AND SOURCE=SKIMMER` ||
		!defaults.Matches(&selected, EvaluationContext{}) || defaults.Matches(&other, EvaluationContext{}) {
		t.Fatalf("configured GO defaults widened the literal ALL mode: %+v (%v)", defaults.Definitions(), err)
	}
	cc, err := ccConfigurationFromDefaultRules(defaults)
	if err != nil || len(cc.Modes) != 1 || cc.Modes[0] != "ALL" {
		t.Fatalf("configured CC defaults lost the literal ALL mode: %+v (%v)", cc.Modes, err)
	}
}

func TestRuleAllDiagnosticsAndLimits(t *testing.T) {
	for _, test := range []struct {
		source, message string
		column          int
	}{
		{"SNR>ALL", "Use SNR=ALL to match any value", 5},
		{"NOT (NEARBY = ALL)", "Use NEARBY on its own", 13},
		{"BAND=ALL,BOGUS", "unsupported BAND value", 10},
		{"BAND=WARC,ALL,BOGUS", "unsupported BAND value", 15},
		{`BAND=WARC,ALL,"ALL"`, "unsupported BAND value", 15},
		{"MODE=ALL,BOGUS", "unsupported MODE value", 10},
		{"SNR=ALL,BOGUS", "strictly integral value", 9},
	} {
		var diagnostic *expr.SyntaxError
		_, err := compileRule(test.source)
		if !errors.As(err, &diagnostic) || diagnostic.Column != test.column || !strings.Contains(diagnostic.Message, test.message) {
			t.Errorf("%q reported %v, want column %d containing %q", test.source, err, test.column, test.message)
		}
	}
	maxList := "BAND=ALL" + strings.Repeat(",20M", expr.MaxValuesPerAtom-1)
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, maxList})
	if set.Counts().Program.Values != expr.MaxValuesPerAtom {
		t.Fatal("redundant list members evaded the value budget")
	}
	if _, err := set.WithRule(RuleDefinition{0, RulePass, maxList + ",ALL"}); err == nil {
		t.Fatal("over-limit list was accepted")
	}
}
