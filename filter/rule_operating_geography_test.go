package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestRuleOperatingGeography(t *testing.T) {
	value := &spot.Spot{
		DXMetadata: spot.CallMetadata{Continent: "EU", ADIF: 230},
		DEMetadata: spot.CallMetadata{Continent: "NA", ADIF: 1},
	}
	canada := EvaluationContext{OperatingGeography: OperatingGeography{Continent: "NA", DXCC: 1}}
	germany := EvaluationContext{OperatingGeography: OperatingGeography{Continent: "EU", DXCC: 230}}
	unknown := EvaluationContext{}
	for _, tc := range []struct {
		rule     string
		context  EvaluationContext
		spot     *spot.Spot
		expected bool
	}{
		{"DECONT=MYCONT AND DEDXCC=MYDXCC", canada, value, true},
		{"DXCONT=MYCONT AND DXDXCC=MYDXCC", germany, value, true},
		{"DECONT=MYCONT AND DEDXCC=MYDXCC", germany, value, false},
		{"DXCONT=MYCONT AND DXDXCC=MYDXCC", canada, value, false},
		{"DXCONT<>MYCONT,AS AND DEDXCC<>MYDXCC,291", germany, value, false},
		{"DXCONT<>MYCONT,AS AND DXDXCC<>MYDXCC,291", canada, value, true},
		{"DXCONT<>MYCONT,EU OR DXDXCC<>MYDXCC,230", canada, value, false},
		{"DXCONT=MYCONT OR DEDXCC=MYDXCC", unknown, value, false},
		{"DXCONT<>MYCONT,AS OR DEDXCC<>MYDXCC,291", unknown, value, false},
		{"DXCONT=MYCONT,EU AND DEDXCC=MYDXCC,1", unknown, value, true},
		{"NOT DXCONT=MYCONT AND NOT DEDXCC=MYDXCC", unknown, value, true},
		{"DXCONT=MYCONT OR DXDXCC<>MYDXCC", canada, &spot.Spot{}, false},
		{"DXCONT=MYCONT,ALL AND DEDXCC=MYDXCC,ALL", unknown, &spot.Spot{}, true},
		{"DXCONT<>MYCONT,ALL OR DEDXCC<>MYDXCC,ALL", canada, value, false},
	} {
		t.Run(tc.rule, func(t *testing.T) {
			set, err := PrepareRuleSet([]RuleDefinition{{Slot: 0, Action: RulePass, Expr: tc.rule}})
			if err != nil {
				t.Fatal(err)
			}
			if got := set.Matches(tc.spot, tc.context); got != tc.expected {
				t.Fatalf("match=%v, want %v", got, tc.expected)
			}
		})
	}
}

func TestCompileOperatingGeography(t *testing.T) {
	program, err := compileRuleWithDXCCResolver(`decont=[mycont,eu] AND dxdxcc=mydxcc,k AND COMMENT=MyDXCC`, func(label string) (int, error) {
		if label != "K" {
			t.Fatalf("unexpected CTY label resolution for %q", label)
		}
		return 291, nil
	}, true)
	const canonical = `DECONT=MYCONT,EU AND DXDXCC=MYDXCC,291 AND COMMENT="MyDXCC"`
	if err != nil || program.canonical != canonical || program.valueCount != 5 || program.rawValueCount != 5 {
		t.Fatalf("got %+v (%v), want %q and five operands", program, err, canonical)
	}
	if _, err := compileRule(canonical); err != nil {
		t.Fatalf("canonical rule needs no CTY resolver: %v", err)
	}
	for _, source := range []string{`DECONT="MYCONT"`, `DXDXCC="MYDXCC"`, `DXZONE=MYDXCC`, `DXCONT=MYDXCC`, `DECONT>MYCONT`, `DXDXCC>MYDXCC`, `DECONT=ALL,MYDXCC`} {
		if _, err := compileRule(source); err == nil {
			t.Errorf("accepted invalid rule %q", source)
		}
	}
	for _, source := range []string{`DECONT=MYCONT`, `DXDXCC=MYDXCC`} {
		if _, err := compileRuleWithDXCCResolver(source, nil, false); err == nil {
			t.Errorf("accepted %q under the retired operand grammar", source)
		}
	}
}

func BenchmarkRuleOperatingGeography(b *testing.B) {
	value := &spot.Spot{
		DXMetadata: spot.CallMetadata{Continent: "EU", ADIF: 230},
		DEMetadata: spot.CallMetadata{Continent: "NA", ADIF: 1},
	}
	context := EvaluationContext{OperatingGeography: OperatingGeography{Continent: "NA", DXCC: 1}}
	for name, rule := range map[string]string{
		"session": `DECONT=MYCONT AND DXDXCC<>MYDXCC`,
		"literal": `DECONT=NA AND DXDXCC<>1`,
	} {
		b.Run(name, func(b *testing.B) {
			set, err := PrepareRuleSet([]RuleDefinition{{Slot: 0, Action: RulePass, Expr: rule}})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if !set.Matches(value, context) {
					b.Fatal("expected a match")
				}
			}
		})
	}
}
