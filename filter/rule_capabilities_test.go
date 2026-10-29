package filter

import (
	"slices"
	"testing"
)

func TestRuleCapabilitiesAgreeWithCompiler(t *testing.T) {
	values := map[string]string{
		"BAND": "20M", "MODE": "CW", "SOURCE": "HUMAN", "EVENT": "NONE",
		"DXCALL": "N2WQ", "DECALL": "*ABC", "DXCONT": "NA", "DECONT": "EU",
		"DXZONE": "5", "DEZONE": "14", "DXDXCC": "291", "DEDXCC": "1",
		"DXSTATE": "CT", "DESTATE": "ON", "DXGRID2": "FN", "DEGRID2": "EM",
		"FREQ": "7000", "SNR": "0", "CONF": "?", "PATH": "HIGH", "COMMENT": `"POTA"`,
	}
	fields := SupportedRuleFields()
	if len(fields) != len(values) {
		t.Fatal("capabilities omitted or invented a field")
	}
	for _, name := range fields {
		value, exists := values[name]
		if !exists {
			t.Fatalf("unexpected field %q", name)
		}
		operators := RuleFieldOperators(name)
		for _, operator := range []string{"=", "<>", "<", "<=", ">", ">="} {
			_, err := compileRule(name + operator + value)
			if (err == nil) != slices.Contains(operators, operator) {
				t.Fatalf("field %s operator %s disagrees with admission: %v", name, operator, err)
			}
		}
		wantGeo := slices.Contains([]string{
			"DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC",
			"DXSTATE", "DESTATE", "DXGRID2", "DEGRID2",
		}, name)
		if IsGeographicRuleField(name) != wantGeo {
			t.Fatalf("incorrect geography classification for %s", name)
		}
		operators[0] = "mutated"
		if RuleFieldOperators(name)[0] != "=" {
			t.Fatal("operator metadata exposes mutable shared state")
		}
	}
	predicates := SupportedRulePredicates()
	if len(predicates) != 1 || predicates[0] != "NEARBY" || RuleFieldOperators("NEARBY") != nil {
		t.Fatal("predicate confused with comparison field")
	}
	if _, err := compileRule(predicates[0]); err != nil {
		t.Fatal(err)
	}
	predicates[0] = "mutated"
	if SupportedRulePredicates()[0] != "NEARBY" {
		t.Fatal("predicate inventory shares mutable storage")
	}

	fields[0] = "mutated"
	if SupportedRuleFields()[0] != "BAND" ||
		RuleFieldOperators("unknown") != nil || IsGeographicRuleField("unknown") {
		t.Fatal("vocabulary ownership or unknown lookup failed")
	}
}
