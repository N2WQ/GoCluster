package filter

import "dxcluster/filter/expr"

// SupportedRuleFields returns a detached canonical vocabulary. Compilation,
// HELP and machine capabilities share the same finite field-name owner.
func SupportedRuleFields() []string {
	result := make([]string, int(ruleFieldNearby))
	copy(result, ruleFieldNames[:ruleFieldNearby])
	return result
}

// RuleFieldOperators reports accepted operator spellings for a canonical field.
// Unknown names have no operators; this is a lookup, not input admission.
func RuleFieldOperators(name string) []string {
	field, known := ruleFieldForName(name)
	if !known || field == ruleFieldNearby {
		return nil
	}
	operators := []string{string(expr.Equal), string(expr.NotEqual)}
	switch field {
	case ruleFieldFreq, ruleFieldSNR, ruleFieldDXZone, ruleFieldDEZone:
		operators = append(operators, string(expr.Less), string(expr.LessEqual), string(expr.Greater), string(expr.GreaterEqual))
	}
	return operators
}

// SupportedRulePredicates separates operand-free syntax from comparisons.
func SupportedRulePredicates() []string { return []string{"NEARBY"} }

func IsGeographicRuleField(name string) bool {
	field, known := ruleFieldForName(name)
	return known && isGeographicField(field)
}
