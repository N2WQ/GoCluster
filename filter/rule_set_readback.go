package filter

// RuleProgramCounts describes logical program size; duplicate list values count.
type RuleProgramCounts struct {
	Nodes  int
	Atoms  int
	Values int
}

type RuleSetCounts struct {
	Rules    int
	Normal   RuleProgramCounts
	Nearby   RuleProgramCounts
	Retained RuleProgramCounts
}

// RuleReadback preserves the full canonical definition in RuleDefinition.Expr.
// EffectiveExpr is empty and Suspended is true only for an absent NEARBY rule.
// EffectiveExpr includes the implicit HUMAN guard when applicable; it describes
// behavior, not saved syntax, and can therefore exceed the parsed syntax budget.
type RuleReadback struct {
	RuleDefinition
	EffectiveExpr string
	Suspended     bool
	// ImplicitHumanOnly is true only for an active source-less PASS rule.
	// EffectiveExpr discloses this guard; saved Expr and syntax counts do not.
	ImplicitHumanOnly bool
}

// Readback returns at most ten detached records; toggling NEARBY never changes
// the saved definition used by later edits, persistence, or ordinary evaluation.
func (s *PreparedRuleSet) Readback(nearby bool) []RuleReadback {
	if s == nil {
		return nil
	}
	result := make([]RuleReadback, len(s.rules))
	for i := range s.rules {
		rule := &s.rules[i]
		result[i].RuleDefinition = rule.definition
		program := rule.program(nearby)
		result[i].Suspended = program == nil
		if program != nil {
			result[i].EffectiveExpr = program.canonical
			result[i].ImplicitHumanOnly = rule.humanOnly
			if rule.humanOnly {
				result[i].EffectiveExpr = "(" + program.canonical + ") AND SOURCE=HUMAN"
			}
		}
	}
	return result
}

func programRuleCounts(program *compiledRule) RuleProgramCounts {
	if program == nil {
		return RuleProgramCounts{}
	}
	return RuleProgramCounts{Nodes: len(program.nodes), Atoms: program.atomCount, Values: program.valueCount}
}

func (c *RuleProgramCounts) add(other RuleProgramCounts) {
	c.Nodes += other.Nodes
	c.Atoms += other.Atoms
	c.Values += other.Values
}

// Counts exposes bounds without walking syntax or allocating. Retained counts
// each distinct node vector once (and its atom entries); projected operand data
// is shared, so Retained.Values counts full-program values only, not references
// to those same immutable operands from a second vector.
func (s *PreparedRuleSet) Counts() RuleSetCounts {
	var counts RuleSetCounts
	if s != nil {
		counts.Rules = len(s.rules)
		for i := range s.rules {
			rule := &s.rules[i]
			normal, nearby := programRuleCounts(rule.normal), programRuleCounts(rule.nearby)
			counts.Normal.add(normal)
			counts.Nearby.add(nearby)
			counts.Retained.add(normal)
			if rule.nearby != rule.normal {
				counts.Retained.Nodes += nearby.Nodes
				counts.Retained.Atoms += nearby.Atoms
			}
		}
	}
	return counts
}
