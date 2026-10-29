package filter

// RuleProgramCounts describes logical program size; duplicate values count.
type RuleProgramCounts struct{ Nodes, Atoms, Values int }

type RuleSetCounts struct {
	Rules   int
	Program RuleProgramCounts
}

// RuleReadback is the single canonical, detached saved rule.
type RuleReadback struct{ RuleDefinition }

func (s *PreparedRuleSet) Readback() []RuleReadback {
	if s == nil {
		return nil
	}
	result := make([]RuleReadback, len(s.rules))
	for i := range s.rules {
		result[i].RuleDefinition = s.rules[i].definition
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

// Counts exposes bounded retained program cardinality without walking syntax.
// Dropping the last rule-set/snapshot reference releases each owned program.
func (s *PreparedRuleSet) Counts() RuleSetCounts {
	var counts RuleSetCounts
	if s != nil {
		counts.Rules = len(s.rules)
		for i := range s.rules {
			counts.Program.add(programRuleCounts(s.rules[i].program))
		}
	}
	return counts
}
