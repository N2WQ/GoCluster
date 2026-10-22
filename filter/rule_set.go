// File role: Bounded immutable personal rules, independent of active Filter state.
package filter

import (
	"fmt"
	"sort"

	"dxcluster/spot"
)

const (
	MaxRuleSlots       = 10
	MaxTotalRuleAtoms  = 128
	MaxTotalRuleValues = 512
)

type RuleAction string

const (
	RulePass   RuleAction = "pass"
	RuleReject RuleAction = "reject"
)

type RuleDefinition struct {
	Slot   int        `yaml:"slot"`
	Action RuleAction `yaml:"action"`
	Expr   string     `yaml:"expr"`
}

type preparedRule struct {
	definition     RuleDefinition
	normal, nearby *compiledRule
	humanOnly      bool
}

// PreparedRuleSet owns at most ten sorted slots. Programs and their operands
// freeze before publication; edits copy slots and share only immutable programs.
// There is no history or secondary cache: dropping a set releases its ownership.
// A nil set is the empty, unrestricted personal rule set.
type PreparedRuleSet struct {
	rules []preparedRule
}

// PrepareRuleSet validates the complete replacement without retaining or
// changing caller-owned definitions. Slot order is readback order, not priority.
func PrepareRuleSet(definitions []RuleDefinition) (*PreparedRuleSet, error) {
	if len(definitions) > MaxRuleSlots {
		return nil, fmt.Errorf("rules exceed slot limit %d", MaxRuleSlots)
	}
	var seen [MaxRuleSlots]bool
	for _, definition := range definitions {
		if err := validateRuleDefinition(definition); err != nil {
			return nil, err
		}
		if seen[definition.Slot] {
			return nil, fmt.Errorf("duplicate rule slot %d", definition.Slot)
		}
		seen[definition.Slot] = true
	}
	rules := make([]preparedRule, 0, len(definitions))
	atoms, values := 0, 0
	for _, definition := range definitions {
		rule, err := prepareRule(definition)
		if err != nil {
			return nil, err
		}
		atoms += rule.normal.atomCount
		values += rule.normal.valueCount
		if err := validateRuleTotals(atoms, values); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].definition.Slot < rules[j].definition.Slot })
	return publishRuleSet(rules), nil
}

func validateRuleSlot(slot int) error {
	if slot < 0 || slot >= MaxRuleSlots {
		return fmt.Errorf("rule slot %d must be 0..%d", slot, MaxRuleSlots-1)
	}
	return nil
}

func validateRuleDefinition(definition RuleDefinition) error {
	if err := validateRuleSlot(definition.Slot); err != nil {
		return err
	}
	if definition.Action != RulePass && definition.Action != RuleReject {
		return fmt.Errorf("rule slot %d action must be pass or reject", definition.Slot)
	}
	return nil
}

func validateRuleTotals(atoms, values int) error {
	if atoms > MaxTotalRuleAtoms {
		return fmt.Errorf("rules exceed total atom limit %d", MaxTotalRuleAtoms)
	}
	if values > MaxTotalRuleValues {
		return fmt.Errorf("rules exceed total value limit %d", MaxTotalRuleValues)
	}
	return nil
}

func publishRuleSet(rules []preparedRule) *PreparedRuleSet {
	if len(rules) == 0 {
		return nil
	}
	return &PreparedRuleSet{rules: rules}
}

// Definitions returns a detached, slot-sorted slice of full canonical rules,
// including geography even when the caller is currently using NEARBY.
func (s *PreparedRuleSet) Definitions() []RuleDefinition {
	if s == nil {
		return nil
	}
	result := make([]RuleDefinition, len(s.rules))
	for i := range s.rules {
		result[i] = s.rules[i].definition
	}
	return result
}

// WithRule prepares only the changed slot. Budget failures leave this set and
// every previously published snapshot untouched.
func (s *PreparedRuleSet) WithRule(definition RuleDefinition) (*PreparedRuleSet, error) {
	if err := validateRuleDefinition(definition); err != nil {
		return nil, err
	}
	rule, err := prepareRule(definition)
	if err != nil {
		return nil, err
	}
	var previous []preparedRule
	if s != nil {
		previous = s.rules
	}
	count := len(previous)
	index := sort.Search(count, func(i int) bool { return previous[i].definition.Slot >= definition.Slot })
	replace := index < count && previous[index].definition.Slot == definition.Slot
	if !replace {
		count++
	}
	if count > MaxRuleSlots {
		return nil, fmt.Errorf("rules exceed slot limit %d", MaxRuleSlots)
	}
	atoms, values := rule.normal.atomCount, rule.normal.valueCount
	for i := range previous {
		if replace && i == index {
			continue
		}
		atoms += previous[i].normal.atomCount
		values += previous[i].normal.valueCount
	}
	if err := validateRuleTotals(atoms, values); err != nil {
		return nil, err
	}
	rules := make([]preparedRule, count)
	copy(rules, previous[:index])
	rules[index] = rule
	tail := index
	if replace {
		tail++
	}
	copy(rules[index+1:], previous[tail:])
	return publishRuleSet(rules), nil
}

func (s *PreparedRuleSet) WithoutRule(slot int) (*PreparedRuleSet, error) {
	if err := validateRuleSlot(slot); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil //nolint:nilnil // Nil is the valid empty rule set.
	}
	index := sort.Search(len(s.rules), func(i int) bool { return s.rules[i].definition.Slot >= slot })
	if index == len(s.rules) || s.rules[index].definition.Slot != slot {
		return s, nil
	}
	if len(s.rules) == 1 {
		return nil, nil //nolint:nilnil // Removing the last slot produces the valid empty set.
	}
	rules := make([]preparedRule, len(s.rules)-1)
	copy(rules, s.rules[:index])
	copy(rules[index:], s.rules[index+1:])
	return publishRuleSet(rules), nil
}

func (r *preparedRule) program(nearby bool) *compiledRule {
	if nearby {
		return r.nearby
	}
	return r.normal
}

// Matches evaluates personal rules only. The caller still owns NEARBY geometry
// and non-rule controls. Absent projected rules do not contribute a PASS or veto.
func (s *PreparedRuleSet) Matches(sp *spot.Spot, pathClass string, nearby bool) bool {
	if sp == nil {
		return false
	}
	if s == nil {
		return true
	}
	view := newRuleView(sp, pathClass)
	for i := range s.rules {
		rule := &s.rules[i]
		if rule.definition.Action == RuleReject {
			if program := rule.program(nearby); program != nil && program.evaluate(&view, program.root) {
				return false
			}
		}
	}
	hasPass := false
	for i := range s.rules {
		rule := &s.rules[i]
		if rule.definition.Action == RulePass {
			if program := rule.program(nearby); program != nil {
				hasPass = true
				if (!rule.humanOnly || sp.IsHuman) && program.evaluate(&view, program.root) {
					return true
				}
			}
		}
	}
	return !hasPass
}

func (s *PreparedRuleSet) NeedsPath(nearby bool) bool {
	if s != nil {
		for i := range s.rules {
			if program := s.rules[i].program(nearby); program != nil && program.needsPath {
				return true
			}
		}
	}
	return false
}

func (s *PreparedRuleSet) Equal(other *PreparedRuleSet) bool {
	if s == other {
		return true
	}
	var left, right []preparedRule
	if s != nil {
		left = s.rules
	}
	if other != nil {
		right = other.rules
	}
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].definition != right[i].definition {
			return false
		}
	}
	return true
}
