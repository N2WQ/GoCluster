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
	definition RuleDefinition
	program    *compiledRule
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
		rule, err := prepareRule(definition, nil)
		if err != nil {
			return nil, err
		}
		atoms += rule.program.atomCount
		values += rule.program.valueCount
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
// without a separate projection or user-specific representation.
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
	return s.WithRuleDXCCResolver(definition, nil)
}

// WithRuleDXCCResolver resolves canonical DXCC operands during preparation only.
// The caller owns one resolver per request; neither it nor its state is retained.
// Nil preserves numeric-only preparation for native records, presets and defaults.
func (s *PreparedRuleSet) WithRuleDXCCResolver(definition RuleDefinition, resolve func(string) (int, error)) (*PreparedRuleSet, error) {
	if err := validateRuleDefinition(definition); err != nil {
		return nil, err
	}
	rule, err := prepareRule(definition, resolve)
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
	atoms, values := rule.program.atomCount, rule.program.valueCount
	for i := range previous {
		if replace && i == index {
			continue
		}
		atoms += previous[i].program.atomCount
		values += previous[i].program.valueCount
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

// Matches evaluates personal rules only. One lazy view caches proximity and
// normalized fields for this spot/user across every PASS and REJECT rule.
func (s *PreparedRuleSet) Matches(sp *spot.Spot, context EvaluationContext) bool {
	if sp == nil {
		return false
	}
	if s == nil {
		return true
	}
	view := newRuleView(sp, context.PathClass)
	view.fine, view.coarse = context.FineCell, context.CoarseCell
	for i := range s.rules {
		rule := &s.rules[i]
		if rule.definition.Action == RuleReject && rule.program.evaluate(&view, rule.program.root) {
			return false
		}
	}
	hasPass := false
	for i := range s.rules {
		rule := &s.rules[i]
		if rule.definition.Action == RulePass {
			hasPass = true
			if rule.program.evaluate(&view, rule.program.root) {
				return true
			}
		}
	}
	return !hasPass
}

func (s *PreparedRuleSet) NeedsPath() bool {
	if s != nil {
		for i := range s.rules {
			if s.rules[i].program.needsPath {
				return true
			}
		}
	}
	return false
}

func (s *PreparedRuleSet) NeedsNearby() bool {
	if s != nil {
		for i := range s.rules {
			if s.rules[i].program.needsNearby {
				return true
			}
		}
	}
	return false
}

func prepareRule(definition RuleDefinition, resolve func(string) (int, error)) (preparedRule, error) {
	program, err := compileRuleWithDXCCResolver(definition.Expr, resolve)
	if err != nil {
		return preparedRule{}, fmt.Errorf("rule slot %d: %w", definition.Slot, err)
	}
	definition.Expr = program.canonical
	if definition.Action == RulePass {
		definition.Action = RulePass
	} else {
		definition.Action = RuleReject
	}
	// Only admission needs syntax; published programs own a single bounded node
	// vector and detached operands, never uploaded trees or per-user state.
	program.syntax = nil
	return preparedRule{definition: definition, program: program}, nil
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
