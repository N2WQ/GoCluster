package telnet

import (
	"errors"
	"strconv"

	"dxcluster/filter"
	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

func decodeV5MachineRules(node *yaml.Node, path string) (*filter.PreparedRuleSet, error) {
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return nil, schemaError(path, "expected a sequence")
	}
	if len(node.Content) > filter.MaxRuleSlots {
		return nil, schemaError(path, "exceeds 10-rule limit")
	}
	// Check all identities before WithRule, whose normal editing semantics
	// deliberately replace an existing slot. A wire list may not do so.
	var seen [filter.MaxRuleSlots]bool
	var definitions [filter.MaxRuleSlots]filter.RuleDefinition
	for i, item := range node.Content {
		itemPath := path + "[" + strconv.Itoa(i) + "]"
		fields, err := machineObject(item, itemPath, []string{"slot", "action", "expr"}, true)
		if err != nil {
			return nil, err
		}
		slot, err := machineInteger(fields["slot"], itemPath+".slot", false)
		if err != nil {
			return nil, err
		}
		if slot < 0 || slot >= filter.MaxRuleSlots {
			return nil, schemaError(itemPath+".slot", "expected slot 0..9")
		}
		if seen[slot] {
			return nil, schemaError(itemPath+".slot", "duplicate rule slot")
		}
		seen[slot] = true
		action, err := machineString(fields["action"], itemPath+".action")
		if err != nil {
			return nil, err
		}
		if action != string(filter.RulePass) && action != string(filter.RuleReject) {
			return nil, schemaError(itemPath+".action", "expected lowercase pass or reject")
		}
		source, err := machineString(fields["expr"], itemPath+".expr")
		if err != nil {
			return nil, err
		}
		if len(source) > expr.MaxBytes {
			return nil, schemaError(itemPath+".expr", "col 1025: exceeds 1024-byte expression limit")
		}
		definitions[i] = filter.RuleDefinition{Slot: slot, Action: filter.RuleAction(action), Expr: source}
	}
	var set *filter.PreparedRuleSet
	for i := range node.Content {
		next, err := set.WithRule(definitions[i])
		if err != nil {
			reason := "col 1: " + err.Error()
			var syntax *expr.SyntaxError
			if errors.As(err, &syntax) {
				reason = syntax.Error()
			}
			return nil, schemaError(path+"["+strconv.Itoa(i)+"].expr", reason)
		}
		set = next
	}
	return set, nil
}
