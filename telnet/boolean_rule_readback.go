// File role: Pure, bounded numbered-rule body; no routing, pause or footer owner.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
)

// Both passes use only already-prepared immutable rules. The first pass admits
// the complete body before allocating output; errors never return partial text.
func renderBooleanHumanBody(write func(*humanResponse) error) (string, error) {
	for _, countOnly := range []bool{true, false} {
		h := humanResponse{countOnly: countOnly}
		if err := write(&h); err != nil {
			return "", err
		}
		if !countOnly {
			return string(h.data), nil
		}
	}
	return "", nil
}

func writeBooleanSavedRule(h *humanResponse, rule filter.RuleDefinition) error {
	label := fmt.Sprintf("%s RULE %d", strings.ToUpper(string(rule.Action)), rule.Slot)
	if len(label)+1+len(rule.Expr) <= humanReadbackWidth {
		return h.line(label + " " + rule.Expr)
	}
	if err := h.quoted(humanPrefix(label), rule.Expr, ""); err != nil {
		return err
	}
	return h.row("", "Long rule: display quotes/escapes and + are not command syntax. Use YAML for long rules.")
}

// formatBooleanFilterSnapshot borrows a coherent, caller-owned snapshot. It
// never prepares rules, creates defaults or projects legacy category maps.
func formatBooleanFilterSnapshot(f *filter.Filter) (string, error) {
	return renderBooleanHumanBody(func(h *humanResponse) error {
		return writeBooleanFilterSnapshot(h, f)
	})
}

func writeBooleanFilterSnapshot(h *humanResponse, f *filter.Filter) error {
	if err := h.line("Numbered personal filter rules"); err != nil {
		return err
	}
	for _, text := range []string{
		"PASS rules are ORed; any matching REJECT wins. With no active PASS rule, personal rules allow all except matching REJECTs.",
		"Slot IDs are not priorities. Default slot 0 is an ordinary editable rule; adding another PASS broadens selection. Edit/remove slot 0 to narrow it.",
	} {
		if err := h.row("", text); err != nil {
			return err
		}
	}
	if err := writeBooleanRuleRows(h, f); err != nil {
		return err
	}
	if err := writeBooleanControls(h, f); err != nil {
		return err
	}
	return writeBooleanRuleCounts(h, f.PreparedRules().Counts())
}

func writeBooleanRuleRows(h *humanResponse, f *filter.Filter) error {
	rows := f.PreparedRules().Definitions()
	if len(rows) == 0 {
		return h.row("Rules", "None; all sources, modes and bands allowed.")
	}
	for _, rule := range rows {
		if err := writeBooleanSavedRule(h, rule); err != nil {
			return err
		}
	}
	return nil
}

func writeBooleanControls(h *humanResponse, f *filter.Filter) error {
	var controls [6]*bool
	if f != nil {
		controls = [6]*bool{f.IncludeBeacons, f.AllowWWV, f.AllowWCY, f.AllowAnnounce, f.AllowSelf, f.AllowToxic}
	}
	for _, control := range []struct {
		name  string
		value *bool
	}{
		{"BEACON", controls[0]}, {"WWV", controls[1]},
		{"WCY", controls[2]}, {"ANNOUNCE/PC93", controls[3]},
		{"SELF", controls[4]}, {"TOXIC", controls[5]},
	} {
		state := "DEFAULT (ON)"
		if value := filter.DefaultBoolFromPointer(control.value); value == filter.DefaultBoolFalse {
			state = "OFF"
		} else if value == filter.DefaultBoolTrue {
			state = "ON"
		}
		if err := h.row(control.name, state+"; separate control"); err != nil {
			return err
		}
	}
	return h.row("", "SELF ON can bypass ordinary personal rules; TOXIC still applies. Bulletin delivery uses its separate controls.")
}

func writeBooleanRuleCounts(h *humanResponse, counts filter.RuleSetCounts) error {
	if err := h.row("Counts", fmt.Sprintf("%d/%d occupied slots; atoms %d/%d; values %d/%d.",
		counts.Rules, filter.MaxRuleSlots, counts.Program.Atoms, filter.MaxTotalRuleAtoms,
		counts.Program.Values, filter.MaxTotalRuleValues)); err != nil {
		return err
	}
	return h.row("Program", fmt.Sprintf("nodes %d; atoms %d; values %d.",
		counts.Program.Nodes, counts.Program.Atoms, counts.Program.Values))
}
