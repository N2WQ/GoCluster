// File role: Pure, bounded numbered-rule body; no routing, pause or footer owner.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/pathreliability"
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
	if err := writeBooleanNearby(h, f); err != nil {
		return err
	}
	if err := writeBooleanControls(h, f); err != nil {
		return err
	}
	return writeBooleanRuleCounts(h, f.PreparedRules().Counts())
}

func writeBooleanRuleRows(h *humanResponse, f *filter.Filter) error {
	nearby := f != nil && f.NearbyEnabled
	rows := f.PreparedRules().Readback(nearby)
	if len(rows) == 0 {
		return h.row("Rules", "None; all sources, modes and bands allowed.")
	}
	for _, rule := range rows {
		if err := writeBooleanSavedRule(h, rule.RuleDefinition); err != nil {
			return err
		}
		if nearby {
			if rule.Suspended {
				if err := h.row("Effective", "Suspended: geography-only rule while NEARBY ON."); err != nil {
					return err
				}
			} else if err := h.quoted(humanPrefix("Effective"), rule.EffectiveExpr, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeBooleanNearby(h *humanResponse, f *filter.Filter) error {
	if f == nil || !f.NearbyEnabled {
		return h.row("NEARBY", "OFF; saved full expressions apply.")
	}
	fine := f.NearbyUserFine != pathreliability.InvalidCell
	coarse := f.NearbyUserCoarse != pathreliability.InvalidCell
	state := "unavailable"
	if fine && coarse {
		state = "usable"
	} else if fine || coarse {
		state = "partly usable"
	}
	if err := h.row("NEARBY", fmt.Sprintf("ON; geometry %s (fine cell %s, coarse cell %s).",
		state, booleanCellStatus(fine), booleanCellStatus(coarse))); err != nil {
		return err
	}
	if err := h.row("", "160m, 80m and 60m use the coarse cell; other bands use the fine cell."); err != nil {
		return err
	}
	return h.row("", "Only geography is omitted from effective rules; saved geography is retained. Unavailable geometry does not prevent rule edits.")
}

func booleanCellStatus(usable bool) string {
	if usable {
		return "available"
	}
	return "unavailable"
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
	return h.row("", "SELF ON can bypass ordinary personal rules and NEARBY; TOXIC still applies. Bulletin delivery uses its separate controls.")
}

func writeBooleanRuleCounts(h *humanResponse, counts filter.RuleSetCounts) error {
	if err := h.row("Counts", fmt.Sprintf("%d/%d occupied slots; full atoms %d/%d; full values %d/%d.",
		counts.Rules, filter.MaxRuleSlots, counts.Normal.Atoms, filter.MaxTotalRuleAtoms,
		counts.Normal.Values, filter.MaxTotalRuleValues)); err != nil {
		return err
	}
	for _, program := range []struct {
		name   string
		counts filter.RuleProgramCounts
	}{
		{"Full", counts.Normal}, {"Nearby", counts.Nearby}, {"Retained", counts.Retained},
	} {
		if err := h.row(program.name, fmt.Sprintf("nodes %d; atoms %d; values %d.",
			program.counts.Nodes, program.counts.Atoms, program.counts.Values)); err != nil {
			return err
		}
	}
	return h.row("", "Retained values count shared operands once, not projection references.")
}
