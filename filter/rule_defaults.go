package filter

import (
	"fmt"
	"strings"

	"dxcluster/filter/expr"
)

// PrepareDefaultRuleSet prepares, but does not activate, the visible editable
// PASS slot zero. It never publishes process defaults. Resetting personal rules
// instead uses a nil set; it must not implicitly call this factory.
func PrepareDefaultRuleSet(modes, sources []string) (*PreparedRuleSet, error) {
	sourceClause, err := defaultRuleSourceClause(sources)
	if err != nil {
		return nil, err
	}
	if len(modes) == 0 {
		modes = builtInDefaultModeSelection()
	}
	// The effective expression, not caller input length, bounds scratch storage.
	// Duplicates may be numerous but never enlarge this fixed-capacity selection.
	selected := make([]string, 0, expr.MaxValuesPerAtom)
	bytes := len("MODE=") + len(sourceClause)
	for _, mode := range modes {
		if len(mode) > expr.MaxBytes {
			return nil, fmt.Errorf("default mode name exceeds byte limit %d", expr.MaxBytes)
		}
		candidate := canonicalModeToken(mode)
		if len(candidate) > expr.MaxBytes {
			return nil, fmt.Errorf("canonical default mode exceeds byte limit %d", expr.MaxBytes)
		}
		if !IsSupportedMode(candidate) {
			return nil, fmt.Errorf("unsupported default mode %q", candidate)
		}
		if containsDefaultRuleMode(selected, candidate) {
			continue
		}
		if len(selected) == expr.MaxValuesPerAtom {
			return nil, fmt.Errorf("default mode selection exceeds value limit %d", expr.MaxValuesPerAtom)
		}
		size := len(candidate)
		if len(selected) > 0 {
			size++
		}
		if size > expr.MaxBytes-bytes {
			return nil, fmt.Errorf("default rule exceeds byte limit %d", expr.MaxBytes)
		}
		bytes += size
		selected = append(selected, candidate)
	}
	source := "MODE=" + strings.Join(selected, ",") + sourceClause
	return PrepareRuleSet([]RuleDefinition{{Slot: 0, Action: RulePass, Expr: source}})
}

func containsDefaultRuleMode(modes []string, candidate string) bool {
	for _, mode := range modes {
		if candidate == mode {
			return true
		}
	}
	return false
}

func defaultRuleSourceClause(sources []string) (string, error) {
	var human, skimmer, all bool
	for _, source := range sources {
		if len(source) > expr.MaxBytes {
			return "", fmt.Errorf("default source name exceeds byte limit %d", expr.MaxBytes)
		}
		candidate := strings.ToUpper(strings.TrimSpace(source))
		switch candidate {
		case "":
		case "ALL":
			all = true
		case "HUMAN":
			human = true
		case "SKIMMER":
			skimmer = true
		default:
			// Validate even after ALL: invalid input is never silently dropped.
			return "", fmt.Errorf("unsupported default source %q", candidate)
		}
	}
	if all || human == skimmer {
		return " AND SOURCE=HUMAN,SKIMMER", nil
	}
	if human {
		return " AND SOURCE=HUMAN", nil
	}
	return " AND SOURCE=SKIMMER", nil
}
