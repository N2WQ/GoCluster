// File role: US/Canadian mailing-address state rules and fixed-vocabulary preparation bounds.
// Unknown-state and NEARBY behavior follows the existing geography matcher.
package filter

import (
	"fmt"
	"sort"
	"strings"

	"dxcluster/spot"
	"dxcluster/strutil"
)

// maxStateRuleEntries bounds each allow/block map independently, including
// inactive false entries. The US/Canadian vocabulary contains seventy-three codes.
const maxStateRuleEntries = 73

// SetDXState updates mailing-address rules with the ordinary deny-first contract.
func (f *Filter) SetDXState(state string, enabled bool) {
	state = strutil.NormalizeUpper(state)
	if !spot.IsState(state) {
		return
	}
	applyAllowBlockToggle(&f.DXStates, &f.BlockDXStates, state, enabled, &f.AllDXStates, &f.BlockAllDXStates)
}

// SetDEState updates spotter mailing-address rules independently of DX rules.
func (f *Filter) SetDEState(state string, enabled bool) {
	state = strutil.NormalizeUpper(state)
	if !spot.IsState(state) {
		return
	}
	applyAllowBlockToggle(&f.DEStates, &f.BlockDEStates, state, enabled, &f.AllDEStates, &f.BlockAllDEStates)
}

// ResetDXStates restores unrestricted state matching, including unknown states.
func (f *Filter) ResetDXStates() {
	f.DXStates, f.BlockDXStates = make(map[string]bool), make(map[string]bool)
	f.AllDXStates, f.BlockAllDXStates = true, false
}

// ResetDEStates restores unrestricted spotter state matching.
func (f *Filter) ResetDEStates() {
	f.DEStates, f.BlockDEStates = make(map[string]bool), make(map[string]bool)
	f.AllDEStates, f.BlockAllDEStates = true, false
}

// ValidateStateRules bounds state maps before detached copying. Exact storage
// and machine proposals reject noncanonical keys instead of normalizing them;
// false entries count toward the same fixed bound and must also be valid.
func (c Configuration) ValidateStateRules() error {
	for _, domain := range [...]struct {
		name  string
		rules StringRules
	}{{"dx_states", c.Filters.DXStates}, {"de_states", c.Filters.DEStates}} {
		for _, entries := range [...]map[string]bool{domain.rules.Allow, domain.rules.Block} {
			if len(entries) > maxStateRuleEntries {
				return fmt.Errorf("filters.%s exceeds %d state entries", domain.name, maxStateRuleEntries)
			}
			for key := range entries {
				if !spot.IsState(key) {
					return fmt.Errorf("filters.%s contains an unsupported key", domain.name)
				}
			}
		}
	}
	return nil
}

func formatStateSummary(label string, allow, block map[string]bool, allowAll, blockAll bool) string {
	if blockAll {
		return label + ": allow=NONE block=ALL"
	}
	allowed, blocked := make([]string, 0, len(allow)), make([]string, 0, len(block))
	for key, enabled := range allow {
		if enabled && !block[key] {
			allowed = append(allowed, key)
		}
	}
	for key, enabled := range block {
		if enabled {
			blocked = append(blocked, key)
		}
	}
	sort.Strings(allowed)
	sort.Strings(blocked)
	allowLabel, blockLabel := "NONE", "NONE"
	if len(allow) == 0 && allowAll {
		allowLabel = "ALL"
	} else if len(allowed) > 0 {
		allowLabel = strings.Join(allowed, ", ")
	}
	if len(blocked) > 0 {
		blockLabel = strings.Join(blocked, ", ")
	}
	return fmt.Sprintf("%s: allow=%s block=%s", label, allowLabel, blockLabel)
}
