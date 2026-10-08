// File role: Applies per-mode minimum reports through the existing filter owner.
// Numeric PASS and REJECT have the same inclusive-minimum meaning. All names
// and resultant resource bounds are checked before any live map is replaced.
package telnet

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

func minSNRUsage(action filterAction) string {
	verb, clear := "PASS", "ALL"
	if action == actionBlock {
		verb, clear = "REJECT", "NONE"
	}
	return fmt.Sprintf("Usage: %s MINSNR <mode[,mode...]|ALL> <integer dB|%s>\nNumeric PASS and REJECT set the same inclusive minimum.\nHuman spots and spots without SNR are exempt.\n", verb, clear)
}

func newMinSNRHandler() *domainHandler {
	return &domainHandler{name: "MINSNR", apply: func(c *Client, action filterAction, args []string) (string, bool) {
		if (action != actionAllow && action != actionBlock) || len(args) < 2 {
			return minSNRUsage(action), false
		}
		value := strings.ToUpper(args[len(args)-1])
		clear := action == actionAllow && value == "ALL" || action == actionBlock && value == "NONE"
		threshold := 0
		if !clear {
			var err error
			threshold, err = strconv.Atoi(value)
			if err != nil {
				return minSNRUsage(action), false
			}
		}
		selector := strings.ToUpper(strings.TrimSpace(strings.Join(args[:len(args)-1], " ")))
		all := selector == "ALL"
		var names []string
		if all {
			names = filter.SupportedModes()
		} else {
			for _, part := range strings.Split(selector, ",") {
				if strings.TrimSpace(part) == "" {
					return minSNRUsage(action), false
				}
				names = append(names, strings.Fields(part)...)
			}
		}
		var applyErr error
		c.updateFilter(func(f *filter.Filter) {
			applyErr = applyMinSNRCommand(f, names, threshold, clear, all)
		})
		if applyErr != nil {
			return "Invalid MINSNR filter: " + applyErr.Error() + "\n" + minSNRUsage(action), false
		}
		if clear {
			return "Minimum SNR cleared for selected modes\n", true
		}
		return fmt.Sprintf("Minimum SNR set to %d dB for selected modes\nHuman spots and spots without SNR are exempt.\n", threshold), true
	}}
}

// The caller holds filterMu and its configuration transaction. Clear resolves
// a retained exact name first, so a changed alias cannot clear a different rule.
func applyMinSNRCommand(f *filter.Filter, names []string, threshold int, clear, all bool) error {
	if all && clear {
		f.MinSNR = nil
		return nil
	}
	if err := filter.ConfigurationFromFilter(f, filter.SettingsConfiguration{}).ValidateMinSNRRules(); err != nil {
		return err
	}
	selected := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "ALL" || !filter.ValidMinSNRModeKey(name) {
			return fmt.Errorf("invalid mode list; ALL must appear alone")
		}
		if _, retained := f.MinSNR[name]; !clear || !retained {
			name = spot.CanonicalModeForFilter(name)
			if !filter.IsActiveMinSNRMode(name) {
				return fmt.Errorf("unknown mode; use a supported MODE token")
			}
		}
		selected[name] = struct{}{}
	}
	if len(selected) == 0 {
		return fmt.Errorf("mode list is empty")
	}
	// Admission precedes copying: dormant entries count toward the same bound.
	count, keyBytes := len(f.MinSNR), 0
	for name := range f.MinSNR {
		keyBytes += len(name)
	}
	for name := range selected {
		_, exists := f.MinSNR[name]
		if !exists && !clear {
			count++
			keyBytes += len(name)
		}
	}
	if count > filter.MaxMinSNREntries || keyBytes > filter.MaxMinSNRKeyBytes {
		return fmt.Errorf("threshold map exceeds its entry or mode-name byte limit")
	}
	next := maps.Clone(f.MinSNR)
	if next == nil {
		next = make(map[string]int, len(selected))
	}
	for name := range selected {
		if clear {
			delete(next, name)
		} else {
			next[name] = threshold
		}
	}
	f.MinSNR = next
	return nil
}
