// File role: Displays every saved minimum, including dormant exact mode keys.
// Validate retained bounds and escaped output sizes before sorting or building
// rows. Both human counting and rendering passes use the same borrowed rules.
package telnet

import (
	"fmt"
	"slices"

	"dxcluster/filter"
)

func writeHumanMinSNR(h *humanResponse, values map[string]int, overview bool) error {
	if err := (filter.Configuration{Filters: filter.FilterConfiguration{MinSNR: values}}).ValidateMinSNRRules(); err != nil {
		return err
	}
	if len(values) == 0 {
		if overview {
			return h.row("Min SNR", "None")
		}
		if err := h.line("Minimum SNR: NONE"); err != nil {
			return err
		}
		return h.line("  Human spots and spots without SNR are exempt.")
	}
	budget := maxYAMLBytes - h.size
	inactive := 0
	for mode, threshold := range values {
		suffix := minSNRSuffix(mode, threshold)
		size, fits := effectiveValueSize(mode, budget)
		if !fits || size+len(suffix)+humanLabelWidth+2 > budget {
			return errReadbackTooLarge
		}
		budget -= size + len(suffix) + humanLabelWidth + 2
		if !filter.IsActiveMinSNRMode(mode) {
			inactive++
		}
	}
	keys := make([]string, 0, len(values))
	for mode := range values {
		keys = append(keys, mode)
	}
	slices.Sort(keys)
	label := "Minimum SNR"
	if overview {
		label = "Min SNR"
	}
	if err := h.row(label, fmt.Sprintf("%d configured modes; %d inactive", len(values), inactive)); err != nil {
		return err
	}
	for _, mode := range keys {
		if err := h.valueRow("", mode, minSNRSuffix(mode, values[mode])); err != nil {
			return err
		}
	}
	return h.row("", "Human spots and spots without SNR are exempt")
}

func minSNRSuffix(mode string, threshold int) string {
	suffix := fmt.Sprintf(" >= %d dB", threshold)
	if !filter.IsActiveMinSNRMode(mode) {
		suffix += " (inactive)"
	}
	return suffix
}
