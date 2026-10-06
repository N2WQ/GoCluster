// File role: Displays canonical DXCC labels while keeping entity rules numeric.
// Shared-entity labels are grouped in exact views; overview counts remain counts
// of entities. All label expansion is checked before joining or sorting keys.
package telnet

import (
	"fmt"
	"slices"
	"strings"

	"dxcluster/cty"
	"dxcluster/filter"
)

// dxccLabel bounds the group before joining; missing CTY never hides a rule.
func dxccLabel(index *cty.DXCCIndex, adif, limit int) (string, bool) {
	prefixes := index.Prefixes(adif)
	if len(prefixes) == 0 {
		label := fmt.Sprintf("Unknown DXCC (%d)", adif)
		return label, len(label) <= limit
	}
	size := 0
	for i, prefix := range prefixes {
		if i > 0 {
			size += 2
		}
		if len(prefix) > limit-size {
			return "", false
		}
		size += len(prefix)
	}
	return strings.Join(prefixes, ", "), true
}

// dxccPreview quotes individual stored labels, not the generated separators.
// A shared entity therefore reads I, IG9, IT9 rather than one quoted token.
func dxccPreview(index *cty.DXCCIndex, adif, limit int) (string, bool) {
	prefixes := index.Prefixes(adif)
	if len(prefixes) == 0 {
		return dxccLabel(index, adif, limit)
	}
	size := 0
	for i, prefix := range prefixes {
		if i > 0 {
			size += 2
		}
		n, fits := shortHumanValueSize(prefix, limit-size)
		if !fits {
			return "", false
		}
		size += n
	}
	var label strings.Builder
	for i, prefix := range prefixes {
		if i > 0 {
			label.WriteString(", ")
		}
		value, _ := shortHumanValue(prefix, limit)
		label.WriteString(value)
	}
	return label.String(), true
}

// snapshotCanonicalDXCC preserves the older engine's flag normalization and
// layout while replacing numeric list labels. Counts still describe entities.
func snapshotCanonicalDXCC(allowAll, blockAll bool, allow, block map[int]bool, supported []int, index *cty.DXCCIndex) (allowBlockSnapshot, error) {
	lists := [2]string{}
	counts := [2]int{}
	for i, entries := range []map[int]bool{allow, block} {
		var list strings.Builder
		keys := orderedIntValues(entries)
		counts[i] = len(keys)
		for j, code := range keys {
			if j > 0 {
				list.WriteString(", ")
			}
			label, fits := dxccLabel(index, code, maxYAMLBytes-list.Len())
			if !fits {
				return allowBlockSnapshot{}, errReadbackTooLarge
			}
			list.WriteString(label)
		}
		lists[i] = list.String()
	}
	return buildAllowBlockSnapshot(allowAll || coversAllSupportedInts(allow, supported), blockAll, lists[0], lists[1], counts[0], counts[1]), nil
}

// humanDXCCList counts effective entities even when their expanded names do not
// fit. Once the preview overflows, no more label strings or keys are allocated.
func humanDXCCList(entries map[int]bool, eligible func(int, bool) bool, index *cty.DXCCIndex, limit int) (string, int, bool) {
	count, size, fits := 0, 0, true
	for code, enabled := range entries {
		if !eligible(code, enabled) {
			continue
		}
		count++
		if count > humanReadbackWidth {
			fits = false
		}
		if !fits {
			continue
		}
		label, ok := dxccPreview(index, code, limit)
		if !ok {
			fits = false
			continue
		}
		if count > 1 {
			size += 2
		}
		size += len(label)
		fits = size <= limit
	}
	if !fits {
		return "", count, false
	}
	keys := make([]int, 0, count)
	for code, enabled := range entries {
		if eligible(code, enabled) {
			keys = append(keys, code)
		}
	}
	slices.Sort(keys)
	var text strings.Builder
	for i, code := range keys {
		if i > 0 {
			text.WriteString(", ")
		}
		label, _ := dxccPreview(index, code, limit)
		text.WriteString(label)
	}
	return text.String(), count, true
}

// humanDXCCSummary follows ordinary integer matcher precedence, including
// nonempty false-only allow maps and block-all overriding every selection.
func humanDXCCSummary(rules filter.IntRules, index *cty.DXCCIndex, limit int) string {
	if rules.BlockAll {
		return "None"
	}
	allow, count, fits := humanDXCCList(rules.Allow, func(code int, enabled bool) bool {
		return enabled && !rules.Block[code]
	}, index, limit-5)
	text := "All"
	if len(rules.Allow) > 0 || !rules.AllowAll {
		if count == 0 {
			return "None"
		}
		text = "Only " + allow
		if !fits {
			text = "Only " + humanCount(count, "DXCC entries")
		}
	}
	block, blocked, blockFits := humanDXCCList(rules.Block, func(_ int, enabled bool) bool { return enabled }, index, limit)
	if blocked == 0 {
		return text
	}
	if text == "All" {
		if blockFits && len("All except ")+len(block) <= limit {
			return "All except " + block
		}
		return fmt.Sprintf("All except %d blocked %s", blocked, humanNoun(blocked, "DXCC entries"))
	}
	if fits && blockFits && len(text)+8+len(block) <= limit {
		return text + "; block " + block
	}
	return fmt.Sprintf("Only %s; %d blocked", humanCount(count, "DXCC entries"), blocked)
}

// writeHumanDXCCRules preserves each stored entity entry, including false values.
// Count unsorted borrowed keys first; only the budget-approved pass sorts them.
func writeHumanDXCCRules(h *humanResponse, rules filter.IntRules) error {
	if err := h.line(fmt.Sprintf("  allow_all: %t", rules.AllowAll)); err != nil {
		return err
	}
	if err := h.line(fmt.Sprintf("  block_all: %t", rules.BlockAll)); err != nil {
		return err
	}
	for i, entries := range []map[int]bool{rules.Allow, rules.Block} {
		label := "allow"
		if i == 1 {
			label = "block"
		}
		if len(entries) == 0 {
			if err := h.line("  " + label + ": {}"); err != nil {
				return err
			}
			continue
		}
		if err := h.line("  " + label + ":"); err != nil {
			return err
		}
		write := func(code int) error {
			value, fits := dxccLabel(h.dxcc, code, maxYAMLBytes)
			if !fits {
				return errReadbackTooLarge
			}
			return h.quoted("    ", value, fmt.Sprintf(": %t", entries[code]))
		}
		if h.countOnly {
			for code := range entries {
				if err := write(code); err != nil {
					return err
				}
			}
			continue
		}
		keys := make([]int, 0, len(entries))
		for code := range entries {
			keys = append(keys, code)
		}
		slices.Sort(keys)
		for _, code := range keys {
			if err := write(code); err != nil {
				return err
			}
		}
	}
	return nil
}
