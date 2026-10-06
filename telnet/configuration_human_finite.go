// File role: Shows complete finite-category selections in bounded overview rows.
// Stored tokens stay atomic or use lossless quoted pieces. An escaped-size
// preflight bounds borrowed-key collection before the complete response count.
package telnet

import (
	"slices"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

// humanSelectionRow owns only one printable line. Generated phrases may wrap
// between words; stored tokens are never passed through prose word wrapping.
type humanSelectionRow struct {
	h    *humanResponse
	line string
}

func newHumanSelectionRow(h *humanResponse, label string) humanSelectionRow {
	return humanSelectionRow{h: h, line: humanPrefix(label)}
}

func (r *humanSelectionRow) flush() error {
	if len(r.line) <= humanLabelWidth {
		return nil
	}
	if err := r.h.line(r.line); err != nil {
		return err
	}
	r.line = humanPrefix("")
	return nil
}

func (r *humanSelectionRow) word(text string) error {
	if len(text) > humanValueWidth {
		return errReadbackTooLarge
	}
	separator := ""
	if len(r.line) > humanLabelWidth {
		separator = " "
	}
	if len(r.line)+len(separator)+len(text) > humanReadbackWidth {
		if err := r.flush(); err != nil {
			return err
		}
		separator = ""
	}
	r.line += separator + text
	return nil
}

func (r *humanSelectionRow) phrase(text string) error {
	for word := range strings.FieldsSeq(text) {
		if err := r.word(word); err != nil {
			return err
		}
	}
	return nil
}

func (r *humanSelectionRow) note(text string) error {
	if len(r.line) > humanLabelWidth {
		if len(r.line) == humanReadbackWidth {
			if err := r.flush(); err != nil {
				return err
			}
		} else {
			r.line += ";"
		}
	}
	return r.phrase(text)
}

func (r *humanSelectionRow) values(values []string) error {
	for i, value := range values {
		suffix := ""
		if i+1 < len(values) {
			suffix = ","
		}
		if text, fits := shortHumanValue(value, humanValueWidth-len(suffix)); fits {
			if err := r.word(text + suffix); err != nil {
				return err
			}
			continue
		}
		if err := r.flush(); err != nil {
			return err
		}
		if err := r.h.quoted(r.line, value, suffix); err != nil {
			return err
		}
		r.line = humanPrefix("")
	}
	return nil
}

// part retains the compact geography grouping when another complete summary
// fits; its strings come from the bounded integer preview, not stored tokens.
func (r *humanSelectionRow) part(text string) error {
	if len(r.line) > humanLabelWidth {
		if len(r.line)+3+len(text) <= humanReadbackWidth {
			r.line += " | " + text
			return nil
		}
		if err := r.flush(); err != nil {
			return err
		}
	}
	return r.word(text)
}

// Aggregate escaped bytes are a lower bound on the final indented/wrapped
// response. Reject oversize before copying keys; the same bound caps key count
// and sorting scratch. Only eligible keys are borrowed, under the caller's
// existing configuration locks, and no raw value is quoted into a large string.
func humanFiniteRuleKeys(rules filter.StringRules, validAllow, validBlock func(string) bool, budget int) ([]string, []string, error) {
	restricted := len(rules.Allow) != 0 || !rules.AllowAll
	eligible := [2]func(string, bool) bool{
		func(key string, enabled bool) bool {
			return enabled && key != "" && strings.TrimSpace(key) == key && !rules.Block[key] &&
				(validAllow == nil || validAllow(key))
		},
		func(key string, enabled bool) bool {
			return enabled && strings.TrimSpace(key) == key && (validBlock == nil || validBlock(key))
		},
	}
	entries := [2]map[string]bool{rules.Allow, rules.Block}
	var counts [2]int
	for i, group := range entries {
		for key, enabled := range group {
			if !eligible[i](key, enabled) {
				continue
			}
			size, fits := shortHumanValueSize(key, budget)
			if !fits || size+2 > budget {
				return nil, nil, errReadbackTooLarge
			}
			budget -= size + 2
			counts[i]++
		}
		if i == 0 && restricted && counts[0] == 0 {
			return nil, nil, nil
		}
	}
	var keys [2][]string
	for i, group := range entries {
		keys[i] = make([]string, 0, counts[i])
		for key, enabled := range group {
			if eligible[i](key, enabled) {
				keys[i] = append(keys[i], key)
			}
		}
		slices.Sort(keys[i])
	}
	return keys[0], keys[1], nil
}

func writeHumanFiniteSelection(r *humanSelectionRow, heading string, rules filter.StringRules, validAllow, validBlock func(string) bool) (bool, error) {
	if rules.BlockAll {
		return false, r.word("None")
	}
	allow, block, err := humanFiniteRuleKeys(rules, validAllow, validBlock, maxYAMLBytes-r.h.size)
	if err != nil {
		return false, err
	}
	restricted := len(rules.Allow) != 0 || !rules.AllowAll
	if restricted && len(allow) == 0 {
		return false, r.word("None")
	}
	if !restricted {
		if err := r.word("All"); err != nil {
			return false, err
		}
		if len(block) == 0 {
			return true, nil
		}
		if err := r.word("except"); err != nil {
			return false, err
		}
		return false, r.values(block)
	}
	if heading != "" {
		if err := r.word(heading); err != nil {
			return false, err
		}
	}
	if err := r.values(allow); err != nil {
		return false, err
	}
	if len(block) != 0 {
		if err := r.note("block"); err != nil {
			return false, err
		}
		if err := r.values(block); err != nil {
			return false, err
		}
	}
	return false, nil
}

func writeHumanFiniteRules(h *humanResponse, label, heading string, rules filter.StringRules, validAllow, validBlock func(string) bool, note string, noteUnlessAll bool) error {
	r := newHumanSelectionRow(h, label)
	all, err := writeHumanFiniteSelection(&r, heading, rules, validAllow, validBlock)
	if err != nil {
		return err
	}
	if note != "" && (!noteUnlessAll || !all) {
		if err := r.note(note); err != nil {
			return err
		}
	}
	return r.flush()
}

// EVENT's maps use key presence, including false entries. The taxonomy mask
// bounds canonical-key scratch to 64 families. Explicit blocks still matter
// outside a restrictive allow list because one spot can carry multiple tags.
func writeHumanFiniteEvents(h *humanResponse, rules filter.StringRules) error {
	canonical := filter.StringRules{AllowAll: rules.AllowAll, BlockAll: rules.BlockAll}
	if !rules.BlockAll {
		names := spot.EventNames(^spot.EventMask(0))
		var allowMask, blockMask spot.EventMask
		for key := range rules.Allow {
			allowMask |= humanEventMask(key, names)
		}
		for key := range rules.Block {
			blockMask |= humanEventMask(key, names)
		}
		canonical.Allow, canonical.Block = make(map[string]bool), make(map[string]bool)
		for _, name := range names {
			mask := spot.EventMaskForName(name)
			if !rules.AllowAll && mask&allowMask != 0 {
				canonical.Allow[name] = true
			}
			if mask&blockMask != 0 {
				canonical.Block[name] = true
			}
		}
	}
	return writeHumanFiniteRules(h, "Events", "", canonical, nil, nil, "untagged included", false)
}

func writeHumanGeography(h *humanResponse, label string, continents filter.StringRules, zones filter.IntRules, dxcc filter.IntRules, grids filter.StringRules, nearby bool) error {
	if nearby {
		return h.row(label, "Suspended by NEARBY; rules retained")
	}
	r := newHumanSelectionRow(h, label)
	if err := r.word("Continents:"); err != nil {
		return err
	}
	if _, err := writeHumanFiniteSelection(&r, "Only", continents, humanUpperRuleKey, humanUpperRuleKey); err != nil {
		return err
	}
	for _, part := range []string{
		"Zones: " + humanRuleSummaryWhere(zones, "zones", humanValueWidth-7, filter.IsSupportedZone, nil),
		"DXCC: " + humanDXCCSummary(dxcc, h.dxcc, humanValueWidth-6),
	} {
		if err := r.part(part); err != nil {
			return err
		}
	}
	if err := r.flush(); err != nil {
		return err
	}
	return h.row("", "Grids: "+humanRuleSummaryWhere(grids, "grids", humanValueWidth-7, humanGridRuleKey, humanGridBlockKey))
}
