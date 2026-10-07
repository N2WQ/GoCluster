// File role: Shows effective PASS/REJECT selections in human detail views.
// Borrowed rules remain immutable; escaped-byte admission bounds request-local
// key collection and sorting before either complete-response rendering pass.
package telnet

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

// Literal ALL/NONE and list punctuation must not masquerade as display syntax.
func effectiveValueSize(value string, budget int) (int, bool) {
	size, fits := shortHumanValueSize(value, budget)
	if fits && simpleHumanValue(value) && (value == "ALL" || value == "NONE" || strings.ContainsAny(value, ",;")) {
		size += 2
	}
	return size, fits && size <= budget
}

func writeHumanEffectiveList(h *humanResponse, label string, values []string, all bool) error {
	prefix := "  " + label + ": "
	if all {
		return h.line(prefix + "ALL")
	}
	if len(values) == 0 {
		return h.line(prefix + "NONE")
	}
	budget := maxYAMLBytes - h.size
	for _, value := range values {
		size, fits := effectiveValueSize(value, budget)
		if !fits || size+2 > budget {
			return errReadbackTooLarge
		}
		budget -= size + 2
	}
	line := prefix
	for i, value := range values {
		suffix := ""
		if i+1 < len(values) {
			suffix = ","
		}
		size, fits := effectiveValueSize(value, humanReadbackWidth-len(prefix)-len(suffix))
		if !fits {
			if len(line) > len(prefix) {
				if err := h.line(line); err != nil {
					return err
				}
				prefix = strings.Repeat(" ", len(prefix))
			}
			if err := h.quoted(prefix, value, suffix); err != nil {
				return err
			}
			prefix, line = strings.Repeat(" ", len(prefix)), strings.Repeat(" ", len(prefix))
			continue
		}
		text := value
		if size != len(value) {
			text = strconv.QuoteToASCII(value)
		}
		separator := ""
		if len(line) > len(prefix) {
			separator = " "
		}
		if len(line)+len(separator)+len(text)+len(suffix) > humanReadbackWidth {
			if err := h.line(line); err != nil {
				return err
			}
			prefix = strings.Repeat(" ", len(prefix))
			line, separator = prefix, ""
		}
		line += separator + text + suffix
	}
	if len(line) > len(prefix) {
		return h.line(line)
	}
	return nil
}

// Admission uses eligible content, never raw map cardinality. At least two
// bytes per selected key also cap scratch cardinality before copying/sorting.
func humanEffectiveKeys[K string | int](entries map[K]bool, eligible func(K, bool) bool, sizeOf func(K, int) (int, bool), budget int) ([]K, error) {
	count := 0
	for key, enabled := range entries {
		if !eligible(key, enabled) {
			continue
		}
		size, fits := sizeOf(key, budget)
		if !fits || size+2 > budget {
			return nil, errReadbackTooLarge
		}
		budget -= size + 2
		count++
	}
	keys := make([]K, 0, count)
	for key, enabled := range entries {
		if eligible(key, enabled) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys, nil
}

func writeHumanEffectiveStringRules(h *humanResponse, rules filter.StringRules, validAllow, validBlock func(string) bool) error {
	if rules.BlockAll {
		return writeHumanEffectiveAll(h)
	}
	allow, err := humanEffectiveKeys(rules.Allow, func(key string, enabled bool) bool {
		return enabled && key != "" && strings.TrimSpace(key) == key && !rules.Block[key] && (validAllow == nil || validAllow(key))
	}, effectiveValueSize, maxYAMLBytes-h.size)
	if err != nil {
		return err
	}
	if err = writeHumanEffectiveList(h, "PASS", allow, rules.AllowAll && len(rules.Allow) == 0); err != nil {
		return err
	}
	block, err := humanEffectiveKeys(rules.Block, func(key string, enabled bool) bool {
		return enabled && strings.TrimSpace(key) == key && (validBlock == nil || validBlock(key))
	}, effectiveValueSize, maxYAMLBytes-h.size)
	if err != nil {
		return err
	}
	return writeHumanEffectiveList(h, "REJECT", block, false)
}

func writeHumanEffectiveAll(h *humanResponse) error {
	if err := writeHumanEffectiveList(h, "PASS", nil, false); err != nil {
		return err
	}
	return writeHumanEffectiveList(h, "REJECT", nil, true)
}

// Integer identity remains ADIF or zone identity; DXCC labels expand only
// after bounded entity-key admission and keep the captured CTY associations.
func writeHumanEffectiveIntRules(h *humanResponse, rules filter.IntRules, dxcc bool) error {
	if rules.BlockAll {
		return writeHumanEffectiveAll(h)
	}
	labels := func(code int) []string {
		if dxcc {
			if prefixes := h.dxcc.Prefixes(code); len(prefixes) != 0 {
				return prefixes
			}
			return []string{"Unknown DXCC (" + strconv.Itoa(code) + ")"}
		}
		return []string{strconv.Itoa(code)}
	}
	sizeOf := func(code, budget int) (int, bool) {
		total := 0
		for _, label := range labels(code) {
			size, fits := effectiveValueSize(label, budget-total)
			if !fits || size+2 > budget-total {
				return 0, false
			}
			total += size + 2
		}
		// The key collector adds the last separator; avoid counting it twice.
		return total - 2, true
	}
	for i, entries := range []map[int]bool{rules.Allow, rules.Block} {
		keys, err := humanEffectiveKeys(entries, func(code int, enabled bool) bool {
			return enabled && (i == 1 || !rules.Block[code]) && (dxcc || i == 1 || filter.IsSupportedZone(code))
		}, sizeOf, maxYAMLBytes-h.size)
		if err != nil {
			return err
		}
		var values []string
		for _, code := range keys {
			values = append(values, labels(code)...)
		}
		label := "PASS"
		if i == 1 {
			label = "REJECT"
		}
		if err := writeHumanEffectiveList(h, label, values, i == 0 && rules.AllowAll && len(rules.Allow) == 0); err != nil {
			return err
		}
	}
	return nil
}

func writeHumanEffectiveEvents(h *humanResponse, rules filter.StringRules) error {
	canonical := filter.StringRules{AllowAll: rules.AllowAll, BlockAll: rules.BlockAll}
	if !rules.BlockAll {
		names := spot.EventNames(^spot.EventMask(0))
		var allow, block spot.EventMask
		for key := range rules.Allow {
			allow |= humanEventMask(key, names)
		}
		for key := range rules.Block {
			block |= humanEventMask(key, names)
		}
		canonical.Allow, canonical.Block = make(map[string]bool), make(map[string]bool)
		for _, name := range names {
			mask := spot.EventMaskForName(name)
			if !rules.AllowAll && allow&mask != 0 {
				canonical.Allow[name] = true
			}
			if block&mask != 0 {
				canonical.Block[name] = true
			}
		}
	}
	if err := writeHumanEffectiveStringRules(h, canonical, nil, nil); err != nil {
		return err
	}
	return h.line("  Untagged spots are always included.")
}

func writeHumanEffectivePath(h *humanResponse, rules filter.StringRules) error {
	if rules.BlockAll {
		return writeHumanEffectiveAll(h)
	}
	var pass, reject []string
	for _, class := range filter.SupportedPathClasses {
		if humanPathPass(class, rules) {
			pass = append(pass, class)
		} else if rules.Block[class] || class == filter.PathClassClosed && rules.Block[filter.PathClassUnlikely] {
			reject = append(reject, class)
		}
	}
	slices.Sort(pass)
	slices.Sort(reject)
	if err := writeHumanEffectiveList(h, "PASS", pass, rules.AllowAll && len(rules.Allow) == 0); err != nil {
		return err
	}
	return writeHumanEffectiveList(h, "REJECT", reject, false)
}

func writeHumanEffectivePatterns(h *humanResponse, allow, block []string) error {
	if slices.Contains(block, "*") {
		return writeHumanEffectiveAll(h)
	}
	if err := writeHumanEffectiveList(h, "PASS", allow, len(allow) == 0 || slices.Contains(allow, "*")); err != nil {
		return err
	}
	if err := writeHumanEffectiveList(h, "REJECT", block, false); err != nil {
		return err
	}
	if len(block) != 0 {
		return h.line("  REJECT takes precedence.")
	}
	return nil
}

func writeHumanEffectiveCategory(h *humanResponse, category readbackCategory, cfg filter.FilterConfiguration, status configurationReadbackStatus) error {
	name := humanCategoryNames[category.name]
	if category.kind == 't' {
		value := "ON"
		if category.toggle == filter.DefaultBoolFalse {
			value = "OFF"
		}
		if err := h.line(name + ": " + value); err != nil {
			return err
		}
		if category.name == "SELF" && value == "ON" {
			return h.line("  Self spots can bypass ordinary filters; TOXIC still applies.")
		}
		return nil
	}
	if category.kind == 'n' {
		return writeHumanNearbyState(h, cfg, status, "ON", "OFF")
	}
	if err := h.line(name); err != nil {
		return err
	}
	if cfg.NearbyEnabled && (strings.HasPrefix(category.name, "DX") || strings.HasPrefix(category.name, "DE")) && category.kind != 'p' {
		if err := writeHumanEffectiveList(h, "PASS", nil, true); err != nil {
			return err
		}
		if err := writeHumanEffectiveList(h, "REJECT", nil, false); err != nil {
			return err
		}
		return h.line("  Suspended by NEARBY; rules retained.")
	}
	if category.kind == 'p' {
		return writeHumanEffectivePatterns(h, category.allow, category.block)
	}
	if category.name == "EVENT" {
		return writeHumanEffectiveEvents(h, cfg.Events)
	}
	if category.name == "PATH" {
		return writeHumanEffectivePath(h, cfg.PathClasses)
	}
	if rules, ok := category.rules.(filter.IntRules); ok {
		return writeHumanEffectiveIntRules(h, rules, category.name == "DXDXCC" || category.name == "DEDXCC")
	}
	validAllow, validBlock := humanUpperRuleKey, humanUpperRuleKey
	switch category.name {
	case "BAND":
		validAllow, validBlock = humanBandRuleKey, humanBandBlockKey
	case "MODE":
		validAllow, validBlock = humanModeRuleKey, humanModeRuleKey
	case "SOURCE":
		validAllow = func(value string) bool { return value == "HUMAN" || value == "SKIMMER" }
		validBlock = validAllow
	case "CONFIDENCE":
		validAllow, validBlock = humanConfidenceRuleKey, humanConfidenceBlockKey
	case "DXSTATE", "DESTATE":
		validAllow, validBlock = spot.IsState, spot.IsState
	case "DXGRID2", "DEGRID2":
		validAllow, validBlock = humanGridRuleKey, humanGridBlockKey
	}
	rules, ok := category.rules.(filter.StringRules)
	if !ok {
		return fmt.Errorf("invalid human filter category %q", category.name)
	}
	if err := writeHumanEffectiveStringRules(h, rules, validAllow, validBlock); err != nil {
		return err
	}
	if category.name == "MODE" {
		if humanRulesPass(filter.UnknownModeToken, rules) {
			return h.line("  Unknown modes are included.")
		}
		return h.line("  Unknown modes are hidden.")
	}
	if category.name == "CONFIDENCE" && humanConfidenceRestricted(rules) {
		return h.line("  Exempt modes still pass.")
	}
	return nil
}

// Confidence's observable domain is fixed, including a missing glyph. False
// or unreachable saved block entries do not justify an exemption warning.
func humanConfidenceRestricted(rules filter.StringRules) bool {
	if !humanRulesPass("", rules) {
		return true
	}
	for _, symbol := range filter.SupportedConfidenceSymbols {
		if !humanRulesPass(symbol, rules) {
			return true
		}
	}
	return false
}
