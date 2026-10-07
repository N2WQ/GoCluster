// File role: Explains matcher-specific behavior in human configuration views.
// Unbounded-category previews retain count-only preparation when names do not
// fit. Finite overview selections wrap through configuration_human_finite.go.
package telnet

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"dxcluster/filter"
	"dxcluster/spot"
)

// humanRuleList scans borrowed entries for counts and rendered length before
// allocating keys. Its cap follows terminal width; inactive ordinary entries
// are never copied.
func humanRuleList[K string | int](entries map[K]bool, eligible func(K, bool) bool, limit int) (string, int, bool) {
	count, rendered, fits := 0, 0, true
	for key, enabled := range entries {
		if !eligible(key, enabled) {
			continue
		}
		count++
		if count > humanReadbackWidth {
			fits = false
		}
		if !fits {
			continue
		}
		size, ok := humanPreviewKeySize(key, limit)
		if !ok {
			fits = false
			continue
		}
		if count > 1 {
			rendered += 2
		}
		rendered += size
		if rendered > limit {
			fits = false
		}
	}
	if !fits {
		return "", count, false
	}
	keys := make([]K, 0, count)
	for key, enabled := range entries {
		if eligible(key, enabled) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	var text strings.Builder
	for i, key := range keys {
		if i > 0 {
			text.WriteString(", ")
		}
		value, _ := humanPreviewKey(key, limit)
		text.WriteString(value)
	}
	return text.String(), count, true
}
func humanPreviewKey[K string | int](key K, limit int) (string, bool) {
	switch value := any(key).(type) {
	case string:
		return shortHumanValue(value, limit)
	case int:
		text := strconv.Itoa(value)
		return text, len(text) <= limit
	}
	return "", false
}
func humanPreviewKeySize[K string | int](key K, limit int) (int, bool) {
	switch value := any(key).(type) {
	case string:
		return shortHumanValueSize(value, limit)
	case int:
		var scratch [24]byte
		size := len(strconv.AppendInt(scratch[:0], int64(value), 10))
		return size, size <= limit
	}
	return 0, false
}
func humanRuleSummary[K string | int](rules filter.RuleSet[K], noun string, limit int) string {
	return humanRuleSummaryWhere(rules, noun, limit, nil, nil)
}
func humanRuleSummaryWhere[K string | int](rules filter.RuleSet[K], noun string, limit int, validAllow, validBlock func(K) bool) string {
	if rules.BlockAll {
		return "None"
	}
	allow, count, fits := humanRuleList(rules.Allow, func(key K, value bool) bool {
		if text, ok := any(key).(string); ok && (text == "" || strings.TrimSpace(text) != text) {
			return false
		}
		return value && !rules.Block[key] && (validAllow == nil || validAllow(key))
	}, limit-5)
	text := "All"
	if len(rules.Allow) > 0 || !rules.AllowAll {
		if count == 0 {
			return "None"
		}
		text = "Only " + allow
		if !fits {
			text = "Only " + humanCount(count, noun)
		}
	}
	block, blocked, blockFits := humanRuleList(rules.Block, func(key K, value bool) bool {
		if text, ok := any(key).(string); ok && strings.TrimSpace(text) != text {
			return false
		}
		return value && (validBlock == nil || validBlock(key))
	}, limit)
	if blocked == 0 {
		return text
	}
	if text == "All" {
		if blockFits && len("All except ")+len(block) <= limit {
			return "All except " + block
		}
		return fmt.Sprintf("All except %d blocked %s", blocked, humanNoun(blocked, noun))
	}
	if fits && blockFits && len(text)+8+len(block) <= limit {
		return text + "; block " + block
	}
	return fmt.Sprintf("Only %s; %d blocked", humanCount(count, noun), blocked)
}

// Ordinary maps use exact keys, while spot tokens are already normalized. Do
// not present unreachable restored keys as effective selections or denials.
// These predicates avoid constructing normalized copies of oversized keys.
func humanUpperRuleKey(key string) bool {
	if !utf8.ValidString(key) {
		return false
	}
	for _, value := range key {
		if unicode.ToUpper(value) != value {
			return false
		}
	}
	return true
}
func humanModeRuleKey(key string) bool {
	return humanUpperRuleKey(key) && spot.CanonicalModeForFilter(key) == key
}
func humanGridRuleKey(key string) bool {
	return len(key) == 2 && humanUpperRuleKey(key)
}
func humanGridBlockKey(key string) bool {
	// A missing spot grid produces an empty token; an empty block rejects it.
	return key == "" || humanGridRuleKey(key)
}
func humanBandRuleKey(key string) bool {
	if key == "" || !utf8.ValidString(key) || key[len(key)-1] >= '0' && key[len(key)-1] <= '9' {
		return false
	}
	for _, value := range key {
		if value == ' ' || unicode.ToLower(value) != value {
			return false
		}
	}
	//nolint:misspell // Match both unit spellings normalized by spot.NormalizeBand.
	for _, unit := range []string{"meters", "meter", "metres", "metre", "centimeters", "centimeter", "centimetres", "centimetre"} {
		if strings.Contains(key, unit) {
			return false
		}
	}
	return true
}
func humanConfidenceRuleKey(key string) bool {
	switch key {
	case "?", "S", "C", "P", "V", "B":
		return true
	default:
		return false
	}
}
func humanConfidenceBlockKey(key string) bool {
	return key == "" || humanConfidenceRuleKey(key)
}
func humanBandBlockKey(key string) bool {
	return key == "" || humanBandRuleKey(key)
}
func humanSourceSummary(rules filter.StringRules) string {
	human, skimmer := humanRulesPass("HUMAN", rules), humanRulesPass("SKIMMER", rules)
	switch {
	case human && skimmer:
		return "All (HUMAN, SKIMMER)"
	case human:
		if rules.AllowAll && len(rules.Allow) == 0 {
			return "All except SKIMMER"
		}
		return "Only HUMAN"
	case skimmer:
		if rules.AllowAll && len(rules.Allow) == 0 {
			return "All except HUMAN"
		}
		return "Only SKIMMER"
	default:
		return "None"
	}
}

// Event masks are the matcher's actual identity, including normalized aliases.
// Taxonomy names are bounded by the 64-bit mask, independently of user rule
// cardinality. Compare uppercase runes without allocating a giant normalized key.
func humanEventMask(key string, names []string) spot.EventMask {
	key = strings.TrimSpace(key)
	for _, name := range names {
		index, matches := 0, true
		for _, value := range key {
			if index >= len(name) || unicode.ToUpper(value) != rune(name[index]) {
				matches = false
				break
			}
			index++
		}
		if matches && index == len(name) {
			return spot.EventMaskForName(name)
		}
	}
	return 0
}
func humanRulesPass(token string, rules filter.StringRules) bool {
	if rules.BlockAll || rules.Block[token] {
		return false
	}
	if len(rules.Allow) != 0 {
		return rules.Allow[token]
	}
	return rules.AllowAll
}

// PATH's legacy CLOSED branch has different semantics from ordinary maps.
// Matcher-backed tests are the oracle; no matching behavior is changed.
func humanPathPass(token string, rules filter.StringRules) bool {
	if token != filter.PathClassClosed {
		return humanRulesPass(token, rules)
	}
	if rules.BlockAll || rules.Block[filter.PathClassClosed] {
		return false
	}
	if rules.Allow[filter.PathClassClosed] {
		return true
	}
	if rules.Block[filter.PathClassUnlikely] {
		return false
	}
	if len(rules.Allow) != 0 {
		return rules.Allow[filter.PathClassUnlikely]
	}
	return rules.AllowAll
}
func humanPathLegacy(rules filter.StringRules) bool {
	return !rules.BlockAll && !rules.Allow[filter.PathClassClosed] && !rules.Block[filter.PathClassClosed] &&
		(rules.Allow[filter.PathClassUnlikely] || rules.Block[filter.PathClassUnlikely])
}
func humanPathSummary(rules filter.StringRules) string {
	selected := make([]string, 0, len(filter.SupportedPathClasses))
	for _, token := range filter.SupportedPathClasses {
		if humanPathPass(token, rules) {
			selected = append(selected, token)
		}
	}
	if len(selected) == len(filter.SupportedPathClasses) {
		return "All"
	}
	if len(selected) == 0 {
		return "None"
	}
	slices.Sort(selected)
	if rules.AllowAll && len(rules.Allow) == 0 {
		excluded := make([]string, 0, len(filter.SupportedPathClasses)-len(selected))
		for _, token := range filter.SupportedPathClasses {
			if !humanPathPass(token, rules) {
				excluded = append(excluded, token)
			}
		}
		slices.Sort(excluded)
		return "All except " + strings.Join(excluded, ", ")
	}
	return "Only " + strings.Join(selected, ", ")
}
func humanPatternList(values []string, limit int) (string, bool) {
	if len(values) > humanReadbackWidth {
		return "", false
	}
	rendered := max(0, 2*(len(values)-1))
	for _, value := range values {
		size, fits := shortHumanValueSize(value, limit-rendered)
		if !fits {
			return "", false
		}
		rendered += size
	}
	var text strings.Builder
	text.Grow(rendered)
	for i, value := range values {
		if i > 0 {
			text.WriteString(", ")
		}
		item, _ := shortHumanValue(value, limit)
		text.WriteString(item)
	}
	return text.String(), true
}
func humanPatternSummary(allow, block []string) string {
	if slices.Contains(block, "*") {
		return "None"
	}
	all := len(allow) == 0 || slices.Contains(allow, "*")
	allowText, fits := humanPatternList(allow, humanValueWidth-5)
	text := "All"
	if !all {
		text = "Only " + allowText
		if !fits {
			text = "Only " + humanCount(len(allow), "patterns")
		}
	}
	if len(block) == 0 {
		return text
	}
	blockText, blockFits := humanPatternList(block, humanValueWidth)
	if all {
		if blockFits && len(blockText)+11 <= humanValueWidth {
			return "All except " + blockText
		}
		return fmt.Sprintf("All except %s blocked %s", strconv.Itoa(len(block)), humanNoun(len(block), "patterns"))
	}
	if fits && blockFits && len(text)+8+len(blockText) <= humanValueWidth {
		return text + "; block " + blockText
	}
	return fmt.Sprintf("Only %s; %d blocked", humanCount(len(allow), "patterns"), len(block))
}

func humanNoun(count int, noun string) string {
	if count != 1 {
		return noun
	}
	switch noun {
	case "classes":
		return "class"
	case "DXCC entries":
		return "DXCC entry"
	default:
		return strings.TrimSuffix(noun, "s")
	}
}

func humanCount(count int, noun string) string {
	return strconv.Itoa(count) + " " + humanNoun(count, noun)
}
func writeHumanNearby(h *humanResponse, cfg filter.FilterConfiguration, status configurationReadbackStatus) error {
	return writeHumanNearbyState(h, cfg, status, "On", "Off")
}

func writeHumanNearbyState(h *humanResponse, cfg filter.FilterConfiguration, status configurationReadbackStatus, on, off string) error {
	if !cfg.NearbyEnabled {
		return h.row("Nearby", off)
	}
	if status.Effective.NearbyActive {
		description := on + "; grid "
		grid := status.Effective.Grid
		if len(grid) <= humanValueWidth-len(description) && simpleHumanValue(grid) {
			return h.row("Nearby", description+grid)
		}
		// Usable cells do not guarantee a simple stored grid. Preserve every
		// retained byte through bounded quoting rather than prose wrapping.
		return h.quoted(humanPrefix("Nearby")+description, grid, "")
	}
	if err := h.row("Nearby", on+", unavailable; usable grid cells missing"); err != nil {
		return err
	}
	return h.row("", "DX spots on affected bands are rejected")
}
func writeHumanOverview(h *humanResponse, cfg filter.FilterConfiguration, status configurationReadbackStatus) error {
	if err := writeHumanFiniteRules(h, "Bands", "Only", cfg.Bands, humanBandRuleKey, humanBandBlockKey, "", false); err != nil {
		return err
	}
	unknown := "unknown modes hidden"
	if humanRulesPass(filter.UnknownModeToken, cfg.Modes) {
		unknown = "unknown modes included"
	}
	if err := writeHumanFiniteRules(h, "Modes", "", cfg.Modes, humanModeRuleKey, humanModeRuleKey, unknown, false); err != nil {
		return err
	}
	if err := h.row("Sources", humanSourceSummary(cfg.Sources)); err != nil {
		return err
	}
	if err := writeHumanFiniteEvents(h, cfg.Events); err != nil {
		return err
	}
	if err := writeHumanFiniteRules(h, "Confidence", "Only", cfg.Confidence, humanConfidenceRuleKey, humanConfidenceBlockKey, "exempt modes still pass", true); err != nil {
		return err
	}
	if err := h.row("Path", humanPathSummary(cfg.PathClasses)); err != nil {
		return err
	}
	if humanPathLegacy(cfg.PathClasses) {
		if err := h.row("", "CLOSED follows UNLIKELY unless explicitly selected"); err != nil {
			return err
		}
	}
	for _, state := range []struct {
		label string
		rules filter.StringRules
	}{
		{"DX states", cfg.DXStates}, {"DE states", cfg.DEStates},
	} {
		if cfg.NearbyEnabled {
			if err := h.row(state.label, "Suspended by NEARBY; rules retained"); err != nil {
				return err
			}
		} else if err := writeHumanFiniteRules(h, state.label, "Only", state.rules, spot.IsState, spot.IsState, "", false); err != nil {
			return err
		}
	}
	if err := writeHumanGeography(h, "DX geography", cfg.DXContinents, cfg.DXZones, cfg.DXDXCC, cfg.DXGrid2, cfg.NearbyEnabled); err != nil {
		return err
	}
	if err := writeHumanGeography(h, "DE geography", cfg.DEContinents, cfg.DEZones, cfg.DEDXCC, cfg.DEGrid2, cfg.NearbyEnabled); err != nil {
		return err
	}
	if err := h.row("DX calls", humanPatternSummary(cfg.DXCallsigns, cfg.BlockDXCallsigns)); err != nil {
		return err
	}
	if err := h.row("DE calls", humanPatternSummary(cfg.DECallsigns, cfg.BlockDECallsigns)); err != nil {
		return err
	}
	if err := writeHumanNearby(h, cfg, status); err != nil {
		return err
	}
	if err := h.group("Include", []string{
		"Beacons: " + humanToggle(cfg.IncludeBeacons), "WWV: " + humanToggle(cfg.AllowWWV),
		"WCY: " + humanToggle(cfg.AllowWCY), "Announce: " + humanToggle(cfg.AllowAnnounce),
	}); err != nil {
		return err
	}
	if err := h.group("", []string{"Self: " + humanToggle(cfg.AllowSelf), "Toxic: " + humanToggle(cfg.AllowToxic)}); err != nil {
		return err
	}
	if cfg.AllowSelf != filter.DefaultBoolFalse {
		if err := h.row("", "Self spots can bypass ordinary filters; TOXIC still applies"); err != nil {
			return err
		}
	}
	if err := h.line(""); err != nil {
		return err
	}
	if err := h.line("Detailed selections: SHOW FILTER FULL"); err != nil {
		return err
	}
	return h.line("One category: SHOW FILTER <category>")
}
