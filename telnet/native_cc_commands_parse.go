// File role: Bounded native CC command recognition. It deliberately does not
// guess per-band terminal syntax or translate Boolean rules into CC state.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

const nativeCCCommandGuidance = "Use HELP SET/FILTER for native CC filters. Boolean rule edits require DIALECT GO."

type nativeCCEdit uint8

const (
	nativeCCMode nativeCCEdit = iota
	nativeCCSource
	nativeCCGeography
	nativeCCDXBM
	nativeCCDefaults
	nativeCCControl
)

type nativeCCCommand struct {
	edit     nativeCCEdit
	category string
	action   filter.CCAction
	values   []string
	enabled  bool
}

func parseNativeCCCommand(line string) (nativeCCCommand, bool, string) {
	words := strings.Fields(line)
	if len(words) == 0 {
		return nativeCCCommand{}, false, ""
	}
	head := strings.ToUpper(words[0])
	if head == "PASS" || head == "REJECT" {
		if len(words) >= 2 && booleanSeparateDomain(strings.ToUpper(words[1])) {
			return nativeCCCommand{}, false, ""
		}
		return nativeCCCommand{}, true, nativeCCCommandGuidance
	}
	if head == "RESET" {
		if len(words) >= 2 && strings.EqualFold(words[1], "FILTER") {
			return nativeCCCommand{}, true, "CC resets use SET/NOFILTER; it restores operator mode/source defaults and keeps the six switches."
		}
		return nativeCCCommand{}, false, ""
	}
	if head == "SET/FILTER" {
		return parseNativeCCFilter(words[1:])
	}
	if head == "UNSET/FILTER" {
		if len(words) > 1 && booleanSeparateDomain(strings.ToUpper(words[1])) {
			return nativeCCCommand{}, false, ""
		}
		return nativeCCCommand{}, true, "UNSET/FILTER changes the six switches. For geography or DXBM use SET/FILTER with /PASS, /REJECT or /OFF."
	}
	if strings.HasPrefix(head, "SET/") || strings.HasPrefix(head, "UNSET/") {
		return parseNativeCCShortcut(head, len(words))
	}
	return nativeCCCommand{}, false, ""
}

func parseNativeCCShortcut(head string, count int) (nativeCCCommand, bool, string) {
	unset := strings.HasPrefix(head, "UNSET/")
	name := strings.TrimPrefix(strings.TrimPrefix(head, "UNSET/"), "SET/")
	if name == "NOFILTER" && !unset {
		return nativeCCExactShortcut(nativeCCCommand{edit: nativeCCDefaults}, head, count)
	}
	if name == "SKIMMER" || name == "NOSKIMMER" && !unset {
		return nativeCCExactShortcut(nativeCCCommand{edit: nativeCCSource, category: "SKIMMER", enabled: !unset && name == "SKIMMER"}, head, count)
	}
	controls := map[string]string{"ANN": "ANNOUNCE", "ANNOUNCE": "ANNOUNCE", "BEACON": "BEACON", "WWV": "WWV", "WCY": "WCY", "SELF": "SELF", "TOXIC": "TOXIC"}
	if domain, exists := controls[name]; exists {
		return nativeCCExactShortcut(nativeCCCommand{edit: nativeCCControl, category: domain, enabled: !unset}, head, count)
	}
	if !unset && strings.HasPrefix(name, "NO") {
		if domain, exists := controls[strings.TrimPrefix(name, "NO")]; exists {
			return nativeCCExactShortcut(nativeCCCommand{edit: nativeCCControl, category: domain}, head, count)
		}
	}
	enabled, mode := !unset, name
	if !unset && strings.HasPrefix(name, "NO") {
		enabled, mode = false, strings.TrimPrefix(name, "NO")
	}
	if spot.IsCCShortcutMode(mode) {
		return nativeCCExactShortcut(nativeCCCommand{edit: nativeCCMode, category: mode, enabled: enabled}, head, count)
	}
	if spot.IsSupportedFilterMode(mode) {
		return nativeCCCommand{}, true, "That mode has no enabled CC shortcut in the operator taxonomy. Use native schema-seven YAML for its selection."
	}
	return nativeCCCommand{}, true, "Unsupported CC shortcut. OWN, BOB, DX/NODX, TALK, WX and announcement/weather geography are not implemented. " + nativeCCCommandGuidance
}

func nativeCCExactShortcut(command nativeCCCommand, head string, count int) (nativeCCCommand, bool, string) {
	if count != 1 {
		return nativeCCCommand{}, true, "Usage: " + head + " (no additional arguments)"
	}
	return command, true, ""
}

func parseNativeCCFilter(words []string) (nativeCCCommand, bool, string) {
	if len(words) == 0 {
		return nativeCCCommand{}, true, "Usage: SET/FILTER <DOC|DOS|DXCTY|DXSTATE|DXBM>/<PASS|REJECT> <list>, or <category>/OFF."
	}
	operand := strings.ToUpper(words[0])
	base, suffix, inline := strings.Cut(operand, "/")
	if booleanSeparateDomain(base) && (!inline || suffix == "ON" || suffix == "OFF") {
		return nativeCCCommand{}, false, ""
	}
	// The final slash belongs to the action: canonical entity labels themselves
	// may contain a slash, for example 3D2/R in implied-DOC syntax.
	last := strings.LastIndexByte(operand, '/')
	action, valuesStart := "", 1
	if last >= 0 {
		base, action = operand[:last], operand[last+1:]
	} else if len(words) > 1 && strings.HasPrefix(words[1], "/") {
		base, action, valuesStart = operand, strings.ToUpper(strings.TrimPrefix(words[1], "/")), 2
	}
	if action != "PASS" && action != "REJECT" && action != "OFF" {
		return nativeCCCommand{}, true, "Use SET/FILTER <category>/PASS <list>, /REJECT <list> or /OFF. Per-band geography terminal syntax is unverified; use schema-seven YAML."
	}
	command := nativeCCCommand{edit: nativeCCGeography, category: base, action: filter.CCAction(strings.ToLower(action))}
	if base == "DXBM" {
		command.edit = nativeCCDXBM
	} else if base != "DOC" && base != "DOS" && base != "DXCTY" && base != "DXSTATE" {
		for _, category := range []string{"DOC", "DOS", "DXCTY", "DXSTATE", "DXBM", "AOC", "AOS", "WOC", "WOS"} {
			if strings.HasPrefix(base, category) {
				return nativeCCCommand{}, true, "Only global DOC, DOS, DXCTY and DXSTATE commands are implemented. Per-band edits use schema-seven YAML; announcement/weather geography is unavailable."
			}
		}
		if action == "OFF" || valuesStart != len(words) {
			return nativeCCCommand{}, true, "Implied DOC syntax is SET/FILTER <country-list>/PASS or /REJECT, without another list."
		}
		command.category = "DOC"
		values, err := nativeCCList(base)
		if err != nil {
			return nativeCCCommand{}, true, err.Error()
		}
		command.values = values
		return command, true, ""
	}
	if command.action == filter.CCOff {
		if valuesStart != len(words) {
			return nativeCCCommand{}, true, "OFF takes no list or additional arguments."
		}
		return command, true, ""
	}
	values, err := nativeCCList(strings.Join(words[valuesStart:], " "))
	if err != nil {
		return nativeCCCommand{}, true, err.Error()
	}
	command.values = values
	return command, true, ""
}

func nativeCCList(text string) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("PASS and REJECT require a nonempty list; explicit empty selections are available through native YAML")
	}
	var result []string
	for _, part := range strings.Split(text, ",") {
		words := strings.Fields(part)
		if len(words) == 0 {
			return nil, fmt.Errorf("empty list item; no changes applied")
		}
		result = append(result, words...)
	}
	return result, nil
}
