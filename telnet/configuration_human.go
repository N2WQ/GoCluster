// File role: Formats human readbacks as ASCII lines of at most 78 characters.
// Exact strings are quoted in bounded pieces; prose wrapping never touches values.
// A counting pass validates the complete CRLF response before detail maps are sorted.
package telnet

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"dxcluster/cty"
	"dxcluster/filter"
)

const (
	humanReadbackWidth = 78
	humanLabelWidth    = 14
	humanValueWidth    = humanReadbackWidth - humanLabelWidth
)

type humanResponse struct {
	boundedResponse
	countOnly bool
	size      int
	joined    bool
	// dxcc is request-owned and shared by the counting and generation passes.
	dxcc *cty.DXCCIndex
}

// line owns CRLF conversion and final size accounting in both passes. No partial
// output is queued: the caller discards the response if either pass fails.
func (h *humanResponse) line(value string) error {
	if h.err != nil {
		return h.err
	}
	if len(value) > humanReadbackWidth || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("invalid human readback line")
	}
	for i := range len(value) {
		if value[i] < 32 || value[i] > 126 {
			return fmt.Errorf("non-ASCII human readback line")
		}
	}
	if len(value)+2 > maxYAMLBytes-h.size {
		h.err = errReadbackTooLarge
		return h.err
	}
	h.size += len(value) + 2
	if h.countOnly {
		return nil
	}
	_, err := h.Write([]byte(value + "\r\n"))
	return err
}

// prose wraps only generated explanatory text. Stored strings use quoted.
func (h *humanResponse) prose(prefix, continuation, text string) error {
	if len(text) > maxYAMLBytes {
		return errReadbackTooLarge
	}
	for {
		width := humanReadbackWidth - len(prefix)
		if len(text) <= width {
			return h.line(prefix + text)
		}
		end := strings.LastIndexByte(text[:width+1], ' ')
		if end <= 0 {
			end = width
		}
		if err := h.line(prefix + text[:end]); err != nil {
			return err
		}
		text = strings.TrimLeft(text[end:], " ")
		prefix = continuation
	}
}

func humanPrefix(label string) string { return fmt.Sprintf("%-*s", humanLabelWidth, label) }

func (h *humanResponse) row(label, text string) error {
	return h.prose(humanPrefix(label), humanPrefix(""), text)
}

func (h *humanResponse) group(label string, parts []string) error {
	text := ""
	for _, part := range parts {
		if len(text)+3+len(part) > humanValueWidth && text != "" {
			if err := h.row(label, text); err != nil {
				return err
			}
			label, text = "", ""
		}
		if text != "" {
			text += " | "
		}
		text += part
	}
	return h.row(label, text)
}

func simpleHumanValue(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		if value[i] <= 32 || value[i] > 126 || value[i] == '"' || value[i] == '\\' {
			return false
		}
	}
	return true
}

func (h *humanResponse) valueRow(label, value, suffix string) error {
	if simpleHumanValue(value) && len(value)+len(suffix) <= humanValueWidth {
		return h.row(label, value+suffix)
	}
	return h.quoted(humanPrefix(label), value, suffix)
}

// quoted preserves original bytes. Every unit is an independently complete Go
// escape or printable ASCII byte. Invalid UTF-8 is escaped rather than replaced.
// Scratch and each emitted line are bounded; even a huge value is never quoted
// into one temporary string. '+' and indentation are outside the stored value.
func (h *humanResponse) quoted(prefix, value, suffix string) error {
	continuation := strings.Repeat(" ", len(prefix)) + "+ "
	var piece [humanReadbackWidth]byte
	used := 0
	for offset := 0; offset < len(value); {
		_, size := utf8.DecodeRuneInString(value[offset:])
		var scratch [16]byte
		escaped := strconv.AppendQuoteToASCII(scratch[:0], value[offset:offset+size])
		unit := escaped[1 : len(escaped)-1]
		if len(prefix)+2+used+len(unit)+len(suffix) > humanReadbackWidth {
			if used == 0 {
				return errReadbackTooLarge
			}
			if err := h.line(prefix + `"` + string(piece[:used]) + `"`); err != nil {
				return err
			}
			h.joined = true
			prefix, used = continuation, 0
		}
		copy(piece[used:], unit)
		used += len(unit)
		offset += size
	}
	return h.line(prefix + `"` + string(piece[:used]) + `"` + suffix)
}

// shortHumanValue is used only after checking the rendered budget. It allocates
// at most limit bytes and never scans or quotes a long value just for a preview.
func shortHumanValueSize(value string, limit int) (int, bool) {
	if len(value) > limit {
		return 0, false
	}
	if simpleHumanValue(value) {
		return len(value), true
	}
	size := 2
	for offset := 0; offset < len(value); {
		_, width := utf8.DecodeRuneInString(value[offset:])
		var scratch [16]byte
		quoted := strconv.AppendQuoteToASCII(scratch[:0], value[offset:offset+width])
		size += len(quoted) - 2
		if size > limit {
			return 0, false
		}
		offset += width
	}
	return size, size <= limit
}

func shortHumanValue(value string, limit int) (string, bool) {
	if _, fits := shortHumanValueSize(value, limit); !fits {
		return "", false
	}
	if simpleHumanValue(value) {
		return value, true
	}
	return strconv.QuoteToASCII(value), true
}

func writeHumanHeader(h *humanResponse, c *Client, status configurationReadbackStatus) error {
	if err := h.valueRow("User", c.callsign, ""); err != nil {
		return err
	}
	name, suffix := "(none)", ""
	if status.Preset.Associated {
		name = status.Preset.Name
		if status.Preset.Modified {
			suffix = " (modified)"
		}
	}
	if err := h.valueRow("Preset", name, suffix); err != nil {
		return err
	}
	return h.line("")
}

var humanCategoryNames = map[string]string{
	"BAND": "Bands", "MODE": "Modes", "SOURCE": "Sources", "EVENT": "Events",
	"CONFIDENCE": "Confidence", "PATH": "Path", "DXCONT": "DX continents",
	"DECONT": "DE continents", "DXZONE": "DX zones", "DEZONE": "DE zones",
	"DXGRID2": "DX grids", "DEGRID2": "DE grids", "DXDXCC": "DX DXCC",
	"DEDXCC": "DE DXCC", "DXCALL": "DX calls", "DECALL": "DE calls",
	"BEACON": "Beacons", "WWV": "WWV", "WCY": "WCY", "ANNOUNCE": "Announce",
	"SELF": "Self", "TOXIC": "Toxic", "NEARBY": "Nearby",
}

// writeHumanExactRules counts unsorted borrowed keys first. Only the successful
// generation pass owns a key slice, whose size was bounded by the first pass.
func writeHumanExactRules[K string | int](h *humanResponse, rules filter.RuleSet[K]) error {
	if err := h.line(fmt.Sprintf("  allow_all: %t", rules.AllowAll)); err != nil {
		return err
	}
	if err := h.line(fmt.Sprintf("  block_all: %t", rules.BlockAll)); err != nil {
		return err
	}
	for i, entries := range []map[K]bool{rules.Allow, rules.Block} {
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
		write := func(key K) error {
			suffix := ": " + strconv.FormatBool(entries[key])
			switch value := any(key).(type) {
			case string:
				return h.quoted("    ", value, suffix)
			case int:
				return h.line("    " + strconv.Itoa(value) + suffix)
			}
			return nil
		}
		if h.countOnly {
			for key := range entries {
				if err := write(key); err != nil {
					return err
				}
			}
		} else {
			keys := make([]K, 0, len(entries))
			for key := range entries {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				if err := write(key); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeHumanPatterns(h *humanResponse, label string, values []string) error {
	if len(values) == 0 {
		return h.line("  " + label + ": []")
	}
	if err := h.line("  " + label + ":"); err != nil {
		return err
	}
	for i, value := range values {
		if err := h.quoted(fmt.Sprintf("    [%d] ", i+1), value, ""); err != nil {
			return err
		}
	}
	return nil
}

func writeHumanExactCategory(h *humanResponse, category readbackCategory, cfg filter.FilterConfiguration, status configurationReadbackStatus) error {
	if err := h.line(humanCategoryNames[category.name] + " (exact rules)"); err != nil {
		return err
	}
	var err error
	switch rules := category.rules.(type) {
	case filter.StringRules:
		err = writeHumanExactRules(h, rules)
	case filter.IntRules:
		if category.name == "DXDXCC" || category.name == "DEDXCC" {
			err = writeHumanDXCCRules(h, rules)
		} else {
			err = writeHumanExactRules(h, rules)
		}
	default:
		switch category.kind {
		case 'p':
			if err = writeHumanPatterns(h, "allow", category.allow); err == nil {
				err = writeHumanPatterns(h, "block", category.block)
			}
		case 't':
			err = h.line("  selection: " + humanExactToggle(category.toggle))
		default:
			err = h.line(fmt.Sprintf("  nearby_enabled: %t", cfg.NearbyEnabled))
			if err == nil {
				err = writeHumanNearby(h, cfg, status)
			}
		}
	}
	if err != nil {
		return err
	}
	if category.name == "EVENT" {
		if err = h.line(""); err != nil {
			return err
		}
		summary := humanEventSummary(cfg.Events, humanReadbackWidth)
		parts := strings.SplitN(summary, "; block ", 2)
		summary = strings.ReplaceAll(parts[0], ", ", " or ")
		if len(parts) == 2 {
			summary += "; " + parts[1] + " blocked"
		}
		if err = h.prose("", "", "Tagged spots: "+summary+"."); err != nil {
			return err
		}
		if err = h.line("Untagged spots are always included."); err != nil {
			return err
		}
		if err = h.line("False EVENT entries apply because matching uses key presence."); err != nil {
			return err
		}
	}
	if category.name == "PATH" && humanPathLegacy(cfg.PathClasses) {
		if err = h.line("CLOSED follows UNLIKELY unless explicitly allowed or blocked."); err != nil {
			return err
		}
	}
	return nil
}

func humanExactToggle(value filter.DefaultBool) string {
	switch value {
	case filter.DefaultBoolFalse:
		return "false"
	case filter.DefaultBoolTrue:
		return "true"
	default:
		return "DEFAULT (effective true)"
	}
}

func humanToggle(value filter.DefaultBool) string {
	switch value {
	case filter.DefaultBoolFalse:
		return "Off"
	case filter.DefaultBoolTrue:
		return "On"
	default:
		return "On (default)"
	}
}
