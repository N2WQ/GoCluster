// File role: Formats human readbacks as ASCII lines of at most 78 characters.
// Exact strings are quoted in bounded pieces; prose wrapping never touches values.
// A counting pass validates the complete CRLF response before detail maps are sorted.
package telnet

import (
	"fmt"
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
