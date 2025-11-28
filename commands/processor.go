// Package commands implements the minimal command processor used by telnet
// sessions. It focuses on HELP/SHOW/DX and defers filter manipulation to the
// telnet package so both layers stay small and easy to reason about.
package commands

import (
	"fmt"
	"strings"

	"dxcluster/buffer"
	"dxcluster/filter"
	"dxcluster/spot"
)

// Processor handles telnet command parsing and replies that rely on shared state
// (recent spots in the ring buffer).
type Processor struct {
	spotBuffer *buffer.RingBuffer
}

// NewProcessor wraps the shared ring buffer so SHOW/DX commands can read from
// the central spot store.
func NewProcessor(buf *buffer.RingBuffer) *Processor {
	return &Processor{
		spotBuffer: buf,
	}
}

// ProcessCommand parses a single telnet command and returns the response text
// to write back to the client. A response of "BYE" signals the caller to close
// the session.
func (p *Processor) ProcessCommand(cmd string) string {
	cmd = strings.TrimSpace(cmd)

	// Empty command
	if cmd == "" {
		return ""
	}

	// Split into parts
	parts := strings.Fields(strings.ToUpper(cmd))
	command := parts[0]

	switch command {
	case "HELP", "H":
		return p.handleHelp()
	case "SH", "SHOW":
		if len(parts) < 2 {
			return "Usage: SHOW/DX [count]\n"
		}
		return p.handleShow(parts[1:])
	case "BYE", "QUIT", "EXIT":
		return "BYE"
	default:
		return fmt.Sprintf("Unknown command: %s\nType HELP for available commands.\n", command)
	}
}

// handleHelp returns help text for builtin commands and filter controls.
func (p *Processor) handleHelp() string {
	return fmt.Sprintf(`Available commands:
HELP                 - Show this help
SHOW/DX [count]      - Show last N DX spots (default: 10)
BYE                  - Disconnect

Filter commands (use from telnet session):
	SET/FILTER BAND <band>[,<band>...] - Enable specific bands (comma/space separated, or ALL)
	SET/FILTER MODE <mode>[,<mode>...] - Enable modes (comma or space separated, or ALL)
	SET/FILTER DXCONT <cont>[,<cont>...] - Enable DX continents (AF, AN, AS, EU, NA, OC, SA or ALL)
	SET/FILTER DECONT <cont>[,<cont>...] - Enable spotter continents (AF, AN, AS, EU, NA, OC, SA or ALL)
	SET/FILTER DXZONE <zone>[,<zone>...] - Enable DX CQ zones (1-40 or ALL)
	SET/FILTER DEZONE <zone>[,<zone>...] - Enable spotter CQ zones (1-40 or ALL)
	SET/FILTER DXDXCC <code>[,<code>...] - Enable DX ADIF/DXCC codes (comma/space separated, or ALL)
	SET/FILTER DEDXCC <code>[,<code>...] - Enable spotter ADIF/DXCC codes (comma/space separated, or ALL)
	SET/FILTER DXGRID2 <grid>[,<grid>...] - Enable 2-character DX grids (truncates longer tokens; use ALL to reset)
	SET/FILTER DEGRID2 <grid>[,<grid>...] - Enable 2-character DE grids (truncates longer tokens; use ALL to reset)
	SET/FILTER CONFIDENCE <symbol>[,<symbol>...] - Enable consensus glyphs (?,S,C,P,V,B or ALL). FT8/FT4 spots do not carry confidence glyphs and ignore this filter.
	SET/FILTER BEACON - Deliver DX beacons (calls ending in /B)
	UNSET/FILTER BAND <band>[,<band>...]      - Disable listed bands (use ALL to clear)
	UNSET/FILTER MODE <mode>[,<mode>...]      - Disable listed modes (use ALL to clear)
	UNSET/FILTER DXCONT <cont>[,<cont>...]    - Disable DX continents (use ALL to clear)
	UNSET/FILTER DECONT <cont>[,<cont>...]    - Disable spotter continents (use ALL to clear)
	UNSET/FILTER DXZONE <zone>[,<zone>...]    - Disable DX CQ zones (use ALL to clear)
	UNSET/FILTER DEZONE <zone>[,<zone>...]    - Disable spotter CQ zones (use ALL to clear)
	UNSET/FILTER DXDXCC <code>[,<code>...]    - Disable DX ADIF/DXCC codes (use ALL to clear)
	UNSET/FILTER DEDXCC <code>[,<code>...]    - Disable spotter ADIF/DXCC codes (use ALL to clear)
	UNSET/FILTER DXGRID2 <grid>[,<grid>...]    - Disable listed 2-character DX grids (use ALL to clear)
	UNSET/FILTER DEGRID2 <grid>[,<grid>...]    - Disable listed 2-character DE grids (use ALL to clear)
	UNSET/FILTER CONFIDENCE <symbol>[,<symbol>...] - Disable listed glyphs (use ALL to clear)
	UNSET/FILTER BEACON - Suppress DX beacons
	SHOW/FILTER BANDS             - List supported bands
	SHOW/FILTER MODES             - Show supported modes and enabled state
	SHOW/FILTER DXCONT            - Show supported DX continents and enabled state
	SHOW/FILTER DECONT            - Show supported DE continents and enabled state
	SHOW/FILTER DXZONE            - Show supported DX CQ zones and enabled state
	SHOW/FILTER DEZONE            - Show supported DE CQ zones and enabled state
	SHOW/FILTER DXDXCC            - Show DX ADIF/DXCC filter state
	SHOW/FILTER DEDXCC            - Show DE ADIF/DXCC filter state
	SHOW/FILTER DXGRID2           - Show DX 2-character grid filter state
	SHOW/FILTER DEGRID2           - Show DE 2-character grid filter state
	SHOW/FILTER CONFIDENCE        - Show supported confidence glyphs and enabled state
	SHOW/FILTER BEACON            - Show whether beacon spots are enabled

Supported modes: %s
Supported bands: %s

Examples:
	SHOW/DX            - Show last 10 spots
	SET/FILTER MODE FT8
	SET/FILTER CONFIDENCE P,V
`, strings.Join(filter.SupportedModes, ", "), strings.Join(spot.SupportedBandNames(), ", "))
}

// handleShow routes SHOW subcommands; only SHOW/DX is currently supported.
func (p *Processor) handleShow(args []string) string {
	if len(args) == 0 {
		return "Usage: SHOW/DX [count]\n"
	}

	subCmd := args[0]

	switch subCmd {
	case "DX":
		return p.handleShowDX(args[1:])
	default:
		return fmt.Sprintf("Unknown SHOW subcommand: %s\n", subCmd)
	}
}

// handleShowDX renders the most recent N spots from the shared ring buffer.
func (p *Processor) handleShowDX(args []string) string {
	count := 10 // Default count

	// Parse count if provided
	if len(args) > 0 {
		var err error
		_, err = fmt.Sscanf(args[0], "%d", &count)
		if err != nil || count < 1 || count > 100 {
			return "Invalid count. Use 1-100.\n"
		}
	}

	// Get recent spots
	spots := p.spotBuffer.GetRecent(count)

	if len(spots) == 0 {
		return "No spots available.\n"
	}

	// Build response
	var result strings.Builder
	for _, spot := range spots {
		result.WriteString(spot.FormatDXCluster())
		result.WriteString("\r\n")
	}

	return result.String()
}
