// Package commands implements the minimal command processor used by telnet
// sessions. It focuses on HELP/SHOW/DX and defers filter manipulation to the
// telnet package so both layers stay small and easy to reason about.
package commands

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"dxcluster/buffer"
	"dxcluster/filter"
	"dxcluster/spot"
)

// archiveReader is the minimal interface the archive layer exposes for read paths.
type archiveReader interface {
	Recent(limit int) ([]*spot.Spot, error)
}

// Processor handles telnet command parsing and replies that rely on shared state
// (recent spots in the ring buffer).
type Processor struct {
	spotBuffer *buffer.RingBuffer
	archive    archiveReader
	spotInput  chan<- *spot.Spot
}

// NewProcessor wraps the shared ring buffer so SHOW/DX commands can read from
// the central spot store. When an archive reader is provided, SHOW/DX prefers
// the database and falls back to the ring buffer on errors or when empty.
// spotInput is optional; when provided, DX commands enqueue human spots into
// the shared deduplication pipeline.
func NewProcessor(buf *buffer.RingBuffer, archive archiveReader, spotInput chan<- *spot.Spot) *Processor {
	return &Processor{
		spotBuffer: buf,
		archive:    archive,
		spotInput:  spotInput,
	}
}

// ProcessCommand parses a single telnet command and returns the response text
// to write back to the client. A response of "BYE" signals the caller to close
// the session.
func (p *Processor) ProcessCommand(cmd string) string {
	return p.ProcessCommandForClient(cmd, "", "")
}

// ProcessCommandForClient behaves like ProcessCommand but also accepts the
// client's callsign so DX commands can be posted with a proper spotter ID.
func (p *Processor) ProcessCommandForClient(cmd string, spotter string, spotterIP string) string {
	cmd = strings.TrimSpace(cmd)

	// Empty command
	if cmd == "" {
		return ""
	}

	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return ""
	}
	if strings.EqualFold(fields[0], "DX") {
		return p.handleDX(fields, spotter, spotterIP)
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
DX <freq> <call> <comment> - Post a spot (frequency in kHz)
SHOW/DX [count]      - Show last N DX spots (default: 10)
BYE                  - Disconnect

Filter commands (allow + block, deny wins):
	PASS BAND <band>[,<band>...] - Allow specific bands (comma/space). ALL clears blocklist and allows all.
	PASS MODE <mode>[,<mode>...] - Allow specific modes. ALL clears blocklist and allows all.
	PASS SOURCE <HUMAN|SKIMMER|ALL> - Filter by spot origin: HUMAN=IsHuman true, SKIMMER=IsHuman false. ALL disables SOURCE filtering.
	PASS DXCONT <cont>[,<cont>...] - Allow DX continents (AF, AN, AS, EU, NA, OC, SA). ALL clears blocklist.
	PASS DECONT <cont>[,<cont>...] - Allow DE continents. ALL clears blocklist.
	PASS DXZONE <zone>[,<zone>...] - Allow DX CQ zones (1-40). ALL clears blocklist.
	PASS DEZONE <zone>[,<zone>...] - Allow DE CQ zones. ALL clears blocklist.
	PASS DXDXCC <code>[,<code>...] - Allow DX ADIF/DXCC codes. ALL clears blocklist.
	PASS DEDXCC <code>[,<code>...] - Allow DE ADIF/DXCC codes. ALL clears blocklist.
	PASS DXGRID2 <grid>[,<grid>...] - Allow 2-char DX grids (truncates longer tokens); ALL clears blocklist.
	PASS DEGRID2 <grid>[,<grid>...] - Allow 2-char DE grids; ALL clears blocklist.
	PASS DXCALL <pattern> - Allow DX calls matching the pattern (supports prefix/suffix * wildcard).
	PASS DECALL <pattern> - Allow DE/spotter calls matching the pattern (supports prefix/suffix * wildcard).
	PASS CONFIDENCE <symbol>[,<symbol>...] - Allow confidence glyphs (?,S,C,P,V,B or ALL). FT8/FT4 ignore confidence filtering.
	PASS BEACON - Deliver DX beacons (/B)
	PASS WWV - Deliver WWV bulletins
	PASS WCY - Deliver WCY bulletins
	PASS ANNOUNCE - Deliver PC93 announcements
	REJECT BAND <band>[,<band>...]      - Block listed bands; ALL blocks all bands.
	REJECT MODE <mode>[,<mode>...]      - Block listed modes; ALL blocks all modes.
	REJECT SOURCE <HUMAN|SKIMMER>       - Block human or automated spots.
	REJECT DXCONT <cont>[,<cont>...]    - Block DX continents; ALL blocks all DX continents.
	REJECT DECONT <cont>[,<cont>...]    - Block DE continents; ALL blocks all DE continents.
	REJECT DXZONE <zone>[,<zone>...]    - Block DX CQ zones; ALL blocks all DX zones.
	REJECT DEZONE <zone>[,<zone>...]    - Block DE CQ zones; ALL blocks all DE zones.
	REJECT DXDXCC <code>[,<code>...]    - Block DX ADIF/DXCC codes; ALL blocks all DX DXCCs.
	REJECT DEDXCC <code>[,<code>...]    - Block DE ADIF/DXCC codes; ALL blocks all DE DXCCs.
	REJECT DXGRID2 <grid>[,<grid>...]    - Block 2-char DX grids; ALL blocks all DX grids.
	REJECT DEGRID2 <grid>[,<grid>...]    - Block 2-char DE grids; ALL blocks all DE grids.
	REJECT DXCALL - Remove all DX callsign patterns (allows any DX call, subject to other filters).
	REJECT DECALL - Remove all DE callsign patterns (allows any DE call, subject to other filters).
	REJECT CONFIDENCE <symbol>[,<symbol>...] - Block glyphs; ALL blocks all glyphs (non-exempt modes).
	REJECT BEACON - Suppress DX beacons
	REJECT WWV - Suppress WWV bulletins
	REJECT WCY - Suppress WCY bulletins
	REJECT ANNOUNCE - Suppress PC93 announcements
	SHOW FILTER BANDS             - List supported bands
	SHOW FILTER MODES             - Show supported modes and enabled state
	SHOW FILTER DXCONT            - Show supported DX continents and enabled state
	SHOW FILTER DECONT            - Show supported DE continents and enabled state
	SHOW FILTER DXZONE            - Show supported DX CQ zones and enabled state
	SHOW FILTER DEZONE            - Show supported DE CQ zones and enabled state
	SHOW FILTER DXDXCC            - Show DX ADIF/DXCC filter state
	SHOW FILTER DEDXCC            - Show DE ADIF/DXCC filter state
	SHOW FILTER DXGRID2           - Show DX 2-character grid filter state
	SHOW FILTER DEGRID2           - Show DE 2-character grid filter state
	SHOW FILTER CONFIDENCE        - Show supported confidence glyphs and enabled state
	SHOW FILTER BEACON            - Show whether beacon spots are enabled
	SHOW FILTER WWV               - Show whether WWV bulletins are enabled
	SHOW FILTER WCY               - Show whether WCY bulletins are enabled
	SHOW FILTER ANNOUNCE          - Show whether PC93 announcements are enabled

Supported modes: %s
Supported bands: %s

Examples:
	SHOW/DX            - Show last 10 spots
	PASS MODE FT8
	PASS CONFIDENCE P,V
`, strings.Join(filter.SupportedModes, ", "), strings.Join(spot.SupportedBandNames(), ", "))
}

func (p *Processor) handleDX(fields []string, spotter string, spotterIP string) string {
	spotter = strings.TrimSpace(spotter)
	if spotter == "" {
		return "DX command requires a logged-in callsign.\n"
	}
	if !spot.IsValidCallsign(spotter) {
		return "DX command requires a valid callsign.\n"
	}
	if len(fields) < 4 {
		return "Usage: DX <frequency> <callsign> <comment>\n"
	}
	freq, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || freq <= 0 {
		return "Invalid frequency. Use a kHz value like 7001.0.\n"
	}
	dx := strings.TrimSpace(fields[2])
	if !spot.IsValidCallsign(dx) {
		return "Invalid DX callsign.\n"
	}
	comment := strings.TrimSpace(strings.Join(fields[3:], " "))
	parsed := spot.ParseSpotComment(comment, freq)
	s := spot.NewSpot(dx, spotter, freq, parsed.Mode)
	s.Comment = parsed.Comment
	s.Report = parsed.Report
	s.HasReport = parsed.HasReport
	s.SourceNode = strings.ToUpper(spotter)
	s.SpotterIP = strings.TrimSpace(spotterIP)

	if p.spotInput == nil {
		return "Spot input is not configured on this cluster.\n"
	}
	select {
	case p.spotInput <- s:
		return "Spot queued.\n"
	default:
		log.Printf("DX command: dedup input full, dropping spot from %s", spotter)
		return "Spot queue full; try again.\n"
	}
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

// handleShowDX renders the most recent N spots (archive when enabled, otherwise ring buffer).
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

	// Prefer archive for history; fall back to ring buffer.
	var spots []*spot.Spot
	if p.archive != nil {
		if rows, err := p.archive.Recent(count); err != nil {
			log.Printf("SHOW DX: archive query failed, falling back to ring buffer: %v", err)
		} else {
			spots = rows
		}
	}
	if len(spots) == 0 && p.spotBuffer != nil {
		spots = p.spotBuffer.GetRecent(count)
	}

	if len(spots) == 0 {
		return "No spots available.\n"
	}

	// Display oldest first so the most recent spot is last in the list.
	reverseSpotsInPlace(spots)

	// Build response
	var result strings.Builder
	for _, spot := range spots {
		result.WriteString(spot.FormatDXCluster())
		result.WriteString("\r\n")
	}

	return result.String()
}

// reverseSpotsInPlace flips the order of the provided slice so callers can
// present chronological output even when sources return newest-first.
func reverseSpotsInPlace(spots []*spot.Spot) {
	for i, j := 0, len(spots)-1; i < j; i, j = i+1, j-1 {
		spots[i], spots[j] = spots[j], spots[i]
	}
}
