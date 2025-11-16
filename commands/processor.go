package commands

import (
	"fmt"
	"strconv"
	"strings"

	"dxcluster/buffer"
	"dxcluster/spot"
)

// Processor handles user commands
type Processor struct {
	spotBuffer *buffer.RingBuffer
}

// NewProcessor creates a new command processor
func NewProcessor(spotBuffer *buffer.RingBuffer) *Processor {
	return &Processor{
		spotBuffer: spotBuffer,
	}
}

// Process executes a command and returns the response
func (p *Processor) Process(cmd string) string {
	// Normalize: trim whitespace and convert to lowercase
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return ""
	}

	// Split into command and arguments
	parts := strings.Fields(cmd)
	command := strings.ToLower(parts[0])

	switch command {
	case "help", "?":
		return p.handleHelp()
	case "show/dx", "sh/dx", "dx":
		return p.handleShowDX(parts[1:])
	case "show/station", "sh/sta":
		return p.handleShowStation(parts[1:])
	case "bye", "quit", "exit":
		return "BYE" // Special signal to disconnect
	default:
		return fmt.Sprintf("Unknown command: %s (try HELP)\n", parts[0])
	}
}

// handleHelp returns help text
func (p *Processor) handleHelp() string {
	return `Available commands:

Spot Commands:
  HELP or ?              - Show this help
  SHOW/DX [n]            - Show last n spots (default 10)
  SHOW/STATION <call>    - Show spots for a specific station
  
Filter Commands:
  SET/FILTER BAND <band> - Filter by band (20M, 40M, etc.)
  SET/FILTER MODE <mode> - Filter by mode (CW, SSB, FT8, etc.)
  SET/FILTER CALL <call> - Filter by callsign pattern (W1*, LZ5VV, etc.)
  SHOW/FILTER            - Show current filters
  UNSET/FILTER ALL       - Clear all filters
  UNSET/FILTER BAND      - Clear band filters
  UNSET/FILTER MODE      - Clear mode filters
  UNSET/FILTER CALL      - Clear callsign filters

Other:
  BYE or QUIT            - Disconnect from cluster

Examples:
  SHOW/DX 20             - Show last 20 spots
  SHOW/STATION LZ5VV     - Show all spots for LZ5VV
  SET/FILTER BAND 20M    - Only show 20 meter spots
  SET/FILTER MODE CW     - Only show CW spots
  SET/FILTER CALL W1*    - Only show W1 callsigns
  UNSET/FILTER ALL       - Remove all filters
`
}

// handleShowDX shows recent spots
func (p *Processor) handleShowDX(args []string) string {
	// Default to 10 spots
	count := 10

	// Parse count argument if provided
	if len(args) > 0 {
		if n, err := strconv.Atoi(args[0]); err == nil && n > 0 {
			count = n
			if count > 100 {
				count = 100 // Max 100 spots
			}
		}
	}

	// Get recent spots
	spots := p.spotBuffer.GetRecent(count)

	if len(spots) == 0 {
		return "No spots available.\n"
	}

	// Build response
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Last %d spots:\n", len(spots)))
	for _, s := range spots {
		result.WriteString(s.FormatDXCluster())
		result.WriteString("\n")
	}

	return result.String()
}

// handleShowStation shows spots for a specific callsign
func (p *Processor) handleShowStation(args []string) string {
	if len(args) == 0 {
		return "Usage: SHOW/STATION <callsign>\n"
	}

	callsign := strings.ToUpper(args[0])

	// Get all spots and filter
	allSpots := p.spotBuffer.GetAll()
	var filtered []*spot.Spot

	for _, s := range allSpots {
		if s.DXCall == callsign {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) == 0 {
		return fmt.Sprintf("No spots found for %s\n", callsign)
	}

	// Build response
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Spots for %s:\n", callsign))
	for _, s := range filtered {
		result.WriteString(s.FormatDXCluster())
		result.WriteString("\n")
	}

	return result.String()
}
