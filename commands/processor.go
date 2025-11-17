package commands

import (
	"fmt"
	"strings"

	"dxcluster/buffer"
	"dxcluster/filter"
	"dxcluster/spot"
)

// Processor handles command processing
type Processor struct {
	spotBuffer *buffer.RingBuffer
}

// NewProcessor creates a new command processor
func NewProcessor(buf *buffer.RingBuffer) *Processor {
	return &Processor{
		spotBuffer: buf,
	}
}

// ProcessCommand processes a command from a client
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

// handleHelp returns help text
func (p *Processor) handleHelp() string {
	return fmt.Sprintf(`Available commands:
HELP                 - Show this help
SHOW/DX [count]      - Show last N DX spots (default: 10)
BYE                  - Disconnect

Filter commands (use from telnet session):
	SET/FILTER BAND <band>   - Enable a band filter (valid bands below)
	SET/FILTER MODE <mode>   - Enable a mode filter (valid modes listed below)
	UNSET/FILTER MODE <mode> - Disable a mode filter
	SHOW/FILTER BANDS        - List supported bands
	SHOW/FILTER MODES        - Show supported modes and enabled state

Supported modes: %s
Supported bands: %s

Examples:
	SHOW/DX            - Show last 10 spots
	SET/FILTER MODE FT8
`, strings.Join(filter.SupportedModes, ", "), strings.Join(spot.SupportedBandNames(), ", "))
}

// handleShow handles the SHOW command
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

// handleShowDX shows recent DX spots
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
