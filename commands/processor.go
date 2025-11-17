// Package commands implements user command processing for the DX Cluster telnet interface.
//
// The command processor handles various user commands entered via telnet:
//   - HELP/?     : Display help text
//   - SHOW/DX    : Display recent spots from ring buffer
//   - SHOW/STATION : Filter spots by callsign
//   - BYE/QUIT   : Disconnect from cluster
//
// Commands follow standard DX Cluster syntax (COMMAND/OPTION arguments).
// All commands are case-insensitive and whitespace-tolerant.
//
// The processor integrates with the ring buffer to provide historical spot queries.
package commands

import (
	"fmt"
	"strconv"
	"strings"

	"dxcluster/buffer"
	"dxcluster/spot"
)

// Processor handles parsing and execution of user commands.
//
// The processor requires access to the spot buffer for historical queries
// (SHOW/DX, SHOW/STATION commands). It's created once at server startup
// and shared across all telnet client sessions.
//
// Thread Safety: The processor itself is stateless and thread-safe.
// Buffer access is protected by the ring buffer's internal mutex.
type Processor struct {
	spotBuffer *buffer.RingBuffer // Reference to shared ring buffer for spot queries
}

// NewProcessor creates a new command processor.
//
// Parameters:
//   - spotBuffer: Ring buffer containing recent spots for historical queries
//
// Returns:
//   - *Processor: Initialized command processor ready for use
//
// The processor is stateless and can be safely shared across multiple
// concurrent telnet client sessions.
//
// Example:
//   processor := commands.NewProcessor(spotBuffer)
func NewProcessor(spotBuffer *buffer.RingBuffer) *Processor {
	return &Processor{
		spotBuffer: spotBuffer,
	}
}

// Process executes a user command and returns the response text.
//
// Parameters:
//   - cmd: Raw command string from telnet client (e.g., "SHOW/DX 20")
//
// Returns:
//   - string: Response text to send back to client
//            Returns "BYE" as a special signal for disconnect commands
//
// Command parsing:
//  1. Trims whitespace
//  2. Splits into command and arguments
//  3. Normalizes command to lowercase
//  4. Routes to appropriate handler
//
// Supported commands:
//   - HELP, ?           : handleHelp()
//   - SHOW/DX, SH/DX, DX : handleShowDX()
//   - SHOW/STATION, SH/STA : handleShowStation()
//   - BYE, QUIT, EXIT    : Returns "BYE" (signals disconnect)
//
// Unknown commands return error message suggesting HELP.
//
// Example:
//   response := processor.Process("SHOW/DX 10")
//   // Returns: "Last 10 spots:\nDX de W1ABC: ..."
func (p *Processor) Process(cmd string) string {
	// Normalize: trim whitespace and check for empty command
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return ""
	}

	// Split into command and arguments (whitespace-separated)
	parts := strings.Fields(cmd)
	command := strings.ToLower(parts[0])

	// Route to appropriate command handler
	switch command {
	case "help", "?":
		return p.handleHelp()

	case "show/dx", "sh/dx", "dx":
		return p.handleShowDX(parts[1:])

	case "show/station", "sh/sta":
		return p.handleShowStation(parts[1:])

	case "bye", "quit", "exit":
		return "BYE" // Special signal to telnet server to disconnect client

	default:
		return fmt.Sprintf("Unknown command: %s (try HELP)\n", parts[0])
	}
}

// handleHelp returns formatted help text describing available commands.
//
// Returns:
//   - string: Multi-line help text with command syntax and examples
//
// The help text includes:
//   - Spot query commands (SHOW/DX, SHOW/STATION)
//   - Filter commands (SET/FILTER, UNSET/FILTER, SHOW/FILTER)
//   - Connection commands (BYE, QUIT)
//   - Usage examples
//
// Note: Filter commands are documented but not yet implemented in this processor.
// They are handled by the telnet server's filter integration.
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

// handleShowDX returns the N most recent spots from the ring buffer.
//
// Parameters:
//   - args: Command arguments (optional: number of spots to show)
//
// Returns:
//   - string: Formatted list of recent spots in DX Cluster format
//
// Behavior:
//   - Default: Shows last 10 spots
//   - With argument: Shows last N spots (max 100)
//   - Invalid argument: Falls back to 10 spots
//   - No spots available: Returns "No spots available."
//
// Output format:
//   Last N spots:
//   DX de W1ABC:     14074.5 LZ5VV FT8 2359Z
//   DX de K3XYZ:     7074.0  DL1ABC FT8 2358Z
//   ...
//
// Examples:
//   handleShowDX([])      → Shows last 10 spots
//   handleShowDX(["20"])  → Shows last 20 spots
//   handleShowDX(["200"]) → Shows last 100 spots (capped at 100)
func (p *Processor) handleShowDX(args []string) string {
	// Default to 10 spots
	count := 10

	// Parse count argument if provided
	if len(args) > 0 {
		if n, err := strconv.Atoi(args[0]); err == nil && n > 0 {
			count = n
			if count > 100 {
				count = 100 // Cap at 100 spots to avoid overwhelming client
			}
		}
	}

	// Get recent spots from ring buffer (thread-safe)
	spots := p.spotBuffer.GetRecent(count)

	if len(spots) == 0 {
		return "No spots available.\n"
	}

	// Build response with formatted spots
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Last %d spots:\n", len(spots)))
	for _, s := range spots {
		// Use DX Cluster format: "DX de W1ABC:     14074.5 LZ5VV FT8 2359Z"
		result.WriteString(s.FormatDXCluster())
		result.WriteString("\n")
	}

	return result.String()
}

// handleShowStation returns all spots for a specific callsign.
//
// Parameters:
//   - args: Command arguments (required: callsign to search for)
//
// Returns:
//   - string: Formatted list of spots for the specified callsign
//
// Behavior:
//   - Searches entire ring buffer for matching callsign
//   - Matches on DXCall (spotted station), not DECall (spotter)
//   - Case-insensitive callsign matching
//   - Returns all matching spots in chronological order
//
// Output format:
//   Spots for LZ5VV:
//   DX de W1ABC:     14074.5 LZ5VV FT8 2359Z
//   DX de K3XYZ:     7074.0  LZ5VV FT8 2358Z
//   ...
//
// Examples:
//   handleShowStation(["LZ5VV"])   → Shows all spots for LZ5VV
//   handleShowStation([])          → Returns usage message
//   handleShowStation(["NOTHERE"]) → Returns "No spots found for NOTHERE"
func (p *Processor) handleShowStation(args []string) string {
	if len(args) == 0 {
		return "Usage: SHOW/STATION <callsign>\n"
	}

	// Normalize callsign to uppercase for matching
	callsign := strings.ToUpper(args[0])

	// Get all spots from buffer and filter by callsign
	allSpots := p.spotBuffer.GetAll()
	var filtered []*spot.Spot

	for _, s := range allSpots {
		// Match on DXCall (spotted station)
		if s.DXCall == callsign {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) == 0 {
		return fmt.Sprintf("No spots found for %s\n", callsign)
	}

	// Build response with formatted spots
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Spots for %s:\n", callsign))
	for _, s := range filtered {
		result.WriteString(s.FormatDXCluster())
		result.WriteString("\n")
	}

	return result.String()
}
