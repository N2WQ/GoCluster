package rbn

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
)

// Client represents an RBN telnet client
type Client struct {
	host      string
	port      int
	callsign  string
	name      string
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	connected bool
	shutdown  chan struct{}
	spotChan  chan *spot.Spot
}

// NewClient creates a new RBN client
func NewClient(host string, port int, callsign string, name string) *Client {
	return &Client{
		host:     host,
		port:     port,
		callsign: callsign,
		name:     name,
		shutdown: make(chan struct{}),
		spotChan: make(chan *spot.Spot, 100),
	}
}

// Connect establishes connection to RBN
func (c *Client) Connect() error {
	addr := net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port))
	if c.name != "" {
		log.Printf("Connecting to %s at %s...", c.name, addr)
	} else {
		log.Printf("Connecting to RBN at %s...", addr)
	}

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to RBN: %w", err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connected = true

	if c.name != "" {
		log.Printf("Connected to %s", c.name)
	} else {
		log.Println("Connected to RBN")
	}

	// Start login sequence
	go c.handleLogin()

	// Start reading spots
	go c.readLoop()

	return nil
}

// handleLogin performs the RBN login sequence
func (c *Client) handleLogin() {
	// Wait for login prompt and respond with callsign
	time.Sleep(2 * time.Second)

	if c.name != "" {
		log.Printf("Logging in to %s as %s", c.name, c.callsign)
	} else {
		log.Printf("Logging in to RBN as %s", c.callsign)
	}
	c.writer.WriteString(c.callsign + "\n")
	c.writer.Flush()
}

// readLoop reads lines from RBN
func (c *Client) readLoop() {
	defer func() {
		c.connected = false
		if c.conn != nil {
			c.conn.Close()
		}
	}()

	for {
		select {
		case <-c.shutdown:
			log.Println("RBN client shutting down")
			return
		default:
			// Set read timeout
			c.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

			line, err := c.reader.ReadString('\n')
			if err != nil {
				log.Printf("RBN read error: %v", err)
				return
			}

			line = strings.TrimSpace(line)

			// Skip empty lines
			if line == "" {
				continue
			}

			// Log and parse DX spots
			if strings.HasPrefix(line, "DX de") {
				c.parseSpot(line)
			}
		}
	}
}

// normalizeRBNCallsign removes SSID from RBN skimmer callsigns
// Example: "W3LPL-1-#" becomes "W3LPL-#"
func normalizeRBNCallsign(call string) string {
	// Check if it ends with -# (RBN skimmer indicator)
	if !strings.HasSuffix(call, "-#") {
		return call
	}

	// Remove the -# suffix temporarily
	withoutHash := strings.TrimSuffix(call, "-#")

	// Split by hyphen to find SSID
	parts := strings.Split(withoutHash, "-")

	// If there are multiple hyphens, remove the last one (the SSID)
	// W3LPL-1 becomes W3LPL
	if len(parts) > 1 {
		// Take all parts except the last (which is the SSID)
		basecall := strings.Join(parts[:len(parts)-1], "-")
		return basecall + "-#"
	}

	// If no SSID, return as-is with -# back
	return call
}

// parseTimeFromRBN parses the HHMMZ format from RBN and creates a proper timestamp
// RBN only provides HH:MM in UTC, so we need to combine it with today's date
// This ensures spots with the same RBN timestamp generate the same hash for deduplication
func parseTimeFromRBN(timeStr string) time.Time {
	// timeStr format is "HHMMZ" e.g. "0531Z"
	if len(timeStr) != 5 || !strings.HasSuffix(timeStr, "Z") {
		// Invalid format, return current time as fallback
		log.Printf("Warning: Invalid RBN time format: %s", timeStr)
		return time.Now().UTC()
	}

	// Extract hour and minute
	hourStr := timeStr[0:2]
	minStr := timeStr[2:4]

	hour, err1 := strconv.Atoi(hourStr)
	min, err2 := strconv.Atoi(minStr)

	if err1 != nil || err2 != nil {
		log.Printf("Warning: Failed to parse RBN time: %s", timeStr)
		return time.Now().UTC()
	}

	// Get current date in UTC
	now := time.Now().UTC()
	year, month, day := now.Date()

	// Construct timestamp with parsed HH:MM and today's date
	// Set seconds to 0 since RBN doesn't provide seconds
	spotTime := time.Date(year, month, day, hour, min, 0, 0, time.UTC)

	// Handle day boundary: if the spot time is more than 12 hours in the future,
	// it's probably from yesterday (we received it just after midnight UTC)
	if spotTime.Sub(now) > 12*time.Hour {
		spotTime = spotTime.AddDate(0, 0, -1)
	}

	// Handle day boundary: if the spot time is more than 12 hours in the past,
	// it might be from tomorrow (rare but possible near midnight)
	if now.Sub(spotTime) > 12*time.Hour {
		spotTime = spotTime.AddDate(0, 0, 1)
	}

	return spotTime
}

// parseSpot parses an RBN spot line into a Spot object
// Handles two formats:
//
//	CW/RTTY: DX de CALL: FREQ DXCALL MODE DB dB WPM WPM COMMENT TIME
//	FT8/FT4: DX de CALL: FREQ DXCALL MODE DB dB COMMENT TIME
func (c *Client) parseSpot(line string) {
	// Normalize whitespace - replace multiple spaces with single space
	normalized := regexp.MustCompile(`\s+`).ReplaceAllString(line, " ")

	// Split by spaces
	parts := strings.Fields(normalized)

	// Minimum: DX de CALL: FREQ DXCALL MODE DB dB TIME
	// Example CW: [DX de G4ZFE-#: 10111.0 LZ2PC CW 11 dB 22 WPM CQ 1928Z]
	// Example FT8: [DX de W3LPL-#: 14074.0 K1ABC FT8 -5 dB 2359Z]
	if len(parts) < 9 {
		log.Printf("RBN spot too short: %s", line)
		return
	}

	// Extract common fields
	deCall := strings.TrimSuffix(parts[2], ":") // Remove trailing colon
	deCall = normalizeRBNCallsign(deCall)       // Normalize RBN callsign

	freqStr := parts[3]
	dxCall := parts[4]
	mode := parts[5]
	dbStr := parts[6]
	// parts[7] is "dB"

	// Detect format by checking if parts[9] is "WPM"
	hasCWFormat := len(parts) >= 10 && parts[9] == "WPM"

	var wpmStr string
	var comment string
	var timeStr string
	var commentStartIdx int

	if hasCWFormat {
		// CW/RTTY format: has WPM field
		wpmStr = parts[8]
		// parts[9] is "WPM"
		commentStartIdx = 10
	} else {
		// FT8/FT4 format: no WPM field
		wpmStr = ""
		commentStartIdx = 8
	}

	// Find the time (4 digits followed by Z) and extract everything between start and time as comment
	for i := commentStartIdx; i < len(parts); i++ {
		if len(parts[i]) == 5 && strings.HasSuffix(parts[i], "Z") {
			// Found the time
			timeStr = parts[i]
			// Everything between comment start and time is comment
			if i > commentStartIdx {
				comment = strings.Join(parts[commentStartIdx:i], " ")
			}
			break
		}
	}

	// If we didn't find a time, use the last element if it looks like a time
	if timeStr == "" && len(parts) > 0 {
		lastPart := parts[len(parts)-1]
		if len(lastPart) == 5 && strings.HasSuffix(lastPart, "Z") {
			timeStr = lastPart
		}
	}

	// Parse frequency
	freq, err := strconv.ParseFloat(freqStr, 64)
	if err != nil {
		log.Printf("Failed to parse frequency '%s': %v", freqStr, err)
		return
	}

	// Parse signal report (dB)
	signalDB, err := strconv.Atoi(dbStr)
	if err != nil {
		log.Printf("Failed to parse signal dB '%s': %v", dbStr, err)
		signalDB = 0 // Default to 0 if parse fails
	}

	// Create spot
	s := spot.NewSpot(dxCall, deCall, freq, mode)

	// CRITICAL: Set the time from the RBN spot, not current time
	// This ensures identical spots generate identical hashes for deduplication
	if timeStr != "" {
		s.Time = parseTimeFromRBN(timeStr)
	}

	s.Report = signalDB // Set signal report in dB

	// Build comment based on format
	if hasCWFormat {
		// CW/RTTY: include WPM
		if comment != "" {
			s.Comment = fmt.Sprintf("%s WPM %s", wpmStr, comment)
		} else {
			s.Comment = fmt.Sprintf("%s WPM", wpmStr)
		}
	} else {
		// FT8/FT4: no WPM, just comment
		s.Comment = comment
	}

	// Determine source type for all modes: FT8/FT4 are digital, others are RBN (CW/RTTY)
	modeUpper := strings.ToUpper(mode)
	if modeUpper == "FT8" {
		s.SourceType = spot.SourceFT8
	} else if modeUpper == "FT4" {
		s.SourceType = spot.SourceFT4
	} else {
		s.SourceType = spot.SourceRBN
	}

	// Set source node for higher-level stats grouping. Distinguish RBN digital feed (port 7001)
	// from standard RBN (port 7000). If client was created for a different port, default to "RBN".
	if c.port == 7001 {
		s.SourceNode = "RBN-DIGITAL"
	} else {
		s.SourceNode = "RBN"
	}

	// Send to spot channel
	select {
	case c.spotChan <- s:
		// Spot sent successfully (logging handled by stats tracker)
	default:
		log.Println("RBN: Spot channel full, dropping spot")
	}
}

// GetSpotChannel returns the channel for receiving spots
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	return c.spotChan
}

// IsConnected returns whether the client is connected
func (c *Client) IsConnected() bool {
	return c.connected
}

// Stop closes the RBN connection
func (c *Client) Stop() {
	log.Println("Stopping RBN client...")
	close(c.shutdown)
	if c.conn != nil {
		c.conn.Close()
	}
}
