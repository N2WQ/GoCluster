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
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	connected bool
	shutdown  chan struct{}
	spotChan  chan *spot.Spot
}

// NewClient creates a new RBN client
func NewClient(host string, port int, callsign string) *Client {
	return &Client{
		host:     host,
		port:     port,
		callsign: callsign,
		shutdown: make(chan struct{}),
		spotChan: make(chan *spot.Spot, 100),
	}
}

// Connect establishes connection to RBN
func (c *Client) Connect() error {
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	log.Printf("Connecting to RBN at %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to RBN: %w", err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connected = true

	log.Println("Connected to RBN")

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

	log.Printf("Logging in to RBN as %s", c.callsign)
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

// parseSpot parses an RBN spot line into a Spot object
func (c *Client) parseSpot(line string) {
	// Normalize whitespace - replace multiple spaces with single space
	normalized := regexp.MustCompile(`\s+`).ReplaceAllString(line, " ")

	// Split by spaces
	parts := strings.Fields(normalized)

	// Minimum: DX de CALL: FREQ DXCALL MODE DB dB WPM WPM COMMENT TIME
	// Example parts: [DX de G4ZFE-#: 10111.0 LZ2PC CW 11 dB 22 WPM CQ 1928Z]
	if len(parts) < 10 {
		log.Printf("RBN spot too short: %s", line)
		return
	}

	// Extract fields
	deCall := strings.TrimSuffix(parts[2], ":") // Remove trailing colon
	deCall = normalizeRBNCallsign(deCall)       // Normalize RBN callsign

	freqStr := parts[3]
	dxCall := parts[4]
	mode := parts[5]
	dbStr := parts[6]
	// parts[7] is "dB"
	wpmStr := parts[8]
	// parts[9] is "WPM"

	// Everything after WPM until the time is the comment
	// Find the time (4 digits followed by Z)
	var comment string
	for i := 10; i < len(parts); i++ {
		if len(parts[i]) == 5 && strings.HasSuffix(parts[i], "Z") {
			// Everything between WPM and time is comment
			if i > 10 {
				comment = strings.Join(parts[10:i], " ")
			}
			break
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
	s.Report = signalDB // Set signal report in dB
	s.Comment = fmt.Sprintf("%s WPM %s", wpmStr, comment)
	s.SourceType = spot.SourceRBN

	// Send to spot channel
	select {
	case c.spotChan <- s:
		log.Printf("Parsed RBN spot: %s spotted by %s on %.1f kHz (%s, %+d dB)",
			dxCall, deCall, freq, mode, signalDB)
	default:
		log.Println("RBN spot channel full, dropping spot")
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
