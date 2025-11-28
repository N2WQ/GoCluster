// Package rbn maintains TCP connections to the Reverse Beacon Network (CW/RTTY
// and FT4/FT8 feeds), parsing telnet lines into canonical *spot.Spot entries
// with CTY enrichment and optional skew corrections.
package rbn

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"dxcluster/cty"
	"dxcluster/skew"
	"dxcluster/spot"
)

// precompiled regex avoids the per-line allocation/compile cost when normalizing RBN lines
var whitespaceRE = regexp.MustCompile(`\s+`)

const (
	minRBNDialFrequencyKHz = 100.0
	maxRBNDialFrequencyKHz = 3000000.0
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
	lookup    *cty.CTYDatabase
	skewStore *skew.Store
	reconnect chan struct{}
	stopOnce  sync.Once
	keepSSID  bool
}

// NewClient creates a new RBN client
func NewClient(host string, port int, callsign string, name string, lookup *cty.CTYDatabase, skewStore *skew.Store, keepSSID bool) *Client {
	return &Client{
		host:      host,
		port:      port,
		callsign:  callsign,
		name:      name,
		shutdown:  make(chan struct{}),
		spotChan:  make(chan *spot.Spot, 100),
		lookup:    lookup,
		skewStore: skewStore,
		reconnect: make(chan struct{}, 1),
		keepSSID:  keepSSID,
	}
}

// Connect establishes the initial RBN connection and starts the supervision loop.
// The first dial runs synchronously so failures are reported to the caller; any
// subsequent disconnects are handled via the background reconnect loop.
func (c *Client) Connect() error {
	if err := c.establishConnection(); err != nil {
		return err
	}
	go c.connectionSupervisor()
	return nil
}

// establishConnection dials the remote RBN feed and spins up the login and read
// goroutines. It is used for the initial connection and each subsequent reconnect.
func (c *Client) establishConnection() error {
	addr := net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port))
	log.Printf("%s: connecting to %s...", c.displayName(), addr)

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.displayName(), err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connected = true

	log.Printf("%s: connection established", c.displayName())

	// Start login sequence and stream reader for this connection.
	go c.handleLogin()
	go c.readLoop()
	return nil
}

// connectionSupervisor waits for disconnect notifications and orchestrates the
// exponential backoff / reconnect attempts while honoring shutdown signals.
func (c *Client) connectionSupervisor() {
	const (
		initialDelay = 5 * time.Second
		maxDelay     = 60 * time.Second
	)

	for {
		select {
		case <-c.shutdown:
			return
		case <-c.reconnect:
			if c.isShutdown() {
				return
			}
			delay := initialDelay

			for {
				if c.isShutdown() {
					return
				}
				log.Printf("%s: attempting reconnect...", c.displayName())
				if err := c.establishConnection(); err != nil {
					log.Printf("%s: reconnect failed: %v (retry in %s)", c.displayName(), err, delay)
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-c.shutdown:
						timer.Stop()
						return
					}
					delay *= 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
				break
			}
		}
	}
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
				if c.isShutdown() {
					return
				}
				log.Printf("%s: read error: %v", c.displayName(), err)
				c.requestReconnect(err)
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

// normalizeRBNCallsign removes the SSID portion from RBN skimmer callsigns. Example:
// "W3LPL-1-#" becomes "W3LPL-#".
func normalizeRBNCallsign(call string) string {
	// Check if it ends with -# (RBN skimmer indicator)
	if !strings.HasSuffix(call, "-#") {
		return spot.NormalizeCallsign(call)
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

// normalizeSpotter normalizes the spotter (DE) callsign using either the legacy
// SSID-collapsing behavior or the exact callsign (when keepSSID is true).
func (c *Client) normalizeSpotter(raw string) string {
	if c.keepSSID {
		return spot.NormalizeCallsign(raw)
	}
	return normalizeRBNCallsign(raw)
}

// splitSpotterToken separates the "DX de CALL:freq" token into its callsign and any
// frequency fragment that may have been glued to the colon without whitespace. When a
// frequency fragment is found, it is inserted back into the token slice immediately
// after the spotter entry so downstream parsing sees the expected field layout.
func splitSpotterToken(parts []string) (string, []string) {
	if len(parts) < 3 {
		return "", parts
	}

	token := parts[2]
	colonIdx := strings.Index(token, ":")
	if colonIdx == -1 {
		return token, parts
	}

	call := token[:colonIdx]
	remainder := token[colonIdx+1:]

	if remainder == "" {
		return call, parts
	}

	// Insert the remainder as a new token after the spotter entry.
	newParts := make([]string, 0, len(parts)+1)
	newParts = append(newParts, parts[:3]...)
	newParts = append(newParts, remainder)
	newParts = append(newParts, parts[3:]...)
	return call, newParts
}

// findFrequencyField scans the tokenized RBN line for the first numeric value that
// looks like a dial frequency. Some telnet feeds occasionally inject the spotter
// callsign twice (once before and once after the colon), which shifts the columns.
// Rather than assume a fixed index, we look for the first value in a realistic HF/VHF
// range and return both the index and parsed float value.
func findFrequencyField(parts []string) (int, float64, bool) {
	for i := 3; i < len(parts); i++ {
		candidate := strings.Trim(parts[i], ",:")
		freq, err := strconv.ParseFloat(candidate, 64)
		if err != nil {
			continue
		}
		if freq >= minRBNDialFrequencyKHz && freq <= maxRBNDialFrequencyKHz {
			return i, freq, true
		}
	}
	return -1, 0, false
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
	normalized := whitespaceRE.ReplaceAllString(line, " ")

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
	deCallRaw, parts := splitSpotterToken(parts)
	if deCallRaw == "" {
		log.Printf("RBN spot missing spotter callsign: %s", line)
		return
	}
	deCall := c.normalizeSpotter(deCallRaw) // Normalize RBN callsign

	freqIdx, freq, ok := findFrequencyField(parts)
	if !ok {
		log.Printf("RBN spot missing numeric frequency: %s", line)
		return
	}
	if freqIdx+4 >= len(parts) {
		log.Printf("RBN spot truncated after frequency: %s", line)
		return
	}

	dxCall := spot.NormalizeCallsign(parts[freqIdx+1])
	mode := parts[freqIdx+2]
	dbStr := parts[freqIdx+3]

	hasCWFormat := freqIdx+6 < len(parts) && strings.EqualFold(parts[freqIdx+6], "WPM")

	var (
		wpmStr          string
		comment         string
		timeStr         string
		commentStartIdx int
	)

	if hasCWFormat {
		// CW/RTTY format: has WPM field immediately after the "dB" token
		wpmStr = parts[freqIdx+5]
		commentStartIdx = freqIdx + 7
	} else {
		// FT8/FT4 format: no WPM field
		wpmStr = ""
		commentStartIdx = freqIdx + 5
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

	// Apply per-skimmer correction to the numeric dial frequency we found earlier
	freq = skew.ApplyCorrection(c.skewStore, deCallRaw, freq)

	// Parse signal report (dB)
	signalDB, err := strconv.Atoi(dbStr)
	if err != nil {
		log.Printf("Failed to parse signal dB '%s': %v", dbStr, err)
		signalDB = 0 // Default to 0 if parse fails
	}

	if !spot.IsValidCallsign(dxCall) {
		// log.Printf("RBN: invalid DX call %s", dxCall) // noisy: caller requested silence
		return
	}
	if !spot.IsValidCallsign(deCall) {
		// log.Printf("RBN: invalid DE call %s", deCall) // noisy: caller requested silence
		return
	}

	dxInfo, ok := c.fetchCallsignInfo(dxCall)
	if !ok {
		return
	}
	deInfo, ok := c.fetchCallsignInfo(deCall)
	if !ok {
		return
	}

	// Create spot
	s := spot.NewSpot(dxCall, deCall, freq, mode)
	s.IsHuman = false
	s.DXMetadata = metadataFromPrefix(dxInfo)
	s.DEMetadata = metadataFromPrefix(deInfo)

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

	s.RefreshBeaconFlag()

	// Determine source type for all modes: FT8/FT4 are digital, others are RBN (CW/RTTY)
	modeUpper := strings.ToUpper(mode)
	switch modeUpper {
	case "FT8":
		s.SourceType = spot.SourceFT8
	case "FT4":
		s.SourceType = spot.SourceFT4
	default:
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

func (c *Client) fetchCallsignInfo(call string) (*cty.PrefixInfo, bool) {
	if c.lookup == nil {
		return nil, true
	}
	info, ok := c.lookup.LookupCallsign(call)
	// if !ok {
	// 	log.Printf("RBN: unknown call %s", call)
	// }
	return info, ok
}

func metadataFromPrefix(info *cty.PrefixInfo) spot.CallMetadata {
	if info == nil {
		return spot.CallMetadata{}
	}
	return spot.CallMetadata{
		Continent: info.Continent,
		Country:   info.Country,
		CQZone:    info.CQZone,
		ITUZone:   info.ITUZone,
		ADIF:      info.ADIF,
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
	log.Printf("Stopping %s client...", c.displayName())
	c.stopOnce.Do(func() {
		close(c.shutdown)
	})
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) isShutdown() bool {
	select {
	case <-c.shutdown:
		return true
	default:
		return false
	}
}

func (c *Client) requestReconnect(reason error) {
	if c.isShutdown() {
		return
	}
	if reason != nil {
		log.Printf("%s: scheduling reconnect after error: %v", c.displayName(), reason)
	}
	select {
	case c.reconnect <- struct{}{}:
	default:
	}
}

func (c *Client) displayName() string {
	if c.name != "" {
		return c.name
	}
	if c.port == 7001 {
		return "RBN Digital"
	}
	return "RBN"
}
