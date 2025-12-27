// Package rbn maintains TCP connections to the Reverse Beacon Network (CW/RTTY
// and FT4/FT8 feeds), parsing telnet lines into canonical *spot.Spot entries
// with CTY enrichment and optional skew corrections.
package rbn

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"dxcluster/cty"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/uls"
	ztelnet "github.com/ziutek/telnet"
)

const (
	minRBNDialFrequencyKHz = 100.0
	maxRBNDialFrequencyKHz = 3000000.0
)

var (
	rbnCallCacheSize  = 4096
	rbnCallCacheTTL   = 10 * time.Minute
	rbnNormalizeCache = spot.NewCallCache(rbnCallCacheSize, rbnCallCacheTTL)
)

// UnlicensedReporter receives drop notifications for US calls failing FCC license checks.
type UnlicensedReporter func(source, role, call, mode string, freqKHz float64)

type unlicensedEvent struct {
	source string
	role   string
	call   string
	mode   string
	freq   float64
}

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
	lookup    func() *cty.CTYDatabase
	skewStore *skew.Store
	reconnect chan struct{}
	stopOnce  sync.Once
	writeMu   sync.Mutex
	keepSSID  bool

	bufferSize int

	unlicensedReporter UnlicensedReporter
	unlicensedQueue    chan unlicensedEvent

	minimalParse bool

	telnetTransport   string
	keepaliveInterval time.Duration
	keepaliveDone     chan struct{}

	rawChan chan<- string // optional passthrough for non-DX lines (minimal parser only)
}

type spotToken struct {
	raw       string
	clean     string
	upper     string
	start     int
	end       int
	trimStart int
	trimEnd   int
}

func tokenizeSpotLine(line string) []spotToken {
	tokens := make([]spotToken, 0, 16)
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		end := i
		raw := line[start:end]
		trimStart := start
		trimEnd := end
		for trimStart < end {
			if strings.ContainsRune(",;:!.", rune(line[trimStart])) {
				trimStart++
			} else {
				break
			}
		}
		for trimEnd > trimStart {
			if strings.ContainsRune(",;:!.", rune(line[trimEnd-1])) {
				trimEnd--
			} else {
				break
			}
		}
		clean := line[trimStart:trimEnd]
		tokens = append(tokens, spotToken{
			raw:       raw,
			clean:     clean,
			upper:     strings.ToUpper(clean),
			start:     start,
			end:       end,
			trimStart: trimStart,
			trimEnd:   trimEnd,
		})
	}
	return tokens
}

// ConfigureCallCache allows callers to tune the normalization cache used for RBN spotters.
func ConfigureCallCache(size int, ttl time.Duration) {
	if size <= 0 {
		size = 4096
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	rbnCallCacheSize = size
	rbnCallCacheTTL = ttl
	rbnNormalizeCache = spot.NewCallCache(rbnCallCacheSize, rbnCallCacheTTL)
}

// NewClient creates a new RBN client. bufferSize controls how many parsed spots
// can queue between the telnet reader and the downstream pipeline; it should be
// sized to absorb RBN burstiness (especially FT8/FT4 decode cycles).
func NewClient(host string, port int, callsign string, name string, lookup func() *cty.CTYDatabase, skewStore *skew.Store, keepSSID bool, bufferSize int) *Client {
	if bufferSize <= 0 {
		bufferSize = 100 // legacy default; callers should override via config
	}
	return &Client{
		host:       host,
		port:       port,
		callsign:   callsign,
		name:       name,
		shutdown:   make(chan struct{}),
		spotChan:   make(chan *spot.Spot, bufferSize),
		lookup:     lookup,
		skewStore:  skewStore,
		reconnect:  make(chan struct{}, 1),
		keepSSID:   keepSSID,
		bufferSize: bufferSize,
	}
}

// UseMinimalParser switches this client into a permissive parser intended for
// human/upstream telnet feeds (not strict RBN formats).
//
// The minimal parser requires DE, DX, and a numeric frequency (kHz). It then
// optionally extracts a mode token, an SNR/report token in the form "<num> dB"
// (or "<num>dB"), and a trailing HHMMZ timestamp. Any remaining tokens are
// treated as a free-form comment after removing structural/mode/report/time
// tokens so the spot can still render cleanly in DX-cluster output.
func (c *Client) UseMinimalParser() {
	if c != nil {
		c.minimalParse = true
	}
}

// SetTelnetTransport selects the telnet parser/negotiation backend.
// Supported values are "native" and "ziutek"; other values fall back to native.
func (c *Client) SetTelnetTransport(transport string) {
	if c == nil {
		return
	}
	c.telnetTransport = strings.ToLower(strings.TrimSpace(transport))
}

// SetRawPassthrough installs a channel for relaying non-DX lines when in minimalParse mode.
// Lines are delivered best-effort using a non-blocking send to avoid wedging ingest.
func (c *Client) SetRawPassthrough(ch chan<- string) {
	if c != nil {
		c.rawChan = ch
	}
}

// EnableKeepalive configures a periodic CRLF keepalive to prevent idle timeouts on upstream telnet feeds.
func (c *Client) EnableKeepalive(interval time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		return
	}
	c.keepaliveInterval = interval
}

func (c *Client) useZiutekTelnet() bool {
	return strings.EqualFold(c.telnetTransport, "ziutek")
}

func parseFrequencyCandidate(tok string) (float64, bool) {
	if tok == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return 0, false
	}
	if f < minRBNDialFrequencyKHz || f > maxRBNDialFrequencyKHz {
		return 0, false
	}
	return f, true
}

func extractCallAndFreq(tok spotToken) (string, float64, bool) {
	if tok.clean == "" {
		return "", 0, false
	}
	raw := tok.raw
	colonIdx := strings.IndexByte(raw, ':')
	if colonIdx == -1 {
		return tok.clean, 0, false
	}
	callPart := strings.TrimSpace(raw[:colonIdx])
	remainder := strings.TrimSpace(strings.Trim(raw[colonIdx+1:], ",;:"))
	freq, ok := parseFrequencyCandidate(remainder)
	return callPart, freq, ok
}

// SetUnlicensedReporter installs a best-effort reporter for unlicensed US drops.
// Reporting is fire-and-forget; when the queue is full we fallback to an async call.
func (c *Client) SetUnlicensedReporter(rep UnlicensedReporter) {
	c.unlicensedReporter = rep
	if rep != nil && c.unlicensedQueue == nil {
		c.unlicensedQueue = make(chan unlicensedEvent, 256)
		go c.unlicensedLoop()
	}
}

func (c *Client) unlicensedLoop() {
	for {
		select {
		case evt := <-c.unlicensedQueue:
			if evt.call == "" {
				continue
			}
			if rep := c.unlicensedReporter; rep != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("rbn: unlicensed reporter panic: %v", r)
						}
					}()
					rep(evt.source, evt.role, evt.call, evt.mode, evt.freq)
				}()
			}
		case <-c.shutdown:
			return
		}
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

func (c *Client) dispatchUnlicensed(role, call, mode string, freq float64) {
	rep := c.unlicensedReporter
	if rep == nil {
		return
	}
	if c.unlicensedQueue != nil {
		select {
		case c.unlicensedQueue <- unlicensedEvent{source: c.sourceKey(), role: role, call: call, mode: mode, freq: freq}:
			return
		default:
			// fall through to async direct call
		}
	}
	go rep(c.sourceKey(), role, call, mode, freq)
}

// establishConnection dials the remote RBN feed and spins up the login and read
// goroutines. It is used for the initial connection and each subsequent reconnect.
func (c *Client) establishConnection() error {
	addr := net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port))
	log.Printf("%s: connecting to %s...", c.displayName(), addr)

	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 2 * time.Minute, // OS-level keepalive to detect silent mid-path drops
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.displayName(), err)
	}

	readerConn := conn
	writerConn := conn
	if c.useZiutekTelnet() {
		tconn, err := ztelnet.NewConn(conn)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to wrap telnet connection for %s: %w", c.displayName(), err)
		}
		readerConn = tconn
		writerConn = tconn
	}

	c.conn = conn
	c.reader = bufio.NewReader(readerConn)
	c.writer = bufio.NewWriter(writerConn)
	c.connected = true
	c.keepaliveDone = make(chan struct{})

	log.Printf("%s: connection established", c.displayName())

	// Start login sequence and stream reader for this connection.
	go c.handleLogin()
	if c.keepaliveInterval > 0 {
		go c.keepaliveLoop()
	}
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
	// Use CRLF for telnet-style compatibility with RBN servers.
	c.writer.WriteString(c.callsign + "\r\n")
	c.writer.Flush()
}

// readLoop reads lines from RBN
func (c *Client) readLoop() {
	// Guard the ingest goroutine so malformed input cannot crash the process.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s: panic in read loop: %v\n%s", c.displayName(), r, debug.Stack())
			c.requestReconnect(fmt.Errorf("panic: %v", r))
		}
	}()
	defer func() {
		c.connected = false
		if c.keepaliveDone != nil {
			close(c.keepaliveDone)
		}
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
				continue
			}

			// In minimal mode, forward any non-DX lines (e.g., WCY/WWV) to the raw passthrough.
			if c.minimalParse && c.rawChan != nil {
				select {
				case c.rawChan <- line:
				default:
				}
			}
		}
	}
}

// normalizeRBNCallsign removes the SSID portion from RBN skimmer callsigns. Example:
// "W3LPL-1-#" becomes "W3LPL-#".
func normalizeRBNCallsign(call string) string {
	if cached, ok := rbnNormalizeCache.Get(call); ok {
		return cached
	}
	// Check if it ends with -# (RBN skimmer indicator)
	if !strings.HasSuffix(call, "-#") {
		normalized := spot.NormalizeCallsign(call)
		rbnNormalizeCache.Add(call, normalized)
		return normalized
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
		normalized := basecall + "-#"
		rbnNormalizeCache.Add(call, normalized)
		return normalized
	}

	// If no SSID, return as-is with -# back
	rbnNormalizeCache.Add(call, call)
	return call
}

// normalizeSpotter normalizes the spotter (DE) callsign for processing. SSID
// suffixes are preserved so dedup/history can keep per-skimmer identity; any
// broadcast-time collapsing is handled downstream.
func (c *Client) normalizeSpotter(raw string) string {
	return spot.NormalizeCallsign(raw)
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

func buildComment(tokens []spotToken, consumed []bool) string {
	parts := make([]string, 0, len(tokens))
	for i, tok := range tokens {
		if consumed[i] {
			continue
		}
		clean := strings.TrimSpace(tok.clean)
		if clean == "" {
			continue
		}
		parts = append(parts, clean)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// parseSpot converts a DX cluster-style telnet line into a canonical Spot.
// Structural fields (DE/DX/freq/time) are parsed locally; comment parsing
// (explicit mode/report/time token handling) is delegated to spot.ParseSpotComment
// so RBN/human/peer inputs stay consistent. Mode inference happens downstream.
func (c *Client) parseSpot(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	tokens := tokenizeSpotLine(line)
	if len(tokens) < 3 {
		return
	}
	if strings.ToUpper(tokens[0].clean) != "DX" || strings.ToUpper(tokens[1].clean) != "DE" {
		return
	}
	consumed := make([]bool, len(tokens))
	consumed[0], consumed[1] = true, true

	deCallRaw, freqFromCall, freqOK := extractCallAndFreq(tokens[2])
	if strings.TrimSpace(deCallRaw) == "" {
		log.Printf("RBN spot missing spotter callsign: %s", line)
		return
	}
	consumed[2] = true
	deCall := c.normalizeSpotter(deCallRaw)

	freq := freqFromCall
	hasFreq := freqOK

	var dxCall string

	for idx := 3; idx < len(tokens); idx++ {
		tok := tokens[idx]
		clean := tok.clean
		if clean == "" {
			consumed[idx] = true
			continue
		}
		if !hasFreq {
			if f, ok := parseFrequencyCandidate(clean); ok {
				freq = f
				hasFreq = true
				consumed[idx] = true
				continue
			}
		}

		if hasFreq && dxCall == "" && spot.IsValidCallsign(clean) {
			dxCall = normalizeRBNCallsign(clean)
			consumed[idx] = true
			continue
		}
	}

	if !hasFreq {
		log.Printf("RBN spot missing numeric frequency: %s", line)
		return
	}
	if dxCall == "" {
		log.Printf("RBN spot missing DX callsign: %s", line)
		return
	}

	parsed := spot.ParseSpotComment(buildComment(tokens, consumed), freq)
	mode := parsed.Mode
	if !spot.IsValidCallsign(dxCall) || !spot.IsValidCallsign(deCall) {
		return
	}

	var dxMeta, deMeta spot.CallMetadata
	if c.minimalParse {
		if info, ok := c.fetchCallsignInfo(dxCall); ok {
			dxMeta = metadataFromPrefix(info)
		}
		if info, ok := c.fetchCallsignInfo(deCall); ok {
			deMeta = metadataFromPrefix(info)
		}
	} else {
		dxInfo, ok := c.fetchCallsignInfo(dxCall)
		if !ok {
			return
		}
		deInfo, ok := c.fetchCallsignInfo(deCall)
		if !ok {
			return
		}
		if deInfo != nil && deInfo.ADIF == 291 && !uls.IsLicensedUS(deCall) {
			c.dispatchUnlicensed("DE", deCall, mode, freq)
			return
		}
		dxMeta = metadataFromPrefix(dxInfo)
		deMeta = metadataFromPrefix(deInfo)
	}

	comment := parsed.Comment
	report := parsed.Report
	hasReport := parsed.HasReport

	if !c.minimalParse {
		freq = skew.ApplyCorrection(c.skewStore, deCallRaw, freq)
	}

	s := spot.NewSpot(dxCall, deCall, freq, mode)
	s.DXMetadata = dxMeta
	s.DEMetadata = deMeta
	if parsed.TimeToken != "" {
		s.Time = parseTimeFromRBN(parsed.TimeToken)
	}
	if hasReport {
		s.Report = report
		s.HasReport = true
	}
	if comment != "" {
		s.Comment = comment
	}
	s.IsHuman = c.minimalParse
	if c.minimalParse {
		s.SourceType = spot.SourceUpstream
		if strings.TrimSpace(c.name) != "" {
			s.SourceNode = c.name
		}
	} else {
		switch s.Mode {
		case "FT8":
			s.SourceType = spot.SourceFT8
		case "FT4":
			s.SourceType = spot.SourceFT4
		default:
			s.SourceType = spot.SourceRBN
		}
		if c.port == 7001 {
			s.SourceNode = "RBN-DIGITAL"
		} else {
			s.SourceNode = "RBN"
		}
	}

	s.RefreshBeaconFlag()
	s.EnsureNormalized()

	select {
	case c.spotChan <- s:
	default:
		log.Printf("%s: Spot channel full (capacity=%d), dropping spot", c.displayName(), cap(c.spotChan))
	}
}

func (c *Client) fetchCallsignInfo(call string) (*cty.PrefixInfo, bool) {
	if c.lookup == nil {
		return nil, true
	}
	db := c.lookup()
	if db == nil {
		return nil, true
	}
	info, ok := db.LookupCallsign(call)
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

func (c *Client) sourceKey() string {
	if c.port == 7001 {
		return "RBN-DIGITAL"
	}
	return "RBN"
}

// keepaliveLoop sends periodic CRLF to keep upstream telnet feeds from timing out idle sessions.
func (c *Client) keepaliveLoop() {
	ticker := time.NewTicker(c.keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.shutdown:
			return
		case <-c.keepaliveDone:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			if c.writer != nil {
				_, _ = c.writer.WriteString("\r\n")
				_ = c.writer.Flush()
			}
			c.writeMu.Unlock()
		}
	}
}
