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
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"os"

	"dxcluster/cty"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/uls"

	"gopkg.in/yaml.v3"
)

// precompiled regex avoids the per-line allocation/compile cost when normalizing RBN lines
var whitespaceRE = regexp.MustCompile(`\s+`)

const (
	minRBNDialFrequencyKHz = 100.0
	maxRBNDialFrequencyKHz = 3000000.0
)

var (
	rbnCallCacheSize  = 4096
	rbnCallCacheTTL   = 10 * time.Minute
	rbnNormalizeCache = spot.NewCallCache(rbnCallCacheSize, rbnCallCacheTTL)
	snrPattern        = regexp.MustCompile(`(?i)([-+]?\d{1,3})\s*dB`)
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
	lookup    *cty.CTYDatabase
	skewStore *skew.Store
	reconnect chan struct{}
	stopOnce  sync.Once
	keepSSID  bool

	bufferSize int

	unlicensedReporter UnlicensedReporter
	unlicensedQueue    chan unlicensedEvent

	minimalParse bool
}

type modeAllocation struct {
	Band      string  `yaml:"band"`
	LowerKHz  float64 `yaml:"lower_khz"`
	CWEndKHz  float64 `yaml:"cw_end_khz"`
	UpperKHz  float64 `yaml:"upper_khz"`
	VoiceMode string  `yaml:"voice_mode"`
}

type modeAllocTable struct {
	Bands []modeAllocation `yaml:"bands"`
}

var (
	modeAllocOnce sync.Once
	modeAlloc     []modeAllocation
)

const modeAllocPath = "config/mode_allocations.yaml"

// detectModeFromTokens attempts to infer mode from free-form human comment tokens.
// It stays conservative: only unambiguous mode tokens are used; otherwise the
// caller falls back to the band-based allocation table.
func detectModeFromTokens(tokens []string, freqKHz float64) string {
	for _, tok := range tokens {
		clean := strings.TrimSpace(strings.Trim(tok, ",.;!:"))
		cleanUpper := strings.ToUpper(clean)
		switch cleanUpper {
		case "FT8", "FT-8":
			return "FT8"
		case "FT4", "FT-4":
			return "FT4"
		case "RTTY":
			return "RTTY"
		case "CWT", "CW":
			return "CW"
		case "MSK144", "MSK-144", "MSK":
			return "MSK144"
		case "USB":
			return "USB"
		case "LSB":
			return "LSB"
		case "SSB":
			if freqKHz >= 10000 {
				return "USB"
			}
			return "LSB"
		}
	}
	return ""
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func detectSNR(tokens []string, mode string) (int, bool) {
	// Fast path: scan the whole comment with a tolerant regex (handles "-12dB" or "-12 dB").
	if len(tokens) > 0 {
		joined := strings.Join(tokens, " ")
		if m := snrPattern.FindStringSubmatch(joined); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil && v >= -200 && v <= 200 {
				return v, true
			}
		}
	}

	for i, tok := range tokens {
		clean := strings.TrimSpace(strings.Trim(tok, ",.;!:"))
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		if len(lower) >= 5 && strings.HasSuffix(lower, "z") && isAllDigits(lower[:4]) {
			continue // skip time tokens
		}
		// Handle two-token pattern: "<num> dB"
		if lower == "db" && i > 0 {
			prev := strings.ToLower(strings.TrimSpace(strings.Trim(tokens[i-1], ",.;!:")))
			if prev == "" || strings.Contains(prev, ".") {
				continue
			}
			if snr, err := strconv.Atoi(prev); err == nil && snr >= -200 && snr <= 200 {
				return snr, true
			}
			continue
		}

		hasDB := strings.HasSuffix(lower, "db")
		numStr := strings.TrimSuffix(lower, "db")
		if strings.Contains(numStr, ".") {
			continue // skip floats (likely frequencies)
		}
		// Require an explicit dB marker either in this token or the next one.
		if !hasDB {
			if i+1 >= len(tokens) {
				continue
			}
			next := strings.ToLower(strings.TrimSpace(strings.Trim(tokens[i+1], ",.;!:")))
			if next != "db" {
				continue
			}
		}
		if snr, err := strconv.Atoi(numStr); err == nil && snr >= -200 && snr <= 200 {
			return snr, true
		}
	}
	return 0, false
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
func NewClient(host string, port int, callsign string, name string, lookup *cty.CTYDatabase, skewStore *skew.Store, keepSSID bool, bufferSize int) *Client {
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

func loadModeAllocations() {
	modeAllocOnce.Do(func() {
		data, err := os.ReadFile(modeAllocPath)
		if err != nil {
			log.Printf("Warning: unable to load mode allocations from %s: %v", modeAllocPath, err)
			return
		}
		var table modeAllocTable
		if err := yaml.Unmarshal(data, &table); err != nil {
			log.Printf("Warning: unable to parse mode allocations (%s): %v", modeAllocPath, err)
			return
		}
		modeAlloc = table.Bands
	})
}

func guessModeFromAlloc(freqKHz float64) string {
	loadModeAllocations()
	for _, b := range modeAlloc {
		if freqKHz >= b.LowerKHz && freqKHz <= b.UpperKHz {
			if b.CWEndKHz > 0 && freqKHz <= b.CWEndKHz {
				return "CW"
			}
			if strings.TrimSpace(b.VoiceMode) != "" {
				return strings.ToUpper(strings.TrimSpace(b.VoiceMode))
			}
		}
	}
	return ""
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

// parseMinimalSpot attempts a permissive parse for human/relay feeds.
//
// Expected tokens: at least two callsigns and one numeric frequency (kHz).
// It is tolerant of additional noise and will:
//   - Detect and remove a trailing HHMMZ token (so time never becomes "comment")
//   - Detect and remove a mode token (FT8/FT4/CW/RTTY/USB/LSB/SSB/MSK144)
//   - Detect and remove a report token only when it is explicitly marked as dB
//     (e.g., "-12 dB" or "-12dB")
//
// Any remaining tokens are joined as the spot's free-form Comment.
func (c *Client) parseMinimalSpot(parts []string, rawLine string) {
	timeIdx := -1
	timeToken := ""
	// Detect trailing HHMMZ token so it does not leak into the comment.
	if len(parts) > 0 {
		last := strings.TrimSpace(parts[len(parts)-1])
		if len(last) == 5 && strings.HasSuffix(last, "Z") {
			digits := last[:4]
			isDigits := true
			for _, r := range digits {
				if r < '0' || r > '9' {
					isDigits = false
					break
				}
			}
			if isDigits {
				timeIdx = len(parts) - 1
				timeToken = last
			}
		}
	}

	var (
		freqKHz float64
		freqOK  bool
		calls   []string
		freqIdx = -1
		callIdx []int
	)
	for i, p := range parts {
		if i == timeIdx {
			continue
		}
		trimmed := strings.TrimSuffix(strings.TrimSpace(p), ":")
		if trimmed == "" {
			continue
		}
		if !freqOK {
			if f, err := strconv.ParseFloat(trimmed, 64); err == nil && f > 0 {
				freqKHz = f
				freqOK = true
				freqIdx = i
				continue
			}
		}
		if spot.IsValidCallsign(trimmed) {
			normalized := normalizeRBNCallsign(trimmed)
			if normalized != "" {
				calls = append(calls, normalized)
				callIdx = append(callIdx, i)
			}
		}
	}
	if !freqOK || len(calls) < 2 {
		log.Printf("Minimal human spot rejected (needs DE, DX, freq): %s", rawLine)
		return
	}
	deCall := c.normalizeSpotter(calls[0])
	dxCall := calls[1]

	used := make(map[int]bool)
	if freqIdx >= 0 {
		used[freqIdx] = true
	}
	if len(callIdx) > 0 {
		used[callIdx[0]] = true
		if len(callIdx) > 1 {
			used[callIdx[1]] = true
		}
	}
	if timeIdx >= 0 {
		used[timeIdx] = true
	}
	commentTokens := make([]string, 0, len(parts))
	for i, tok := range parts {
		if used[i] {
			continue
		}
		// Strip structural noise commonly seen in relayed lines.
		if strings.EqualFold(tok, "DX") || strings.EqualFold(tok, "de") {
			continue
		}
		commentTokens = append(commentTokens, tok)
	}

	// Optional CTY enrichment; allow missing info.
	var dxMeta, deMeta spot.CallMetadata
	if info, ok := c.fetchCallsignInfo(dxCall); ok {
		dxMeta = metadataFromPrefix(info)
	}
	if info, ok := c.fetchCallsignInfo(deCall); ok {
		deMeta = metadataFromPrefix(info)
	}

	// Find mode/SNR while also scrubbing those tokens from the comment.
	mode := ""
	modeIdx := -1
	for i, tok := range commentTokens {
		clean := strings.TrimSpace(strings.Trim(tok, ",.;!:"))
		cleanUpper := strings.ToUpper(clean)
		switch cleanUpper {
		case "FT8", "FT-8":
			mode = "FT8"
			modeIdx = i
		case "FT4", "FT-4":
			mode = "FT4"
			modeIdx = i
		case "RTTY":
			mode = "RTTY"
			modeIdx = i
		case "CWT", "CW":
			mode = "CW"
			modeIdx = i
		case "MSK144", "MSK-144", "MSK":
			mode = "MSK144"
			modeIdx = i
		case "USB":
			mode = "USB"
			modeIdx = i
		case "LSB":
			mode = "LSB"
			modeIdx = i
		case "SSB":
			if freqKHz >= 10000 {
				mode = "USB"
			} else {
				mode = "LSB"
			}
			modeIdx = i
		}
		if mode != "" {
			break
		}
	}
	if mode == "" {
		mode = guessModeFromAlloc(freqKHz)
	}
	if mode == "" {
		mode = "RTTY" // fallback when table missing or out of band
	}

	// Detect SNR and mark consumed tokens.
	snr := 0
	hasSNR := false
	snrIdx := make(map[int]bool)
	for i, tok := range commentTokens {
		clean := strings.TrimSpace(strings.Trim(tok, ",.;!:"))
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		if len(lower) == 5 && strings.HasSuffix(lower, "z") {
			continue // time token
		}
		if lower == "db" && i > 0 {
			prev := strings.ToLower(strings.TrimSpace(strings.Trim(commentTokens[i-1], ",.;!")))
			if prev == "" || strings.Contains(prev, ".") {
				continue
			}
			if v, err := strconv.Atoi(prev); err == nil && v >= -200 && v <= 200 {
				snr = v
				hasSNR = true
				snrIdx[i] = true
				snrIdx[i-1] = true
				break
			}
			continue
		}
		hasDB := strings.HasSuffix(lower, "db")
		numStr := strings.TrimSuffix(lower, "db")
		if strings.Contains(numStr, ".") {
			continue
		}
		if !hasDB {
			if i+1 >= len(commentTokens) {
				continue
			}
			next := strings.ToLower(strings.TrimSpace(strings.Trim(commentTokens[i+1], ",.;!:")))
			if next != "db" {
				continue
			}
		}
		if v, err := strconv.Atoi(numStr); err == nil && v >= -200 && v <= 200 {
			snr = v
			hasSNR = true
			snrIdx[i] = true
			if !hasDB && i+1 < len(commentTokens) {
				snrIdx[i+1] = true
			}
			break
		}
	}

	s := spot.NewSpot(dxCall, deCall, freqKHz, mode)
	s.IsHuman = true
	s.SourceType = spot.SourceUpstream
	if strings.TrimSpace(c.name) != "" {
		s.SourceNode = c.name
	}
	s.DXMetadata = dxMeta
	s.DEMetadata = deMeta
	if timeToken != "" {
		s.Time = parseTimeFromRBN(timeToken)
	}
	if hasSNR {
		s.Report = snr
		s.HasReport = true
	}
	// Strip mode/SNR/time/DX/de from the comment payload.
	cleaned := make([]string, 0, len(commentTokens))
	for i, tok := range commentTokens {
		if i == modeIdx || snrIdx[i] {
			continue
		}
		clean := strings.TrimSpace(strings.Trim(tok, ",.;!:"))
		if clean == "" {
			continue
		}
		upper := strings.ToUpper(clean)
		if upper == "DX" || upper == "DE" {
			continue
		}
		if len(upper) >= 5 && strings.HasSuffix(upper, "Z") && isAllDigits(upper[:4]) {
			continue
		}
		cleaned = append(cleaned, clean)
	}
	if len(cleaned) > 0 {
		s.Comment = strings.Join(cleaned, " ")
	}
	s.EnsureNormalized()

	select {
	case c.spotChan <- s:
	default:
		log.Printf("%s: Spot channel full (capacity=%d), dropping minimal spot", c.displayName(), cap(c.spotChan))
	}
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

	// For minimal/human feeds, bypass strict RBN parsing and use the coarse parser.
	if c.minimalParse {
		c.parseMinimalSpot(parts, line)
		return
	}

	// Minimum: DX de CALL: FREQ DXCALL MODE DB dB TIME
	// Example CW: [DX de G4ZFE-#: 10111.0 LZ2PC CW 11 dB 22 WPM CQ 1928Z]
	// Example FT8: [DX de W3LPL-#: 14074.0 K1ABC FT8 -5 dB 2359Z]
	if len(parts) < 9 {
		if c.minimalParse {
			c.parseMinimalSpot(parts, line)
			return
		}
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

	dxCall := normalizeRBNCallsign(parts[freqIdx+1])
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
	// Drop US spotters without an active FCC license before building the spot to avoid downstream work.
	if deInfo != nil && deInfo.ADIF == 291 && !uls.IsLicensedUS(deCall) {
		c.dispatchUnlicensed("DE", deCall, strings.ToUpper(mode), freq)
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
	s.HasReport = true

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
		log.Printf("%s: Spot channel full (capacity=%d), dropping spot", c.displayName(), cap(c.spotChan))
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

func (c *Client) sourceKey() string {
	if c.port == 7001 {
		return "RBN-DIGITAL"
	}
	return "RBN"
}
