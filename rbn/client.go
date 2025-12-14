// Package rbn maintains TCP connections to the Reverse Beacon Network (CW/RTTY
// and FT4/FT8 feeds), parsing telnet lines into canonical *spot.Spot entries
// with CTY enrichment and optional skew corrections.
package rbn

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"path/filepath"
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

type acTokenKind int

const (
	acTokenUnknown acTokenKind = iota
	acTokenDX
	acTokenDE
	acTokenMode
	acTokenDB
	acTokenWPM
)

type acPattern struct {
	word string
	kind acTokenKind
	mode string
}

type acMatch struct {
	start   int
	end     int
	pattern acPattern
}

type acNode struct {
	fail    int
	next    map[byte]int
	outputs []int
}

type acScanner struct {
	patterns []acPattern
	nodes    []acNode
}

func newACScanner(patterns []acPattern) *acScanner {
	sc := &acScanner{
		patterns: patterns,
		nodes:    []acNode{{next: make(map[byte]int)}},
	}
	for idx, p := range patterns {
		state := 0
		for i := 0; i < len(p.word); i++ {
			ch := p.word[i]
			next, ok := sc.nodes[state].next[ch]
			if !ok {
				next = len(sc.nodes)
				sc.nodes = append(sc.nodes, acNode{next: make(map[byte]int)})
				sc.nodes[state].next[ch] = next
			}
			state = next
		}
		sc.nodes[state].outputs = append(sc.nodes[state].outputs, idx)
	}

	// Build failure links (BFS).
	queue := make([]int, 0, len(sc.nodes))
	for _, next := range sc.nodes[0].next {
		queue = append(queue, next)
	}
	for len(queue) > 0 {
		state := queue[0]
		queue = queue[1:]
		for ch, next := range sc.nodes[state].next {
			fail := sc.nodes[state].fail
			for fail > 0 {
				if target, ok := sc.nodes[fail].next[ch]; ok {
					fail = target
					break
				}
				fail = sc.nodes[fail].fail
			}
			sc.nodes[next].fail = fail
			sc.nodes[next].outputs = append(sc.nodes[next].outputs, sc.nodes[fail].outputs...)
			queue = append(queue, next)
		}
	}
	return sc
}

func (sc *acScanner) FindAll(text string) []acMatch {
	if sc == nil {
		return nil
	}
	state := 0
	matches := make([]acMatch, 0, 8)
	for i := 0; i < len(text); i++ {
		ch := text[i]
		next, ok := sc.nodes[state].next[ch]
		for !ok && state > 0 {
			state = sc.nodes[state].fail
			next, ok = sc.nodes[state].next[ch]
		}
		if ok {
			state = next
		}
		if len(sc.nodes[state].outputs) == 0 {
			continue
		}
		end := i + 1
		for _, pid := range sc.nodes[state].outputs {
			p := sc.patterns[pid]
			start := end - len(p.word)
			if start >= 0 {
				matches = append(matches, acMatch{start: start, end: end, pattern: p})
			}
		}
	}
	return matches
}

func buildMatchIndex(matches []acMatch) map[int][]acMatch {
	if len(matches) == 0 {
		return nil
	}
	index := make(map[int][]acMatch, len(matches))
	for _, m := range matches {
		index[m.start] = append(index[m.start], m)
	}
	return index
}

func classifyToken(matchIndex map[int][]acMatch, trimStart, trimEnd int) (acPattern, bool) {
	if len(matchIndex) == 0 {
		return acPattern{}, false
	}
	for _, m := range matchIndex[trimStart] {
		if m.end == trimEnd {
			return m.pattern, true
		}
	}
	return acPattern{}, false
}

func classifyTokenWithFallback(matchIndex map[int][]acMatch, tok spotToken) (acPattern, bool) {
	if pat, ok := classifyToken(matchIndex, tok.trimStart, tok.trimEnd); ok {
		return pat, true
	}
	// Fallback: scan the token itself to tolerate any positional drift from the
	// global match index (e.g., doubled spaces or trimmed punctuation).
	for _, m := range getKeywordScanner().FindAll(tok.upper) {
		if m.start == 0 && m.end == len(tok.upper) {
			return m.pattern, true
		}
	}
	return acPattern{}, false
}

var keywordPatterns = []acPattern{
	{word: "DX", kind: acTokenDX},
	{word: "DE", kind: acTokenDE},
	{word: "DB", kind: acTokenDB},
	{word: "WPM", kind: acTokenWPM},
	{word: "CW", kind: acTokenMode, mode: "CW"},
	{word: "CWT", kind: acTokenMode, mode: "CW"},
	{word: "RTTY", kind: acTokenMode, mode: "RTTY"},
	{word: "FT8", kind: acTokenMode, mode: "FT8"},
	{word: "FT-8", kind: acTokenMode, mode: "FT8"},
	{word: "FT4", kind: acTokenMode, mode: "FT4"},
	{word: "FT-4", kind: acTokenMode, mode: "FT4"},
	{word: "MSK", kind: acTokenMode, mode: "MSK144"},
	{word: "MSK144", kind: acTokenMode, mode: "MSK144"},
	{word: "MSK-144", kind: acTokenMode, mode: "MSK144"},
	{word: "USB", kind: acTokenMode, mode: "USB"},
	{word: "LSB", kind: acTokenMode, mode: "LSB"},
	{word: "SSB", kind: acTokenMode, mode: "SSB"},
}

var keywordScannerOnce sync.Once
var keywordScanner *acScanner

func getKeywordScanner() *acScanner {
	keywordScannerOnce.Do(func() {
		keywordScanner = newACScanner(keywordPatterns)
	})
	return keywordScanner
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
		paths := []string{modeAllocPath, filepath.Join("..", modeAllocPath)}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var table modeAllocTable
			if err := yaml.Unmarshal(data, &table); err != nil {
				log.Printf("Warning: unable to parse mode allocations (%s): %v", path, err)
				return
			}
			modeAlloc = table.Bands
			return
		}
		log.Printf("Warning: unable to load mode allocations from %s (or parent): file not found", modeAllocPath)
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

func normalizeVoiceMode(mode string, freqKHz float64) string {
	upper := strings.ToUpper(strings.TrimSpace(mode))
	if upper == "SSB" {
		if freqKHz >= 10000 {
			return "USB"
		}
		return "LSB"
	}
	return upper
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

func parseSignedInt(tok string) (int, bool) {
	if tok == "" {
		return 0, false
	}
	if strings.Contains(tok, ".") {
		return 0, false
	}
	v, err := strconv.Atoi(tok)
	if err != nil {
		return 0, false
	}
	if v < -200 || v > 200 {
		return 0, false
	}
	return v, true
}

func parseInlineSNR(tok string) (int, bool) {
	lower := strings.ToLower(strings.TrimSpace(tok))
	if !strings.HasSuffix(lower, "db") {
		return 0, false
	}
	numStr := strings.TrimSuffix(lower, "db")
	if strings.Contains(numStr, ".") || numStr == "" {
		return 0, false
	}
	v, err := strconv.Atoi(numStr)
	if err != nil || v < -200 || v > 200 {
		return 0, false
	}
	return v, true
}

func peelTimePrefix(tok string) (string, string) {
	if len(tok) < 5 {
		return "", tok
	}
	prefix := tok[:5]
	if isTimeToken(prefix) {
		return prefix, strings.TrimSpace(tok[5:])
	}
	return "", tok
}

func isTimeToken(tok string) bool {
	if len(tok) != 5 || tok[4] != 'Z' {
		return false
	}
	for i := 0; i < 4; i++ {
		if tok[i] < '0' || tok[i] > '9' {
			return false
		}
	}
	return true
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

func finalizeMode(mode string, freq float64) string {
	mode = normalizeVoiceMode(mode, freq)
	if mode != "" {
		return mode
	}
	alloc := guessModeFromAlloc(freq)
	if alloc != "" {
		return normalizeVoiceMode(alloc, freq)
	}
	if freq >= 10000 {
		return "USB"
	}
	return "CW"
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
		upper := strings.ToUpper(clean)
		if upper == "DX" || upper == "DE" {
			continue
		}
		if len(upper) == 5 && upper[4] == 'Z' && isAllDigits(upper[:4]) {
			continue
		}
		parts = append(parts, clean)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// parseSpot converts a DX cluster-style telnet line into a canonical Spot using
// a single left-to-right pass paired with an Aho-Corasick keyword scan.
// The AC automaton tags structural tokens (DX/DE, modes, dB, WPM) so we can
// peel fields without multiple rescans or regex passes.
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
	matchIndex := buildMatchIndex(getKeywordScanner().FindAll(strings.ToUpper(line)))
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

	var (
		dxCall          string
		mode            string
		timeToken       string
		wpmStr          string
		report          int
		hasReport       bool
		pendingNumIdx   = -1
		pendingNumValue int
	)

	for idx := 3; idx < len(tokens); idx++ {
		tok := tokens[idx]
		originalClean := tok.clean
		clean := originalClean
		if timeToken == "" {
			if ts, remainder := peelTimePrefix(clean); ts != "" {
				timeToken = ts
				shift := len(originalClean) - len(remainder)
				clean = remainder
				tokens[idx].clean = remainder
				tokens[idx].upper = strings.ToUpper(remainder)
				tokens[idx].trimStart = tok.trimStart + shift
				tokens[idx].trimEnd = tokens[idx].trimStart + len(remainder)
				tok = tokens[idx]
			}
		}
		if clean == "" {
			consumed[idx] = true
			continue
		}
		if timeToken == "" && isTimeToken(clean) {
			timeToken = clean
			consumed[idx] = true
			pendingNumIdx = -1
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

		if pat, ok := classifyTokenWithFallback(matchIndex, tok); ok {
			switch pat.kind {
			case acTokenMode:
				if mode == "" {
					mode = normalizeVoiceMode(pat.mode, freq)
					consumed[idx] = true
					continue
				}
			case acTokenDB:
				if !hasReport && pendingNumIdx >= 0 {
					report = pendingNumValue
					hasReport = true
					consumed[idx] = true
					consumed[pendingNumIdx] = true
					pendingNumIdx = -1
					continue
				}
				consumed[idx] = true
				continue
			case acTokenWPM:
				if wpmStr == "" && pendingNumIdx >= 0 {
					wpmStr = tokens[pendingNumIdx].clean
					consumed[idx] = true
					consumed[pendingNumIdx] = true
					pendingNumIdx = -1
					continue
				}
			case acTokenDX, acTokenDE:
				consumed[idx] = true
				continue
			}
		}

		if !hasReport {
			if v, ok := parseInlineSNR(clean); ok {
				report = v
				hasReport = true
				consumed[idx] = true
				continue
			}
		}

		if hasFreq && dxCall == "" && spot.IsValidCallsign(clean) {
			dxCall = normalizeRBNCallsign(clean)
			consumed[idx] = true
			continue
		}

		if pendingNumIdx == -1 {
			if v, ok := parseSignedInt(clean); ok {
				pendingNumIdx = idx
				pendingNumValue = v
				continue
			}
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

	mode = finalizeMode(mode, freq)
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

	comment := buildComment(tokens, consumed)
	if !hasReport && comment != "" {
		if m := snrPattern.FindStringSubmatch(comment); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil {
				report = v
				hasReport = true
			}
		}
	}
	if !hasReport {
		if m := snrPattern.FindStringSubmatch(line); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil {
				report = v
				hasReport = true
			}
		}
	}
	if wpmStr != "" {
		if comment != "" {
			comment = fmt.Sprintf("%s WPM %s", wpmStr, comment)
		} else {
			comment = fmt.Sprintf("%s WPM", wpmStr)
		}
	}

	if !c.minimalParse {
		freq = skew.ApplyCorrection(c.skewStore, deCallRaw, freq)
	}

	s := spot.NewSpot(dxCall, deCall, freq, mode)
	s.DXMetadata = dxMeta
	s.DEMetadata = deMeta
	if timeToken != "" {
		s.Time = parseTimeFromRBN(timeToken)
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
