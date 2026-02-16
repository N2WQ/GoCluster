// Package rbn maintains TCP connections to the Reverse Beacon Network (CW/RTTY
// and FT4/FT8 feeds), parsing telnet lines into canonical *spot.Spot entries
// with optional skew corrections.
package rbn

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/skew"
	"dxcluster/spot"
	ztelnet "github.com/ziutek/telnet"
)

const (
	minRBNDialFrequencyKHz = 100.0
	maxRBNDialFrequencyKHz = 3000000.0
	rbnMaxLineLength       = 1024
	rbnWriteDeadline       = 10 * time.Second
)

var (
	rbnCallCacheSize  = 4096
	rbnCallCacheTTL   = 10 * time.Minute
	rbnNormalizeCache = spot.NewCallCache(rbnCallCacheSize, rbnCallCacheTTL)
)

// Client represents an RBN telnet client
type Client struct {
	host       string
	port       int
	callsign   string
	name       string
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	connected  atomic.Bool
	shutdown   chan struct{}
	spotChan   chan *spot.Spot
	skewStore  *skew.Store
	reconnect  chan struct{}
	stopOnce   sync.Once
	writeMu    sync.Mutex
	lastLineAt atomic.Int64
	lastSpotAt atomic.Int64
	spotDrops  atomic.Uint64
	keepSSID   bool

	bufferSize int

	minimalParse bool

	telnetTransport   string
	keepaliveInterval time.Duration
	keepaliveDone     chan struct{}

	rawChan chan<- string // optional passthrough for non-DX lines (minimal parser only)
}

type spotToken struct {
	raw       string
	clean     string
	start     int
	end       int
	trimStart int
	trimEnd   int
}

var trimPunctuation = [256]bool{
	',': true,
	';': true,
	':': true,
	'!': true,
	'.': true,
}

type errLineTooLong struct {
	length int
}

func (e errLineTooLong) Error() string {
	return "rbn line too long"
}

// Purpose: Tokenize an RBN spot line into position-aware tokens.
// Key aspects: Records raw/clean slices and punctuation-trim indices.
// Upstream: parseSpot for minimal parsing.
// Downstream: None.
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
		for trimStart < end && trimPunctuation[line[trimStart]] {
			trimStart++
		}
		for trimEnd > trimStart && trimPunctuation[line[trimEnd-1]] {
			trimEnd--
		}
		clean := line[trimStart:trimEnd]
		tokens = append(tokens, spotToken{
			raw:       raw,
			clean:     clean,
			start:     start,
			end:       end,
			trimStart: trimStart,
			trimEnd:   trimEnd,
		})
	}
	return tokens
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca := a[i]
		cb := b[i]
		if ca == cb {
			continue
		}
		if ca >= 'a' && ca <= 'z' {
			ca -= 'a' - 'A'
		}
		if cb >= 'a' && cb <= 'z' {
			cb -= 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ConfigureCallCache configures the callsign normalization cache for RBN spotters.
// Key aspects: Applies defaults and rebuilds the shared cache.
// Upstream: Config load or tests.
// Downstream: spot.NewCallCache.
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

// NewClient constructs an RBN telnet client.
// Key aspects: Initializes channels and caches; bufferSize absorbs bursty ingest.
// Upstream: main.go startup.
// Downstream: Client.Connect.
func NewClient(host string, port int, callsign string, name string, skewStore *skew.Store, keepSSID bool, bufferSize int) *Client {
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
		skewStore:  skewStore,
		reconnect:  make(chan struct{}, 1),
		keepSSID:   keepSSID,
		bufferSize: bufferSize,
	}
}

// UseMinimalParser enables the permissive parser for non-RBN telnet feeds.
// Key aspects: Extracts DE/DX/freq with optional mode/report/time tokens.
// Upstream: main.go for human/relay telnet clients.
// Downstream: parseSpot minimal parsing path.
func (c *Client) UseMinimalParser() {
	if c != nil {
		c.minimalParse = true
	}
}

// SetTelnetTransport select the telnet transport backend.
// Key aspects: Normalizes values; unrecognized values fall back to native.
// Upstream: Config load or caller setup.
// Downstream: useZiutekTelnet.
func (c *Client) SetTelnetTransport(transport string) {
	if c == nil {
		return
	}
	c.telnetTransport = strings.ToLower(strings.TrimSpace(transport))
}

// SetRawPassthrough install a raw line passthrough channel for minimal parsing.
// Key aspects: Non-blocking delivery to avoid ingest stalls.
// Upstream: main.go wiring for peer/raw feeds.
// Downstream: parseSpot raw line forwarding.
func (c *Client) SetRawPassthrough(ch chan<- string) {
	if c != nil {
		c.rawChan = ch
	}
}

// EnableKeepalive enables periodic CRLF keepalives for upstream telnet feeds.
// Key aspects: Stores interval for a later keepaliveLoop.
// Upstream: Config load or caller setup.
// Downstream: keepaliveLoop goroutine.
func (c *Client) EnableKeepalive(interval time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		return
	}
	c.keepaliveInterval = interval
}

// Purpose: Report whether the ziutek telnet backend is selected.
// Key aspects: Case-insensitive check.
// Upstream: Connection setup.
// Downstream: None.
func (c *Client) useZiutekTelnet() bool {
	return strings.EqualFold(c.telnetTransport, "ziutek")
}

// Purpose: Parse a numeric frequency token (kHz).
// Key aspects: Validates against reasonable dial range.
// Upstream: extractCallAndFreq.
// Downstream: strconv.ParseFloat.
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

// Purpose: Extract a callsign and frequency from a token like "CALL:freq".
// Key aspects: Uses the raw token to preserve punctuation positions.
// Upstream: Minimal parser in parseSpot.
// Downstream: parseFrequencyCandidate.
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

// Connect establishes the initial RBN connection and starts supervision.
// Key aspects: First dial is synchronous; reconnects happen in background.
// Upstream: main.go startup.
// Downstream: establishConnection, connectionSupervisor goroutine.
func (c *Client) Connect() error {
	if err := c.establishConnection(); err != nil {
		return err
	}
	// Goroutine: monitor reconnect signals and re-establish connections.
	go c.connectionSupervisor()
	return nil
}

// Purpose: Dial the RBN feed and start login/read loops.
// Key aspects: Wraps in telnet transport as configured and spawns goroutines.
// Upstream: Connect and reconnect loop.
// Downstream: handleLogin, keepaliveLoop, readLoop goroutines.
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
	c.connected.Store(true)
	c.keepaliveDone = make(chan struct{})

	log.Printf("%s: connection established", c.displayName())

	// Start login sequence and stream reader for this connection.
	go c.handleLogin()
	if c.keepaliveInterval > 0 {
		// Goroutine: emit periodic keepalives for upstream connection.
		go c.keepaliveLoop()
	}
	// Goroutine: read and parse incoming lines from the server.
	go c.readLoop()
	return nil
}

// Purpose: Supervise connection lifecycle and handle reconnects.
// Key aspects: Uses backoff and honors shutdown signals.
// Upstream: Connect goroutine.
// Downstream: establishConnection, requestReconnect.
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

// Purpose: Perform the RBN login sequence after connecting.
// Key aspects: Waits briefly for prompt; sends callsign with CRLF.
// Upstream: establishConnection goroutine.
// Downstream: writer.WriteString/Flush.
func (c *Client) handleLogin() {
	// Wait for login prompt and respond with callsign
	time.Sleep(2 * time.Second)

	if c.name != "" {
		log.Printf("Logging in to %s as %s", c.name, c.callsign)
	} else {
		log.Printf("Logging in to RBN as %s", c.callsign)
	}
	// Use CRLF for telnet-style compatibility with RBN servers.
	if err := c.writeStringLine(c.callsign + "\r\n"); err != nil {
		if c.isShutdown() {
			return
		}
		log.Printf("%s: login write failed: %v", c.displayName(), err)
		c.requestReconnect(err)
	}
}

// readLineBounded reads a single line with a hard cap to avoid unbounded buffers.
// It discards overlong lines and returns errLineTooLong so callers can continue safely.
func (c *Client) readLineBounded(maxLen int) (string, error) {
	if c == nil || c.reader == nil {
		return "", io.EOF
	}
	if maxLen <= 0 {
		maxLen = rbnMaxLineLength
	}
	buf := make([]byte, 0, maxLen)
	for {
		chunk, err := c.reader.ReadSlice('\n')
		if err == nil {
			if len(buf)+len(chunk) > maxLen {
				return "", errLineTooLong{length: len(buf) + len(chunk)}
			}
			buf = append(buf, chunk...)
			return string(buf), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(buf)+len(chunk) > maxLen {
				if discardErr := c.discardLineRemainder(); discardErr != nil {
					return "", discardErr
				}
				return "", errLineTooLong{length: len(buf) + len(chunk)}
			}
			buf = append(buf, chunk...)
			continue
		}
		return "", err
	}
}

func (c *Client) discardLineRemainder() error {
	if c == nil || c.reader == nil {
		return io.EOF
	}
	for {
		_, err := c.reader.ReadSlice('\n')
		if err == nil {
			return nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return err
	}
}

// Purpose: Read and parse incoming lines from the RBN connection.
// Key aspects: Uses read deadlines; triggers reconnect on errors.
// Upstream: establishConnection goroutine.
// Downstream: parseSpot, requestReconnect, raw passthrough.
func (c *Client) readLoop() {
	// Guard the ingest goroutine so malformed input cannot crash the process.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s: panic in read loop: %v\n%s", c.displayName(), r, debug.Stack())
			c.requestReconnect(fmt.Errorf("panic: %v", r))
		}
	}()
	defer func() {
		c.connected.Store(false)
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
			if err := c.conn.SetReadDeadline(time.Now().UTC().Add(5 * time.Minute)); err != nil {
				if c.isShutdown() {
					return
				}
				log.Printf("%s: failed to set read deadline: %v", c.displayName(), err)
				c.requestReconnect(err)
				return
			}

			line, err := c.readLineBounded(rbnMaxLineLength)
			if err != nil {
				var tooLong errLineTooLong
				if errors.As(err, &tooLong) {
					log.Printf("%s: dropping overlong line (%d bytes)", c.displayName(), tooLong.length)
					continue
				}
				if c.isShutdown() {
					return
				}
				log.Printf("%s: read error: %v", c.displayName(), err)
				c.requestReconnect(err)
				return
			}

			now := time.Now().UTC()
			line = strings.TrimSpace(line)

			// Skip empty lines
			if line == "" {
				continue
			}
			c.lastLineAt.Store(now.UnixNano())

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

// Purpose: Normalize RBN skimmer callsigns while preserving the -# suffix.
// Key aspects: Strips SSIDs from skimmer calls like "W3LPL-1-#".
// Upstream: parseSpot spotter normalization.
// Downstream: rbnNormalizeCache, spot.NormalizeCallsign.
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

	// If there are multiple hyphens, remove the last one (the SSID).
	// W3LPL-1 becomes W3LPL.
	if lastDash := strings.LastIndexByte(withoutHash, '-'); lastDash > 0 {
		basecall := withoutHash[:lastDash]
		normalized := basecall + "-#"
		rbnNormalizeCache.Add(call, normalized)
		return normalized
	}

	// If no SSID, return as-is with -# back
	rbnNormalizeCache.Add(call, call)
	return call
}

// Purpose: Normalize the spotter callsign while preserving SSIDs.
// Key aspects: Leaves SSID in place so dedup/history keep per-skimmer identity.
// Upstream: parseSpot.
// Downstream: spot.NormalizeCallsign.
func (c *Client) normalizeSpotter(raw string) string {
	return spot.NormalizeCallsign(raw)
}

const rbnMaxFutureSkew = 2 * time.Minute

// Purpose: Parse RBN HHMMZ timestamps into full UTC times.
// Key aspects: Uses today's UTC date; clamps future times beyond a small skew.
// Upstream: parseSpot.
// Downstream: time.Date, time.Now.
func parseTimeFromRBN(timeStr string) time.Time {
	return parseTimeFromRBNAt(timeStr, time.Now().UTC())
}

func parseTimeFromRBNAt(timeStr string, now time.Time) time.Time {
	// timeStr format is "HHMMZ" e.g. "0531Z"
	if len(timeStr) != 5 || !strings.HasSuffix(timeStr, "Z") {
		// Invalid format, return current time as fallback
		log.Printf("Warning: Invalid RBN time format: %s", timeStr)
		return now
	}

	// Extract hour and minute
	hourStr := timeStr[0:2]
	minStr := timeStr[2:4]

	hour, err1 := strconv.Atoi(hourStr)
	min, err2 := strconv.Atoi(minStr)

	if err1 != nil || err2 != nil || hour < 0 || hour > 23 || min < 0 || min > 59 {
		log.Printf("Warning: Failed to parse RBN time: %s", timeStr)
		return now
	}

	// Construct timestamp with parsed HH:MM and today's date.
	// Set seconds to 0 since RBN doesn't provide seconds.
	year, month, day := now.Date()
	spotTime := time.Date(year, month, day, hour, min, 0, 0, time.UTC)

	if spotTime.After(now.Add(rbnMaxFutureSkew)) {
		return now
	}

	return spotTime
}

// Purpose: Build a comment string from unconsumed tokens.
// Key aspects: Preserves token order and trims empty parts.
// Upstream: parseSpot minimal parsing.
// Downstream: None.
func buildComment(tokens []spotToken, consumed []bool) string {
	if len(tokens) == 0 || len(consumed) == 0 {
		return ""
	}
	totalLen := 0
	count := 0
	for i, tok := range tokens {
		if consumed[i] {
			continue
		}
		if tok.clean == "" {
			continue
		}
		if count > 0 {
			totalLen++
		}
		totalLen += len(tok.clean)
		count++
	}
	if count == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(totalLen)
	written := 0
	for i, tok := range tokens {
		if consumed[i] || tok.clean == "" {
			continue
		}
		if written > 0 {
			_ = b.WriteByte(' ')
		}
		_, _ = b.WriteString(tok.clean)
		written++
	}
	return b.String()
}

func isAllDigitsASCII(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func stripRBNSpeedCQComment(mode, comment string) string {
	// Purpose: Remove the exact "<number> WPM CQ" or "<number> BPS CQ" payload for RBN-only spots.
	// Key aspects: Exact 3-token match, digit-only speed, mode-specific unit gating.
	// Upstream: parseSpot (RBN path only).
	// Downstream: Spot.Comment assignment and fixed-width output formatting.
	if comment == "" || mode == "" {
		return comment
	}
	fields := strings.Fields(comment)
	if len(fields) != 3 {
		return comment
	}
	if !isAllDigitsASCII(fields[0]) {
		return comment
	}
	if !strings.EqualFold(fields[2], "CQ") {
		return comment
	}
	if strings.EqualFold(mode, "CW") && strings.EqualFold(fields[1], "WPM") {
		return ""
	}
	if strings.EqualFold(mode, "RTTY") && strings.EqualFold(fields[1], "BPS") {
		return ""
	}
	return comment
}

// Purpose: Parse a DX line into a canonical Spot.
// Key aspects: Extracts DE/DX/freq/time locally and delegates comment parsing.
// Upstream: readLoop for RBN/minimal feeds.
// Downstream: spot.ParseSpotComment, skew.ApplyCorrection.
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
	if !equalFoldASCII(tokens[0].clean, "DX") || !equalFoldASCII(tokens[1].clean, "DE") {
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
		return
	}

	parsed := spot.ParseSpotComment(buildComment(tokens, consumed), freq)
	mode := parsed.Mode
	if !spot.IsValidNormalizedCallsign(dxCall) || !spot.IsValidNormalizedCallsign(deCall) {
		return
	}

	comment := parsed.Comment
	if !c.minimalParse {
		comment = stripRBNSpeedCQComment(mode, comment)
	}
	report := parsed.Report
	hasReport := parsed.HasReport

	if !c.minimalParse && !hasReport {
		return
	}
	if !c.minimalParse && hasReport && report == 0 {
		return
	}

	if !c.minimalParse {
		freq = skew.ApplyCorrection(c.skewStore, deCallRaw, freq)
	}

	s := spot.NewSpotNormalized(dxCall, deCall, freq, mode)
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

	c.lastSpotAt.Store(time.Now().UTC().UnixNano())
	select {
	case c.spotChan <- s:
	default:
		c.spotDrops.Add(1)
		log.Printf("%s: Spot channel full (capacity=%d), dropping spot", c.displayName(), cap(c.spotChan))
	}
}

// GetSpotChannel exposes the output spot channel.
// Key aspects: Read-only channel for downstream consumers.
// Upstream: main.go pipeline wiring.
// Downstream: None.
func (c *Client) GetSpotChannel() <-chan *spot.Spot {
	return c.spotChan
}

// IsConnected reports whether the client is connected.
// Key aspects: Tracks connection state via a boolean flag.
// Upstream: Diagnostics/health checks.
// Downstream: None.
func (c *Client) IsConnected() bool {
	if c == nil {
		return false
	}
	return c.connected.Load()
}

// HealthSnapshot reports recent activity and queue state for diagnostics.
type HealthSnapshot struct {
	Connected    bool
	LastLineAt   time.Time
	LastSpotAt   time.Time
	SpotQueueLen int
	SpotQueueCap int
	SpotDrops    uint64
}

// HealthSnapshot provides a consistent ingest health snapshot.
// Key aspects: Uses atomics for timestamps and drop counts.
// Upstream: ingest health monitor.
// Downstream: None.
func (c *Client) HealthSnapshot() HealthSnapshot {
	if c == nil {
		return HealthSnapshot{}
	}
	snap := HealthSnapshot{
		Connected:    c.connected.Load(),
		SpotQueueLen: len(c.spotChan),
		SpotQueueCap: cap(c.spotChan),
		SpotDrops:    c.spotDrops.Load(),
	}
	if ns := c.lastLineAt.Load(); ns > 0 {
		snap.LastLineAt = time.Unix(0, ns)
	}
	if ns := c.lastSpotAt.Load(); ns > 0 {
		snap.LastSpotAt = time.Unix(0, ns)
	}
	return snap
}

// Stop stops the RBN client and closes connections.
// Key aspects: Signals shutdown once and closes the underlying conn.
// Upstream: main.go shutdown.
// Downstream: conn.Close, shutdown channel.
func (c *Client) Stop() {
	log.Printf("Stopping %s client...", c.displayName())
	c.stopOnce.Do(func() {
		close(c.shutdown)
	})
	if c.conn != nil {
		c.conn.Close()
	}
}

// Purpose: Report whether shutdown has been signaled.
// Key aspects: Non-blocking channel check.
// Upstream: readLoop, connectionSupervisor, requestReconnect.
// Downstream: None.
func (c *Client) isShutdown() bool {
	select {
	case <-c.shutdown:
		return true
	default:
		return false
	}
}

// Purpose: Signal the reconnect supervisor to re-dial.
// Key aspects: Non-blocking send; logs reason once.
// Upstream: readLoop error paths.
// Downstream: connectionSupervisor.
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

// Purpose: Return a human-friendly name for logging.
// Key aspects: Uses configured name or port-based defaults.
// Upstream: Logging in multiple client methods.
// Downstream: None.
func (c *Client) displayName() string {
	if c.name != "" {
		return c.name
	}
	if c.port == 7001 {
		return "RBN Digital"
	}
	return "RBN"
}

// Purpose: Send periodic CRLF keepalives to upstream telnet feed.
// Key aspects: Stops on shutdown or connection teardown.
// Upstream: establishConnection goroutine when keepalive enabled.
// Downstream: writer.WriteString/Flush.
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
			if err := c.writeStringLine("\r\n"); err != nil {
				if c.isShutdown() {
					return
				}
				log.Printf("%s: keepalive write failed: %v", c.displayName(), err)
				c.requestReconnect(err)
				return
			}
		}
	}
}

func (c *Client) writeStringLine(line string) error {
	if c == nil {
		return errors.New("nil RBN client")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.conn == nil || c.writer == nil {
		return errors.New("RBN connection not initialized")
	}
	if err := c.conn.SetWriteDeadline(time.Now().UTC().Add(rbnWriteDeadline)); err != nil {
		return err
	}
	defer func() {
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil && !c.isShutdown() {
			log.Printf("%s: failed to clear write deadline: %v", c.displayName(), err)
		}
	}()
	if _, err := c.writer.WriteString(line); err != nil {
		return err
	}
	return c.writer.Flush()
}
