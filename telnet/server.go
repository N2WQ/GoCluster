// Package telnet implements the multi-client telnet server for the DX Cluster.
//
// The telnet server is the primary user interface, handling:
//   - Client connections and authentication (callsign-based login)
//   - Real-time spot broadcasting to all connected clients
//   - Per-client filtering (band, mode, callsign patterns)
//   - User command processing (HELP, SHOW/DX, SHOW/STATION, BYE)
//   - Telnet protocol handling (IAC sequences, line ending conversion)
//
// Architecture:
//   - One goroutine per connected client (handleClient)
//   - Broadcast goroutine for distributing spots to all clients
//   - Non-blocking spot delivery (full channels don't block the system)
//   - Each client has their own Filter instance for personalized feeds
//
// Client Session Flow:
//  1. Client connects → Welcome message sent
//  2. Prompt for callsign → Client enters callsign
//  3. Login complete → Client receives greeting and help
//  4. Command loop → Process commands and broadcast spots
//  5. Client types BYE or disconnects → Session ends
//
// Concurrency Design:
//   - clientsMu protects the clients map (add/remove operations)
//   - Each client goroutine operates independently
//   - Broadcast uses non-blocking sends to avoid slow client blocking
//   - Graceful degradation: Full spot channels result in dropped spots for that client
//
// Maximum concurrent connections: Configurable (typically 500)
package telnet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dxcluster/commands"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/internal/ratelimit"
	"dxcluster/pathreliability"
	"dxcluster/reputation"
	"dxcluster/solarweather"
	"dxcluster/spot"
	"dxcluster/strutil"
	ztelnet "github.com/ziutek/telnet"
)

type dedupePolicy uint8

const (
	dedupePolicyFast dedupePolicy = iota
	dedupePolicyMed
	dedupePolicySlow
)

func parseDedupePolicy(value string) dedupePolicy {
	switch filter.NormalizeDedupePolicy(value) {
	case filter.DedupePolicyMed:
		return dedupePolicyMed
	case filter.DedupePolicySlow:
		return dedupePolicySlow
	default:
		return dedupePolicyFast
	}
}

func parseDedupePolicyToken(value string) (dedupePolicy, bool) {
	trimmed := strutil.NormalizeUpper(value)
	switch trimmed {
	case filter.DedupePolicyFast:
		return dedupePolicyFast, true
	case filter.DedupePolicyMed:
		return dedupePolicyMed, true
	case filter.DedupePolicySlow:
		return dedupePolicySlow, true
	default:
		return dedupePolicyFast, false
	}
}

func (p dedupePolicy) label() string {
	switch p {
	case dedupePolicyMed:
		return filter.DedupePolicyMed
	case dedupePolicySlow:
		return filter.DedupePolicySlow
	default:
		return filter.DedupePolicyFast
	}
}

func dedupeKeyLabel(policy dedupePolicy) string {
	if policy == dedupePolicySlow {
		return "cqzone"
	}
	return "grid2"
}

// Server represents a multi-client telnet server for DX Cluster connections.
//
// The server maintains a map of connected clients and broadcasts spots to all clients
// in real-time. Each client has its own goroutine for handling commands and receiving spots.
//
// Fields:
//   - port: TCP port to listen on (configured via `telnet.port` in `data/config/runtime.yaml`)
//   - welcomeMessage: Initial message sent to connecting clients
//   - maxConnections: Maximum concurrent client connections (typically 500)
//   - listener: TCP listener for accepting new connections
//   - clients: Map of callsign → Client for all connected clients
//   - clientsMutex: Read-write mutex protecting the clients map
//   - shutdown: Channel for coordinating graceful shutdown
//   - broadcast: Buffered channel for spot broadcasting (capacity 100)
//   - processor: Command processor for handling user commands
//
// Thread Safety:
//   - Start() and Stop() can be called from any goroutine
//   - BroadcastSpot() is thread-safe (uses mutex)
//   - Each client goroutine operates independently
type Server struct {
	port                 int                               // TCP port to listen on
	welcomeMessage       string                            // Welcome message for new connections
	maxConnections       int                               // Maximum concurrent client connections
	duplicateLoginMsg    string                            // Message sent to evicted duplicate session
	greetingTemplate     string                            // Post-login greeting with placeholders
	loginPrompt          string                            // Login prompt before callsign entry
	loginEmptyMessage    string                            // Message for empty callsign
	loginInvalidMsg      string                            // Message for invalid callsign
	inputTooLongMsg      string                            // Template for input length violations
	inputInvalidMsg      string                            // Template for invalid character violations
	dialectWelcomeMsg    string                            // Template for dialect welcome line
	dialectSourceDef     string                            // Label for default dialect source
	dialectSourcePers    string                            // Label for persisted dialect source
	pathStatusMsg        string                            // Template for path reliability status line
	clusterCall          string                            // Cluster/node callsign for greeting substitution
	listener             net.Listener                      // TCP listener
	clients              map[string]*Client                // Map of callsign → Client
	clientsMutex         sync.RWMutex                      // Protects clients map
	shutdown             chan struct{}                     // Shutdown coordination channel
	broadcast            chan *broadcastPayload            // Broadcast channel for spots (buffered, configurable)
	broadcastWorkers     int                               // Number of goroutines delivering spots
	workerQueues         []chan *broadcastJob              // Per-worker job queues
	workerQueueSize      int                               // Capacity of each worker's queue
	batchInterval        time.Duration                     // Broadcast batch interval; 0 means immediate
	batchMax             int                               // Max jobs per batch before flush
	metrics              broadcastMetrics                  // Broadcast metrics counters
	keepaliveInterval    time.Duration                     // Optional periodic CRLF to keep idle sessions alive
	clientShards         [][]*Client                       // Cached shard layout for broadcasts
	shardsDirty          atomic.Bool                       // Flag to rebuild shards on client add/remove
	processor            *commands.Processor               // Command processor for user commands
	skipHandshake        bool                              // When true, omit Telnet IAC negotiation
	transport            string                            // Telnet transport backend ("native" or "ziutek")
	useZiutek            bool                              // True when the external telnet transport is enabled
	echoMode             string                            // Input echo policy ("server", "local", "off")
	clientBufferSize     int                               // Per-client spot channel capacity
	controlQueueSize     int                               // Per-client control queue capacity
	readIdleTimeout      time.Duration                     // Read deadline for logged-in sessions (timeouts do not disconnect)
	loginTimeout         time.Duration                     // Pre-login timeout before disconnect
	dropExtremeRate      float64                           // Drop ratio threshold for disconnect
	dropExtremeWindow    time.Duration                     // Window for extreme drop evaluation
	dropExtremeMinAtt    int                               // Minimum attempts before extreme drop disconnect
	clientListListener   atomic.Value                      // optional func()
	latency              latencyMetrics                    // latency samples for delivery path
	loginLineLimit       int                               // Maximum bytes accepted for login/callsign input
	commandLineLimit     int                               // Maximum bytes accepted for post-login commands
	filterEngine         *filterCommandEngine              // Table-driven filter command parser/executor
	reputationGate       *reputation.Gate                  // Optional reputation gate for login metadata
	startTime            time.Time                         // Process start time for uptime tokens
	pathPredictor        *pathreliability.Predictor        // Optional path reliability predictor
	pathDisplay          bool                              // Toggle glyph rendering
	solarWeather         *solarweather.Manager             // Optional solar/geomagnetic override evaluator
	noiseOffsets         map[string]float64                // Noise class lookup
	gridLookup           func(string) (string, bool, bool) // Optional grid lookup from store
	dedupeFastEnabled    bool                              // Fast secondary dedupe policy enabled
	dedupeMedEnabled     bool                              // Med secondary dedupe policy enabled
	dedupeSlowEnabled    bool                              // Slow secondary dedupe policy enabled
	nearbyLoginWarning   string                            // Warning appended when NEARBY is active
	queueDropLog         ratelimit.Counter                 // Rate-limited log counter for broadcast queue drops
	workerDropLog        ratelimit.Counter                 // Rate-limited log counter for worker queue drops
	clientDropLog        ratelimit.Counter                 // Rate-limited log counter for per-client drops
	pathPredTotal        atomic.Uint64                     // Path predictions computed (glyphs)
	pathPredDerived      atomic.Uint64                     // Predictions using derived user/DX grids
	pathPredCombined     atomic.Uint64                     // Predictions with sufficient combined data
	pathPredInsufficient atomic.Uint64                     // Predictions with insufficient data
	pathPredNoSample     atomic.Uint64                     // Insufficient predictions with no samples
	pathPredLowWeight    atomic.Uint64                     // Insufficient predictions below min weight
	pathPredOverrideR    atomic.Uint64                     // R overrides applied
	pathPredOverrideG    atomic.Uint64                     // G overrides applied
}

// Client represents a connected telnet client session.
//
// Each client has:
//   - Dedicated goroutine for handling commands and receiving spots
//   - Personal Filter for customizing which spots they receive
//   - Buffered spot channel for non-blocking spot delivery
//   - Telnet protocol handling (IAC sequence processing)
//
// The client remains active until they type BYE or their connection drops.
type Client struct {
	conn           net.Conn               // TCP connection to client
	reader         *bufio.Reader          // Buffered reader for client input
	writer         *bufio.Writer          // Buffered writer for client output
	writeMu        sync.Mutex             // Guards writer/deadline usage for echo + writer loop
	callsign       string                 // Client's amateur radio callsign
	connected      time.Time              // Timestamp when client connected
	server         *Server                // Back-reference to server for formatting/helpers
	address        string                 // Client's IP address
	recentIPs      []string               // Most-recent-first IP history for this callsign
	spotChan       chan *spotEnvelope     // Buffered channel for spot delivery (configurable capacity)
	controlChan    chan controlMessage    // Buffered channel for control/bulletin delivery
	done           chan struct{}          // Closed to stop writer and prevent new enqueues
	closeOnce      sync.Once              // Ensures close logic runs once
	echoInput      bool                   // True when we should echo typed characters back to the client
	dialect        DialectName            // Active command dialect for filter commands
	grid           string                 // User grid (4+ chars) for path reliability
	gridDerived    bool                   // True when grid was derived from CTY prefix info
	gridCell       pathreliability.CellID // Cached cell for path reliability
	gridCoarseCell pathreliability.CellID // Cached coarse (res-1) cell for nearby filtering
	noiseClass     string                 // Noise class token (e.g., QUIET, URBAN)
	noisePenalty   float64                // dB penalty applied DX->user
	skipNextEOL    bool                   // Consume a single LF/NUL after CR (RFC 854 compliance)
	dedupePolicy   atomic.Uint32          // Secondary dedupe policy (fast/med/slow)
	diagEnabled    atomic.Bool            // Diagnostic comment override enabled
	// solarMu guards solar summary settings shared across goroutines.
	solarMu             sync.Mutex
	solarSummaryMinutes int
	solarNextSummaryAt  time.Time

	// filterMu guards filter, which is read by telnet broadcast workers while the
	// client session goroutine mutates it in response to PASS/REJECT commands.
	// Without this lock, Go's runtime can terminate the process with:
	// "fatal error: concurrent map read and map write"
	// because Filter contains many maps.
	filterMu sync.RWMutex
	filter   *filter.Filter // Personal spot filter (band, mode, callsign)
	// pathMu guards grid/noise settings shared across goroutines.
	pathMu     sync.RWMutex
	dropCount  uint64     // Count of spots dropped for this client due to backpressure
	dropWindow dropWindow // Sliding window for extreme drop detection
}

// InputValidationError represents a non-fatal ingress violation (length or character guardrails).
// Returning this error allows the caller to keep the connection open and prompt the user again.
type InputValidationError struct {
	reason  string
	context string
	kind    inputErrorKind
	maxLen  int
	allowed string
}

func (e *InputValidationError) Error() string {
	return e.reason
}

type inputErrorKind string

const (
	inputErrorTooLong     inputErrorKind = "too_long"
	inputErrorInvalidChar inputErrorKind = "invalid_char"
)

func (c *Client) saveFilter() error {
	if c == nil || c.filter == nil {
		return nil
	}
	callsign := strings.TrimSpace(c.callsign)
	if callsign == "" {
		return nil
	}
	state := c.pathSnapshot()
	// Persisting the filter marshals multiple maps; guard with a read lock so it
	// cannot run concurrently with PASS/REJECT updates. Broadcast workers also
	// hold read locks while matching, so persistence does not stall spot delivery.
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	record := &filter.UserRecord{
		Filter:       *c.filter,
		RecentIPs:    c.recentIPs,
		Dialect:      string(c.dialect),
		DedupePolicy: c.getDedupePolicy().label(),
		Grid:         strings.ToUpper(strings.TrimSpace(state.grid)),
		NoiseClass:   strings.ToUpper(strings.TrimSpace(state.noiseClass)),
	}
	record.SolarSummaryMinutes = c.getSolarSummaryMinutes()
	if existing, err := filter.LoadUserRecord(callsign); err == nil {
		record.RecentIPs = filter.MergeRecentIPs(record.RecentIPs, existing.RecentIPs)
	}
	if err := filter.SaveUserRecord(callsign, record); err != nil {
		log.Printf("Warning: failed to save user record for %s: %v", callsign, err)
		return err
	}
	log.Printf("Saved user record for %s", callsign)
	return nil
}

func (c *Client) setDedupePolicy(policy dedupePolicy) {
	if c == nil {
		return
	}
	c.dedupePolicy.Store(uint32(policy))
}

func (c *Client) getDedupePolicy() dedupePolicy {
	if c == nil {
		return dedupePolicyFast
	}
	return dedupePolicy(c.dedupePolicy.Load())
}

func (c *Client) getSolarSummaryMinutes() int {
	if c == nil {
		return 0
	}
	c.solarMu.Lock()
	defer c.solarMu.Unlock()
	return c.solarSummaryMinutes
}

func (c *Client) setSolarSummaryMinutes(minutes int, now time.Time) {
	if c == nil {
		return
	}
	c.solarMu.Lock()
	defer c.solarMu.Unlock()
	if minutes <= 0 {
		c.solarSummaryMinutes = 0
		c.solarNextSummaryAt = time.Time{}
		return
	}
	c.solarSummaryMinutes = minutes
	c.solarNextSummaryAt = nextSolarSummaryAt(now, minutes)
}

func (c *Client) nextSolarSummaryAt(now time.Time) (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	c.solarMu.Lock()
	defer c.solarMu.Unlock()
	if c.solarSummaryMinutes <= 0 {
		return time.Time{}, false
	}
	if c.solarNextSummaryAt.IsZero() {
		c.solarNextSummaryAt = nextSolarSummaryAt(now, c.solarSummaryMinutes)
	}
	return c.solarNextSummaryAt, true
}

func (c *Client) advanceSolarSummaryAt(now time.Time) {
	if c == nil {
		return
	}
	c.solarMu.Lock()
	defer c.solarMu.Unlock()
	if c.solarSummaryMinutes <= 0 {
		c.solarNextSummaryAt = time.Time{}
		return
	}
	c.solarNextSummaryAt = nextSolarSummaryAt(now, c.solarSummaryMinutes)
}

// updateFilter applies a mutation to the per-client Filter while holding the
// write lock, protecting against concurrent reads from broadcast workers.
func (c *Client) updateFilter(fn func(f *filter.Filter)) {
	if c == nil || c.filter == nil || fn == nil {
		return
	}
	c.filterMu.Lock()
	fn(c.filter)
	c.filterMu.Unlock()
}

type pathState struct {
	grid           string
	gridDerived    bool
	gridCell       pathreliability.CellID
	gridCoarseCell pathreliability.CellID
	noiseClass     string
	noisePenalty   float64
}

func (c *Client) pathSnapshot() pathState {
	if c == nil {
		return pathState{}
	}
	c.pathMu.RLock()
	state := pathState{
		grid:           c.grid,
		gridDerived:    c.gridDerived,
		gridCell:       c.gridCell,
		gridCoarseCell: c.gridCoarseCell,
		noiseClass:     c.noiseClass,
		noisePenalty:   c.noisePenalty,
	}
	c.pathMu.RUnlock()
	return state
}

type broadcastPayload struct {
	spot      *spot.Spot
	allowFast bool
	allowMed  bool
	allowSlow bool
	enqueueAt time.Time
}

type broadcastJob struct {
	spot      *spot.Spot
	allowFast bool
	allowMed  bool
	allowSlow bool
	clients   []*Client
	enqueueAt time.Time
}

type spotEnvelope struct {
	spot      *spot.Spot
	enqueueAt time.Time
}

type controlMessage struct {
	line       string
	raw        []byte
	closeAfter bool
}

type dropBucket struct {
	attempts uint64
	drops    uint64
}

// dropWindow tracks spot enqueue attempts and drops over a sliding window.
// It is guarded by a mutex because updates can arrive from multiple workers.
type dropWindow struct {
	mu          sync.Mutex
	window      time.Duration
	bucketWidth time.Duration
	start       time.Time
	startIdx    int
	buckets     []dropBucket
}

type broadcastMetrics struct {
	queueDrops     uint64
	clientDrops    uint64
	senderFailures uint64
}

func (m *broadcastMetrics) snapshot() (queueDrops, clientDrops, senderFailures uint64) {
	queueDrops = atomic.LoadUint64(&m.queueDrops)
	clientDrops = atomic.LoadUint64(&m.clientDrops)
	senderFailures = atomic.LoadUint64(&m.senderFailures)
	return
}

func (s *Server) recordSenderFailure() uint64 {
	if s == nil {
		return 0
	}
	return atomic.AddUint64(&s.metrics.senderFailures, 1)
}

const dropWindowBuckets = 6

func newDropWindow(window time.Duration) dropWindow {
	if window <= 0 {
		return dropWindow{}
	}
	width := window / time.Duration(dropWindowBuckets)
	if width <= 0 {
		width = window
	}
	return dropWindow{
		window:      window,
		bucketWidth: width,
		buckets:     make([]dropBucket, dropWindowBuckets),
	}
}

func (w *dropWindow) advance(now time.Time) {
	if w == nil || w.bucketWidth <= 0 || len(w.buckets) == 0 {
		return
	}
	if w.start.IsZero() {
		w.start = now.Truncate(w.bucketWidth)
		w.startIdx = 0
		return
	}
	if now.Before(w.start) {
		for i := range w.buckets {
			w.buckets[i] = dropBucket{}
		}
		w.start = now.Truncate(w.bucketWidth)
		w.startIdx = 0
		return
	}
	offset := int(now.Sub(w.start) / w.bucketWidth)
	if offset < len(w.buckets) {
		return
	}
	shift := offset - len(w.buckets) + 1
	if shift >= len(w.buckets) {
		for i := range w.buckets {
			w.buckets[i] = dropBucket{}
		}
		w.start = now.Truncate(w.bucketWidth)
		w.startIdx = 0
		return
	}
	for i := 0; i < shift; i++ {
		w.start = w.start.Add(w.bucketWidth)
		w.startIdx = (w.startIdx + 1) % len(w.buckets)
		w.buckets[w.startIdx] = dropBucket{}
	}
}

func (w *dropWindow) record(now time.Time, dropped bool) (uint64, uint64) {
	if w == nil || w.window <= 0 || w.bucketWidth <= 0 {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(now)
	offset := int(now.Sub(w.start) / w.bucketWidth)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(w.buckets) {
		offset = len(w.buckets) - 1
	}
	idx := (w.startIdx + offset) % len(w.buckets)
	w.buckets[idx].attempts++
	if dropped {
		w.buckets[idx].drops++
	}
	var attempts uint64
	var drops uint64
	for i := range w.buckets {
		attempts += w.buckets[i].attempts
		drops += w.buckets[i].drops
	}
	return attempts, drops
}

// handleDialectCommand lets a client select a filter command dialect explicitly.
func (s *Server) handleDialectCommand(client *Client, line string) (string, bool) {
	if client == nil || s == nil || s.filterEngine == nil {
		return "", false
	}
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) == 0 {
		return "", false
	}
	if !strings.EqualFold(fields[0], "DIALECT") {
		return "", false
	}
	if len(fields) == 1 {
		return fmt.Sprintf("Current dialect: %s\n", strings.ToUpper(string(client.dialect))), true
	}
	if len(fields) == 2 && strings.EqualFold(fields[1], "LIST") {
		names := s.filterEngine.availableDialectNames()
		return fmt.Sprintf("Available dialects: %s\n", strings.ToUpper(strings.Join(names, ", "))), true
	}
	nameToken := strings.ToLower(strings.TrimSpace(fields[1]))
	selected, ok := s.filterEngine.dialectAliases[nameToken]
	if !ok {
		// Also accept exact dialect names.
		if dialect := DialectName(nameToken); s.filterEngine.dialects[dialect] != nil {
			selected = dialect
			ok = true
		}
	}
	if !ok {
		return fmt.Sprintf("Unknown dialect: %s\nAvailable dialects: %s\n", fields[1], strings.ToUpper(strings.Join(s.filterEngine.availableDialectNames(), ", "))), true
	}
	if client.dialect == selected {
		return fmt.Sprintf("Dialect already set to %s\n", strings.ToUpper(string(selected))), true
	}
	client.dialect = selected
	// Persist updated dialect selection.
	_ = client.saveFilter()
	return fmt.Sprintf("Dialect set to %s\n", strings.ToUpper(string(selected))), true
}

type dialectTemplateData struct {
	dialect        string
	source         string
	defaultDialect string
}

func formatDialectWelcome(template string, data dialectTemplateData) string {
	if strings.TrimSpace(template) == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"<DIALECT>", data.dialect,
		"<DIALECT_SOURCE>", data.source,
		"<DIALECT_DEFAULT>", data.defaultDialect,
	)
	return replacer.Replace(template)
}

func (s *Server) dialectSourceLabel(active DialectName, created bool, loadErr error, defaultDialect DialectName) string {
	source := s.dialectSourceDef
	if loadErr == nil && !created {
		source = s.dialectSourcePers
	}
	if active != defaultDialect {
		source = s.dialectSourcePers
	}
	return source
}

func displayGrid(grid string, derived bool) string {
	grid = strings.TrimSpace(grid)
	if grid == "" {
		return ""
	}
	if derived {
		return strings.ToLower(grid)
	}
	return strings.ToUpper(grid)
}

func (s *Server) formatPathStatusMessage(client *Client) string {
	if s == nil || client == nil {
		return ""
	}
	template := s.pathStatusMsg
	if strings.TrimSpace(template) == "" {
		return ""
	}
	state := client.pathSnapshot()
	grid := displayGrid(state.grid, state.gridDerived)
	noise := strings.TrimSpace(state.noiseClass)
	replacer := strings.NewReplacer(
		"<GRID>", grid,
		"<NOISE>", noise,
	)
	return replacer.Replace(template)
}

// handlePathSettingsCommand processes SET GRID/SET NOISE commands.
func (s *Server) handlePathSettingsCommand(client *Client, line string) (string, bool) {
	if client == nil {
		return "", false
	}
	upper := strings.Fields(strings.ToUpper(strings.TrimSpace(line)))
	if len(upper) < 2 || upper[0] != "SET" {
		return "", false
	}
	switch upper[1] {
	case "GRID":
		if len(upper) < 3 {
			return "Usage: SET GRID <4-6 char maidenhead>\n", true
		}
		grid := strings.ToUpper(strings.TrimSpace(upper[2]))
		if len(grid) < 4 {
			return "Grid must be at least 4 characters (e.g., FN31)\n", true
		}
		cell := pathreliability.EncodeCell(grid)
		if cell == pathreliability.InvalidCell {
			return "Invalid grid. Please provide a 4-6 character Maidenhead locator.\n", true
		}
		coarseCell := pathreliability.EncodeCoarseCell(grid)
		client.pathMu.Lock()
		client.grid = grid
		client.gridDerived = false
		client.gridCell = cell
		client.gridCoarseCell = coarseCell
		client.pathMu.Unlock()
		client.updateFilter(func(f *filter.Filter) {
			if f.NearbyActive() {
				f.UpdateNearbyUserCells(cell, coarseCell)
			}
		})
		if err := client.saveFilter(); err != nil {
			return fmt.Sprintf("Grid set to %s (warning: failed to persist: %v)\n", grid, err), true
		}
		return fmt.Sprintf("Grid set to %s\n", grid), true
	case "NOISE":
		if len(upper) < 3 {
			return "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>\n", true
		}
		class := strings.ToUpper(strings.TrimSpace(upper[2]))
		penalty := s.noisePenaltyForClass(class)
		if class == "" || (penalty == 0 && class != "QUIET" && class != "RURAL" && class != "SUBURBAN" && class != "URBAN" && class != "INDUSTRIAL") {
			return "Unknown noise class. Use QUIET, RURAL, SUBURBAN, URBAN, or INDUSTRIAL.\n", true
		}
		client.pathMu.Lock()
		client.noiseClass = class
		client.noisePenalty = penalty
		client.pathMu.Unlock()
		if err := client.saveFilter(); err != nil {
			return fmt.Sprintf("Noise class set to %s (warning: failed to persist: %v)\n", class, err), true
		}
		return fmt.Sprintf("Noise class set to %s\n", class), true
	default:
		return "", false
	}
}

func (s *Server) resolveDedupePolicy(requested dedupePolicy) dedupePolicy {
	if s == nil {
		return requested
	}
	switch requested {
	case dedupePolicyFast:
		if s.dedupeFastEnabled {
			return dedupePolicyFast
		}
	case dedupePolicyMed:
		if s.dedupeMedEnabled {
			return dedupePolicyMed
		}
	case dedupePolicySlow:
		if s.dedupeSlowEnabled {
			return dedupePolicySlow
		}
	}
	if s.dedupeFastEnabled {
		return dedupePolicyFast
	}
	if s.dedupeMedEnabled {
		return dedupePolicyMed
	}
	if s.dedupeSlowEnabled {
		return dedupePolicySlow
	}
	return dedupePolicyFast
}

func (s *Server) formatDedupeStatus(client *Client) string {
	if client == nil {
		return ""
	}
	policy := client.getDedupePolicy().label()
	keyLabel := dedupeKeyLabel(client.getDedupePolicy())
	if !s.dedupeFastEnabled && !s.dedupeMedEnabled && !s.dedupeSlowEnabled {
		return fmt.Sprintf("Dedupe: %s (%s) (secondary disabled)\n", policy, keyLabel)
	}
	fastLabel := "off"
	if s.dedupeFastEnabled {
		fastLabel = "on"
	}
	medLabel := "off"
	if s.dedupeMedEnabled {
		medLabel = "on"
	}
	slowLabel := "off"
	if s.dedupeSlowEnabled {
		slowLabel = "on"
	}
	return fmt.Sprintf("Dedupe: %s (%s) (fast=%s med=%s slow=%s)\n", policy, keyLabel, fastLabel, medLabel, slowLabel)
}

func (s *Server) handleDiagCommand(client *Client, line string) (string, bool) {
	if client == nil {
		return "", false
	}
	upper := strings.Fields(strings.ToUpper(strings.TrimSpace(line)))
	if len(upper) < 2 || upper[0] != "SET" || upper[1] != "DIAG" {
		return "", false
	}
	if len(upper) < 3 {
		return "Usage: SET DIAG <ON|OFF>\n", true
	}
	switch upper[2] {
	case "ON":
		client.diagEnabled.Store(true)
		return "Diagnostic comments: ON\n", true
	case "OFF":
		client.diagEnabled.Store(false)
		return "Diagnostic comments: OFF\n", true
	default:
		return "Usage: SET DIAG <ON|OFF>\n", true
	}
}

// handleSolarCommand processes SET SOLAR commands.
func (s *Server) handleSolarCommand(client *Client, line string) (string, bool) {
	if client == nil {
		return "", false
	}
	upper := strings.Fields(strings.ToUpper(strings.TrimSpace(line)))
	if len(upper) < 2 || upper[0] != "SET" || upper[1] != "SOLAR" {
		return "", false
	}
	if len(upper) < 3 {
		return "Usage: SET SOLAR <15|30|60|OFF>\n", true
	}
	minutes, ok := parseSolarSummaryMinutes(upper[2])
	if !ok {
		return "Usage: SET SOLAR <15|30|60|OFF>\n", true
	}
	now := time.Now().UTC()
	client.setSolarSummaryMinutes(minutes, now)
	if err := client.saveFilter(); err != nil {
		if minutes == 0 {
			return fmt.Sprintf("Solar summaries: OFF (warning: failed to persist: %v)\n", err), true
		}
		return fmt.Sprintf("Solar summaries: every %d minutes (warning: failed to persist: %v)\n", minutes, err), true
	}
	if minutes == 0 {
		return "Solar summaries: OFF\n", true
	}
	return fmt.Sprintf("Solar summaries: every %d minutes\n", minutes), true
}

// handleDedupeCommand processes SET/SHOW DEDUPE commands.
func (s *Server) handleDedupeCommand(client *Client, line string) (string, bool) {
	if client == nil || s == nil {
		return "", false
	}
	upper := strings.Fields(strings.ToUpper(strings.TrimSpace(line)))
	if len(upper) < 2 {
		return "", false
	}
	switch upper[0] {
	case "SHOW":
		if upper[1] != "DEDUPE" {
			return "", false
		}
		return s.formatDedupeStatus(client), true
	case "SET":
		if upper[1] != "DEDUPE" {
			return "", false
		}
		if len(upper) < 3 {
			return "Usage: SET DEDUPE <FAST|MED|SLOW>\n", true
		}
		requested, ok := parseDedupePolicyToken(upper[2])
		if !ok {
			return "Usage: SET DEDUPE <FAST|MED|SLOW>\n", true
		}
		effective := s.resolveDedupePolicy(requested)
		client.setDedupePolicy(effective)
		saveErr := client.saveFilter()

		if !s.dedupeFastEnabled && !s.dedupeMedEnabled && !s.dedupeSlowEnabled {
			if saveErr != nil {
				return fmt.Sprintf("Dedupe policy set to %s (secondary disabled; warning: failed to persist: %v)\n", effective.label(), saveErr), true
			}
			return fmt.Sprintf("Dedupe policy set to %s (secondary disabled)\n", effective.label()), true
		}
		if requested != effective {
			if saveErr != nil {
				return fmt.Sprintf("Dedupe policy set to %s (requested %s disabled; warning: failed to persist: %v)\n", effective.label(), requested.label(), saveErr), true
			}
			return fmt.Sprintf("Dedupe policy set to %s (requested %s disabled)\n", effective.label(), requested.label()), true
		}
		if saveErr != nil {
			return fmt.Sprintf("Dedupe policy set to %s (warning: failed to persist: %v)\n", effective.label(), saveErr), true
		}
		return fmt.Sprintf("Dedupe policy set to %s\n", effective.label()), true
	default:
		return "", false
	}
}

func parseSolarSummaryMinutes(token string) (int, bool) {
	switch strutil.NormalizeUpper(token) {
	case "OFF":
		return 0, true
	case "15":
		return 15, true
	case "30":
		return 30, true
	case "60":
		return 60, true
	default:
		return 0, false
	}
}

func nextSolarSummaryAt(now time.Time, minutes int) time.Time {
	if minutes <= 0 {
		return time.Time{}
	}
	now = now.UTC()
	minute := now.Minute()
	alignedMinute := (minute / minutes) * minutes
	aligned := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), alignedMinute, 0, 0, time.UTC)
	return aligned.Add(time.Duration(minutes) * time.Minute)
}

const (
	// IAC starts a telnet command sequence (Interpret As Command).
	IAC = 255
	// DONT requests the client to disable a telnet option.
	DONT = 254
	// DO requests the client to enable a telnet option.
	DO = 253
	// WONT indicates refusal to enable a telnet option.
	WONT = 252
	// WILL indicates agreement to enable a telnet option.
	WILL = 251
	// SB starts a telnet subnegotiation block.
	SB = 250
	// SE ends a telnet subnegotiation block.
	SE = 240
)

const (
	telnetEchoServer = "server"
	telnetEchoLocal  = "local"
	telnetEchoOff    = "off"
)

const (
	defaultBroadcastQueueSize     = 2048
	defaultBroadcastBatch         = 512
	defaultBroadcastBatchInterval = 250 * time.Millisecond
	defaultClientBufferSize       = 128
	defaultControlQueueSize       = 32
	defaultWorkerQueueSize        = 128
	defaultSendDeadline           = 2 * time.Second
	defaultLoginLineLimit         = 32
	defaultCommandLineLimit       = 128
	defaultReadIdleTimeout        = 24 * time.Hour
	defaultLoginTimeout           = 2 * time.Minute
	defaultDropExtremeRate        = 0.80
	defaultDropExtremeWindow      = 30 * time.Second
	defaultDropExtremeMinAttempts = 100
	defaultDropLogInterval        = 10 * time.Second
)

const (
	passFilterUsageMsg      = "Usage: PASS <type> ...\nPASS BAND <band>[,<band>...] | PASS MODE <mode>[,<mode>...] | PASS SOURCE <HUMAN|SKIMMER|ALL> | PASS DXCALL <pattern>[,<pattern>...] | PASS DECALL <pattern>[,<pattern>...] | PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | PASS PATH <class>[,<class>...] (classes: HIGH,MEDIUM,LOW,UNLIKELY,INSUFFICIENT or ALL) | PASS BEACON | PASS WWV | PASS WCY | PASS ANNOUNCE | PASS NEARBY ON|OFF | PASS DXGRID2 <grid>[,<grid>...] (two characters or ALL) | PASS DEGRID2 <grid>[,<grid>...] (two characters or ALL) | PASS DXCONT <cont>[,<cont>...] | PASS DECONT <cont>[,<cont>...] | PASS DXZONE <zone>[,<zone>...] | PASS DEZONE <zone>[,<zone>...] | PASS DXDXCC <code>[,<code>...] | PASS DEDXCC <code>[,<code>...] (PASS = allow list; removes from block list; ALL allows all). Supported modes include: CW, LSB, USB, JS8, SSTV, RTTY, FT4, FT8, MSK144, PSK.\nType HELP for usage.\n"
	rejectFilterUsageMsg    = "Usage: REJECT <type> ...\nREJECT BAND <band>[,<band>...] | REJECT MODE <mode>[,<mode>...] | REJECT SOURCE <HUMAN|SKIMMER|ALL> | REJECT DXCALL <pattern>[,<pattern>...] | REJECT DECALL <pattern>[,<pattern>...] | REJECT CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | REJECT PATH <class>[,<class>...] (classes: HIGH,MEDIUM,LOW,UNLIKELY,INSUFFICIENT or ALL) | REJECT BEACON | REJECT WWV | REJECT WCY | REJECT ANNOUNCE | REJECT DXGRID2 <grid>[,<grid>...] (two characters or ALL) | REJECT DEGRID2 <grid>[,<grid>...] (two characters or ALL) | REJECT DXCONT <cont>[,<cont>...] | REJECT DECONT <cont>[,<cont>...] | REJECT DXZONE <zone>[,<zone>...] | REJECT DEZONE <zone>[,<zone>...] | REJECT DXDXCC <code>[,<code>...] | REJECT DEDXCC <code>[,<code>...] (REJECT = block list; removes from allow list; ALL blocks all). Supported modes include: CW, LSB, USB, JS8, SSTV, RTTY, FT4, FT8, MSK144, PSK.\nType HELP for usage.\n"
	unknownPassTypeMsg      = "Unknown filter type. Use: BAND, MODE, SOURCE, DXCALL, DECALL, CONFIDENCE, PATH, BEACON, WWV, WCY, ANNOUNCE, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE, DXDXCC, or DEDXCC\nType HELP for usage.\n"
	unknownRejectTypeMsg    = "Unknown filter type. Use: BAND, MODE, SOURCE, DXCALL, DECALL, CONFIDENCE, PATH, BEACON, WWV, WCY, ANNOUNCE, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE, DXDXCC, or DEDXCC\nType HELP for usage.\n"
	invalidFilterCommandMsg = "Invalid filter command. Type HELP for usage.\n"
	nearbyLoginWarningMsg   = "NEARBY filter is ON. Disable NEARBY if you want to use regular location filters"
	nearbyLoginInactiveMsg  = "NEARBY filter is ON but inactive: grid not set or H3 tables unavailable.\n"
)

// ServerOptions configures the telnet server instance.
type ServerOptions struct {
	Port                    int
	WelcomeMessage          string
	DuplicateLoginMsg       string
	LoginGreeting           string
	LoginPrompt             string
	LoginEmptyMessage       string
	LoginInvalidMessage     string
	InputTooLongMessage     string
	InputInvalidCharMessage string
	DialectWelcomeMessage   string
	DialectSourceDefault    string
	DialectSourcePersisted  string
	PathStatusMessage       string
	ClusterCall             string
	MaxConnections          int
	BroadcastWorkers        int
	BroadcastQueue          int
	WorkerQueue             int
	ClientBuffer            int
	ControlQueue            int
	BroadcastBatchInterval  time.Duration
	KeepaliveSeconds        int
	SkipHandshake           bool
	Transport               string
	EchoMode                string
	ReadIdleTimeout         time.Duration
	LoginTimeout            time.Duration
	LoginLineLimit          int
	CommandLineLimit        int
	DropExtremeRate         float64
	DropExtremeWindow       time.Duration
	DropExtremeMinAttempts  int
	ReputationGate          *reputation.Gate
	PathPredictor           *pathreliability.Predictor
	PathDisplayEnabled      bool
	SolarWeather            *solarweather.Manager
	NoiseOffsets            map[string]float64
	GridLookup              func(string) (string, bool, bool)
	CTYLookup               func() *cty.CTYDatabase
	DedupeFastEnabled       bool
	DedupeMedEnabled        bool
	DedupeSlowEnabled       bool
	NearbyLoginWarning      string
}

// NewServer creates a new telnet server
func NewServer(opts ServerOptions, processor *commands.Processor) *Server {
	config := normalizeServerOptions(opts)
	useZiutek := strings.EqualFold(config.Transport, "ziutek")
	return &Server{
		port:               config.Port,
		welcomeMessage:     config.WelcomeMessage,
		maxConnections:     config.MaxConnections,
		duplicateLoginMsg:  config.DuplicateLoginMsg,
		greetingTemplate:   config.LoginGreeting,
		loginPrompt:        config.LoginPrompt,
		loginEmptyMessage:  config.LoginEmptyMessage,
		loginInvalidMsg:    config.LoginInvalidMessage,
		inputTooLongMsg:    config.InputTooLongMessage,
		inputInvalidMsg:    config.InputInvalidCharMessage,
		dialectWelcomeMsg:  config.DialectWelcomeMessage,
		dialectSourceDef:   config.DialectSourceDefault,
		dialectSourcePers:  config.DialectSourcePersisted,
		pathStatusMsg:      config.PathStatusMessage,
		clusterCall:        config.ClusterCall,
		clients:            make(map[string]*Client),
		shutdown:           make(chan struct{}),
		broadcast:          make(chan *broadcastPayload, config.BroadcastQueue),
		broadcastWorkers:   config.BroadcastWorkers,
		workerQueueSize:    config.WorkerQueue,
		batchInterval:      config.BroadcastBatchInterval,
		batchMax:           defaultBroadcastBatch,
		keepaliveInterval:  time.Duration(config.KeepaliveSeconds) * time.Second,
		clientBufferSize:   config.ClientBuffer,
		controlQueueSize:   config.ControlQueue,
		skipHandshake:      config.SkipHandshake,
		transport:          config.Transport,
		useZiutek:          useZiutek,
		echoMode:           config.EchoMode,
		processor:          processor,
		readIdleTimeout:    config.ReadIdleTimeout,
		loginTimeout:       config.LoginTimeout,
		loginLineLimit:     config.LoginLineLimit,
		commandLineLimit:   config.CommandLineLimit,
		filterEngine:       newFilterCommandEngineWithCTY(config.CTYLookup),
		latency:            newLatencyMetrics(),
		reputationGate:     opts.ReputationGate,
		startTime:          time.Now().UTC(),
		pathPredictor:      opts.PathPredictor,
		pathDisplay:        opts.PathDisplayEnabled,
		solarWeather:       opts.SolarWeather,
		noiseOffsets:       opts.NoiseOffsets,
		gridLookup:         opts.GridLookup,
		dedupeFastEnabled:  config.DedupeFastEnabled,
		dedupeMedEnabled:   config.DedupeMedEnabled,
		dedupeSlowEnabled:  config.DedupeSlowEnabled,
		nearbyLoginWarning: normalizeWarningLine(config.NearbyLoginWarning),
		dropExtremeRate:    config.DropExtremeRate,
		dropExtremeWindow:  config.DropExtremeWindow,
		dropExtremeMinAtt:  config.DropExtremeMinAttempts,
		queueDropLog:       ratelimit.NewCounter(defaultDropLogInterval),
		workerDropLog:      ratelimit.NewCounter(defaultDropLogInterval),
		clientDropLog:      ratelimit.NewCounter(defaultDropLogInterval),
	}
}

func normalizeServerOptions(opts ServerOptions) ServerOptions {
	config := opts
	if config.BroadcastWorkers <= 0 {
		config.BroadcastWorkers = defaultBroadcastWorkers()
	}
	if config.BroadcastQueue <= 0 {
		config.BroadcastQueue = defaultBroadcastQueueSize
	}
	if config.WorkerQueue <= 0 {
		config.WorkerQueue = defaultWorkerQueueSize
	}
	if config.ClientBuffer <= 0 {
		config.ClientBuffer = defaultClientBufferSize
	}
	if config.ControlQueue <= 0 {
		config.ControlQueue = defaultControlQueueSize
	}
	if config.BroadcastBatchInterval <= 0 {
		config.BroadcastBatchInterval = defaultBroadcastBatchInterval
	}
	if strings.TrimSpace(config.ClusterCall) == "" {
		config.ClusterCall = "DXC"
	}
	if strings.TrimSpace(config.Transport) == "" {
		config.Transport = "native"
	}
	config.Transport = strings.ToLower(strings.TrimSpace(config.Transport))
	if strings.TrimSpace(config.EchoMode) == "" {
		config.EchoMode = "server"
	}
	config.EchoMode = strings.ToLower(strings.TrimSpace(config.EchoMode))
	if config.LoginLineLimit <= 0 {
		config.LoginLineLimit = defaultLoginLineLimit
	}
	if config.CommandLineLimit <= 0 {
		config.CommandLineLimit = defaultCommandLineLimit
	}
	if config.ReadIdleTimeout <= 0 {
		config.ReadIdleTimeout = defaultReadIdleTimeout
	}
	if config.LoginTimeout <= 0 {
		config.LoginTimeout = defaultLoginTimeout
	}
	if config.DropExtremeRate <= 0 {
		config.DropExtremeRate = defaultDropExtremeRate
	}
	if config.DropExtremeRate > 1 {
		config.DropExtremeRate = defaultDropExtremeRate
	}
	if config.DropExtremeWindow <= 0 {
		config.DropExtremeWindow = defaultDropExtremeWindow
	}
	if config.DropExtremeMinAttempts <= 0 {
		config.DropExtremeMinAttempts = defaultDropExtremeMinAttempts
	}
	if config.PathPredictor == nil {
		config.PathDisplayEnabled = false
	}
	if config.NoiseOffsets == nil {
		config.NoiseOffsets = map[string]float64{}
	}
	if strings.TrimSpace(config.NearbyLoginWarning) == "" {
		config.NearbyLoginWarning = nearbyLoginWarningMsg
	}
	return config
}

// Start begins listening for telnet connections
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := listenWithReuse(addr)
	if err != nil {
		return fmt.Errorf("failed to start telnet server: %w", err)
	}

	s.listener = listener
	log.Printf("Telnet server listening on port %d", s.port)

	// Prepare worker pool before handling spots
	s.startWorkerPool()

	// Start broadcast handler
	go s.handleBroadcasts()

	// Optional keepalive emitter for idle sessions.
	if s.keepaliveInterval > 0 {
		go s.keepaliveLoop()
	}
	if s.solarWeather != nil {
		go s.solarSummaryLoop()
	}

	// Accept connections in a goroutine
	go s.acceptConnections()

	return nil
}

// listenWithReuse enables SO_REUSEADDR so we can rebind quickly after a crash/exit.
// It falls back to a standard Listen when the control call fails.
func listenWithReuse(addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var sockErr error
			controlErr := c.Control(func(fd uintptr) {
				sockErr = setReuseAddr(fd)
			})
			if controlErr != nil {
				return controlErr
			}
			return sockErr
		},
	}
	listener, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		// Fallback to default listener to avoid failing on platforms that reject the control call.
		return net.Listen("tcp", addr)
	}
	return listener, nil
}

// handleBroadcasts sends spots to all connected clients
func (s *Server) handleBroadcasts() {
	for {
		select {
		case <-s.shutdown:
			return
		case payload := <-s.broadcast:
			s.broadcastSpot(payload)
		}
	}
}

// keepaliveLoop emits periodic CRLF to all connected clients to prevent idle
// disconnects by intermediate network devices when the spot stream is quiet.
func (s *Server) keepaliveLoop() {
	ticker := time.NewTicker(s.keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
			s.clientsMutex.RLock()
			for _, client := range s.clients {
				_ = client.Send("\r\n")
			}
			s.clientsMutex.RUnlock()
		}
	}
}

// solarSummaryLoop emits wall-clock aligned solar summaries to opted-in clients.
func (s *Server) solarSummaryLoop() {
	nextTick := nextMinuteBoundary(time.Now().UTC())
	timer := time.NewTimer(time.Until(nextTick))
	defer timer.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case <-timer.C:
			now := time.Now().UTC().Truncate(time.Minute)
			s.emitSolarSummaries(now)
			nextTick = nextMinuteBoundary(now)
			timer.Reset(time.Until(nextTick))
		}
	}
}

func nextMinuteBoundary(now time.Time) time.Time {
	now = now.UTC()
	return now.Truncate(time.Minute).Add(time.Minute)
}

func (s *Server) emitSolarSummaries(now time.Time) {
	if s == nil || s.solarWeather == nil {
		return
	}
	message := s.solarWeather.Summary(now)
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	for _, client := range s.clients {
		nextAt, ok := client.nextSolarSummaryAt(now)
		if !ok || now.Before(nextAt) {
			continue
		}
		if message != "" {
			s.enqueueBulletin(client, "SOLAR", message)
		}
		client.advanceSolarSummaryAt(now)
	}
}

// BroadcastSpot sends a spot to all connected clients with per-policy gates.
func (s *Server) BroadcastSpot(spot *spot.Spot, allowFast, allowMed, allowSlow bool) {
	if s == nil || spot == nil {
		return
	}
	payload := &broadcastPayload{
		spot:      spot,
		allowFast: allowFast,
		allowMed:  allowMed,
		allowSlow: allowSlow,
		enqueueAt: time.Now().UTC(),
	}
	select {
	case s.broadcast <- payload:
	default:
		drops := atomic.AddUint64(&s.metrics.queueDrops, 1)
		if _, ok := s.queueDropLog.Inc(); ok {
			log.Printf("Broadcast channel full (%d/%d buffered), dropping spot (total queue drops=%d)", len(s.broadcast), cap(s.broadcast), drops)
		}
	}
}

// DeliverSelfSpot sends a spot only to the matching callsign client when SELF
// delivery is enabled, even if the broadcast path suppresses the spot.
// It never fans out to other clients.
func (s *Server) DeliverSelfSpot(spot *spot.Spot) {
	if s == nil || spot == nil {
		return
	}
	dxCall := normalizedDXCall(spot)
	if dxCall == "" {
		return
	}

	s.clientsMutex.RLock()
	client := s.clients[dxCall]
	s.clientsMutex.RUnlock()
	if client == nil {
		return
	}

	client.filterMu.RLock()
	allowSelf := client.filter.SelfEnabled()
	client.filterMu.RUnlock()
	if !allowSelf {
		return
	}

	enqueueAt := time.Now().UTC()
	client.enqueueSpot(&spotEnvelope{spot: spot, enqueueAt: enqueueAt})
}

// BroadcastRaw sends a raw line to all connected clients without formatting.
// Use this for non-spot PC frames (e.g., PC26) that should pass through unchanged.
func (s *Server) BroadcastRaw(line string) {
	if s == nil || strings.TrimSpace(line) == "" {
		return
	}
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	for _, client := range s.clients {
		_ = client.Send(line)
	}
}

// BroadcastWWV sends a WWV/WCY bulletin to all connected clients that allow it.
// The kind must be WWV, WCY, PC23, or PC73; unknown kinds are ignored.
func (s *Server) BroadcastWWV(kind string, line string) {
	kind = normalizeWWVKind(kind)
	if kind == "" {
		return
	}
	s.broadcastBulletin(kind, line, true)
}

// BroadcastAnnouncement sends a PC93 announcement to all connected clients
// that allow announcements.
func (s *Server) BroadcastAnnouncement(line string) {
	s.broadcastBulletin("ANNOUNCE", line, true)
}

// SendDirectMessage sends a PC93 talk message to a specific callsign when connected.
func (s *Server) SendDirectMessage(callsign string, line string) {
	if s == nil {
		return
	}
	callsign = strutil.NormalizeUpper(callsign)
	if callsign == "" {
		return
	}
	message := prepareBulletinLine(line)
	if message == "" {
		return
	}
	s.clientsMutex.RLock()
	client := s.clients[callsign]
	s.clientsMutex.RUnlock()
	if client == nil {
		return
	}
	s.enqueueBulletin(client, "TALK", message)
}

func (s *Server) broadcastBulletin(kind string, line string, applyFilter bool) {
	if s == nil {
		return
	}
	message := prepareBulletinLine(line)
	if message == "" {
		return
	}
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	for _, client := range s.clients {
		if applyFilter {
			client.filterMu.RLock()
			allowed := client.filter.AllowsBulletin(kind)
			client.filterMu.RUnlock()
			if !allowed {
				continue
			}
		}
		s.enqueueBulletin(client, kind, message)
	}
}

func (s *Server) enqueueBulletin(client *Client, kind, message string) {
	if client == nil {
		return
	}
	_ = client.enqueueControl(controlMessage{line: message})
}

func prepareBulletinLine(line string) string {
	trimmed := strings.TrimRight(line, "\r\n")
	if strings.TrimSpace(trimmed) == "" {
		return ""
	}
	return trimmed + "\n"
}

func normalizeWWVKind(kind string) string {
	kind = strutil.NormalizeUpper(kind)
	switch kind {
	case "WWV", "WCY":
		return kind
	case "PC23":
		return "WWV"
	case "PC73":
		return "WCY"
	default:
		return ""
	}
}

// broadcastSpot segments clients into shards and enqueues jobs for workers.
func (s *Server) broadcastSpot(payload *broadcastPayload) {
	if payload == nil || payload.spot == nil {
		return
	}
	shards := s.cachedClientShards()
	s.dispatchSpotToWorkers(payload, shards)
}

func (s *Server) startWorkerPool() {
	if s.broadcastWorkers <= 0 {
		s.broadcastWorkers = defaultBroadcastWorkers()
	}
	if len(s.workerQueues) != 0 {
		return
	}
	queueSize := s.workerQueueSize
	if queueSize <= 0 {
		queueSize = defaultWorkerQueueSize
	}
	s.workerQueues = make([]chan *broadcastJob, s.broadcastWorkers)
	for i := 0; i < s.broadcastWorkers; i++ {
		s.workerQueues[i] = make(chan *broadcastJob, queueSize)
		go s.broadcastWorker(i, s.workerQueues[i])
	}
}

func (s *Server) dispatchSpotToWorkers(payload *broadcastPayload, shards [][]*Client) {
	for i, clients := range shards {
		if len(clients) == 0 {
			continue
		}
		job := &broadcastJob{
			spot:      payload.spot,
			allowFast: payload.allowFast,
			allowMed:  payload.allowMed,
			allowSlow: payload.allowSlow,
			clients:   clients,
			enqueueAt: payload.enqueueAt,
		}
		select {
		case s.workerQueues[i] <- job:
		default:
			drops := atomic.AddUint64(&s.metrics.queueDrops, 1)
			if _, ok := s.workerDropLog.Inc(); ok {
				log.Printf("Worker %d queue full (%d pending jobs), dropping %d-client shard (total queue drops=%d)", i, len(s.workerQueues[i]), len(clients), drops)
			}
		}
	}
}

// cachedClientShards returns the shard snapshot, rebuilding only when marked dirty.
func (s *Server) cachedClientShards() [][]*Client {
	if !s.shardsDirty.Load() && len(s.clientShards) > 0 {
		return s.clientShards
	}

	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()

	workers := s.broadcastWorkers
	if workers <= 0 {
		workers = 1
	}
	shards := make([][]*Client, workers)
	idx := 0
	for _, client := range s.clients {
		shard := idx % workers
		shards[shard] = append(shards[shard], client)
		idx++
	}
	s.clientShards = shards
	s.shardsDirty.Store(false)
	return shards
}

func (s *Server) broadcastWorker(id int, jobs <-chan *broadcastJob) {
	log.Printf("Broadcast worker %d started", id)
	// Immediate mode when batching is disabled.
	if s.batchInterval <= 0 {
		for {
			select {
			case <-s.shutdown:
				return
			case job := <-jobs:
				if job == nil {
					continue
				}
				s.deliverJob(job)
			}
		}
	}

	ticker := time.NewTicker(s.batchInterval)
	defer ticker.Stop()
	batch := make([]*broadcastJob, 0, s.batchMax)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, job := range batch {
			if job == nil {
				continue
			}
			s.deliverJob(job)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-s.shutdown:
			flush()
			return
		case job := <-jobs:
			if job == nil {
				continue
			}
			batch = append(batch, job)
			if len(batch) >= s.batchMax {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// normalizedDXCall returns the normalized DX callsign for matching.
func normalizedDXCall(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	if s.DXCallNorm != "" {
		return strutil.NormalizeUpper(s.DXCallNorm)
	}
	return spot.NormalizeCallsign(s.DXCall)
}

// isSelfMatch reports whether the spot DX call matches the provided callsign.
// Both values are normalized for portable/SSID consistency.
func isSelfMatch(s *spot.Spot, callsign string) bool {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if callsign == "" {
		return false
	}
	dxCall := normalizedDXCall(s)
	if dxCall == "" {
		return false
	}
	return dxCall == callsign
}

func (s *Server) deliverJob(job *broadcastJob) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("telnet: panic delivering broadcast job: %v", r)
		}
	}()
	for _, client := range job.clients {
		if client == nil {
			continue
		}
		policyAllowed := job.allowFast
		switch client.getDedupePolicy() {
		case dedupePolicyMed:
			policyAllowed = job.allowMed
		case dedupePolicySlow:
			policyAllowed = job.allowSlow
		}
		if !policyAllowed {
			if !isSelfMatch(job.spot, client.callsign) {
				continue
			}
			client.filterMu.RLock()
			allowSelf := client.filter.SelfEnabled()
			client.filterMu.RUnlock()
			if !allowSelf {
				continue
			}
		} else {
			// Self-match spots can bypass filters when SELF is enabled.
			if isSelfMatch(job.spot, client.callsign) {
				client.filterMu.RLock()
				allowSelf := client.filter.SelfEnabled()
				client.filterMu.RUnlock()
				if !allowSelf {
					continue
				}
			} else {
				pathClass := filter.PathClassInsufficient
				client.filterMu.RLock()
				f := client.filter
				pathActive := f != nil && f.PathFilterActive()
				pathBlockAll := f != nil && f.BlockAllPathClasses
				client.filterMu.RUnlock()
				if pathActive && !pathBlockAll {
					pathClass = s.pathClassForClient(client, job.spot)
				}
				client.filterMu.RLock()
				matches := f != nil && f.MatchesWithPath(job.spot, pathClass)
				client.filterMu.RUnlock()
				if !matches {
					continue
				}
			}
		}
		client.enqueueSpot(&spotEnvelope{spot: job.spot, enqueueAt: job.enqueueAt})
	}
}

// WorkerCount returns the number of broadcast workers currently configured.
func (s *Server) WorkerCount() int {
	workers := s.broadcastWorkers
	if workers <= 0 {
		workers = defaultBroadcastWorkers()
	}
	return workers
}

func (s *Server) BroadcastMetricSnapshot() (queueDrops, clientDrops, senderFailures uint64) {
	return s.metrics.snapshot()
}

type pathPredictionStats struct {
	Total        uint64
	Derived      uint64
	Combined     uint64
	Insufficient uint64
	NoSample     uint64
	LowWeight    uint64
	OverrideR    uint64
	OverrideG    uint64
}

func (s *Server) recordPathPrediction(res pathreliability.Result, userDerived, dxDerived bool) {
	if s == nil {
		return
	}
	s.pathPredTotal.Add(1)
	if userDerived || dxDerived {
		s.pathPredDerived.Add(1)
	}
	switch res.Source {
	case pathreliability.SourceCombined:
		s.pathPredCombined.Add(1)
	default:
		s.pathPredInsufficient.Add(1)
		if res.Weight > 0 {
			s.pathPredLowWeight.Add(1)
		} else {
			s.pathPredNoSample.Add(1)
		}
	}
}

func (s *Server) PathPredictionStatsSnapshot() pathPredictionStats {
	if s == nil {
		return pathPredictionStats{}
	}
	return pathPredictionStats{
		Total:        s.pathPredTotal.Swap(0),
		Derived:      s.pathPredDerived.Swap(0),
		Combined:     s.pathPredCombined.Swap(0),
		Insufficient: s.pathPredInsufficient.Swap(0),
		NoSample:     s.pathPredNoSample.Swap(0),
		LowWeight:    s.pathPredLowWeight.Swap(0),
		OverrideR:    s.pathPredOverrideR.Swap(0),
		OverrideG:    s.pathPredOverrideG.Swap(0),
	}
}

func defaultBroadcastWorkers() int {
	workers := runtime.NumCPU()
	if workers < 4 {
		workers = 4
	}
	return workers
}

// acceptConnections handles incoming connections
func (s *Server) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				// Server is shutting down
				return
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Enforce configured connection limit before spinning up a client goroutine.
		if s.maxConnections > 0 {
			addr := conn.RemoteAddr().String()
			s.clientsMutex.RLock()
			current := len(s.clients)
			s.clientsMutex.RUnlock()
			if current >= s.maxConnections {
				_, _ = conn.Write([]byte("Server full. Try again later.\r\n"))
				conn.Close()
				log.Printf("Rejected connection from %s: max connections reached (%d)", addr, s.maxConnections)
				continue
			}
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(2 * time.Minute)
		}

		// Handle this client in a new goroutine
		go s.handleClient(conn)
	}
}

// handleClient manages a single client connection
func (s *Server) handleClient(conn net.Conn) {
	address := conn.RemoteAddr().String()
	log.Printf("New connection from %s", address)

	spotQueueSize := s.clientBufferSize
	if spotQueueSize <= 0 {
		spotQueueSize = defaultClientBufferSize
	}
	controlQueueSize := s.controlQueueSize
	if controlQueueSize <= 0 {
		controlQueueSize = defaultControlQueueSize
	}

	if s.filterEngine == nil {
		s.filterEngine = newFilterCommandEngine()
	}

	// Create client object and select the telnet transport backend.
	readerConn := conn
	writerConn := conn
	if s.useZiutek {
		tconn, err := ztelnet.NewConn(conn)
		if err != nil {
			log.Printf("telnet: failed to wrap connection from %s: %v", address, err)
			_ = conn.Close()
			return
		}
		readerConn = tconn
		writerConn = tconn
	}
	client := &Client{
		conn:           conn,
		reader:         bufio.NewReader(readerConn),
		writer:         bufio.NewWriter(writerConn),
		connected:      time.Now().UTC(),
		server:         s,
		address:        address,
		spotChan:       make(chan *spotEnvelope, spotQueueSize),
		controlChan:    make(chan controlMessage, controlQueueSize),
		done:           make(chan struct{}),
		filter:         filter.NewFilter(), // Start with no filters (accept all)
		dialect:        s.filterEngine.defaultDialect,
		gridCell:       pathreliability.InvalidCell,
		gridCoarseCell: pathreliability.InvalidCell,
		noiseClass:     "QUIET",
		noisePenalty:   0,
		// Echo policy is configured explicitly so we can support local echo even
		// when clients toggle their own modes.
		echoInput: s.echoMode == telnetEchoServer,
	}
	client.dropWindow = newDropWindow(s.dropExtremeWindow)

	closeOnExit := true
	registered := false
	defer func() {
		if closeOnExit {
			client.close("")
		}
		if registered {
			s.unregisterClient(client)
		}
	}()

	s.negotiateTelnet(client)
	go client.writerLoop()

	// Send welcome message with template tokens (uptime, user count, etc.).
	loginTime := time.Now().UTC()
	s.sendPreLoginMessage(client, s.welcomeMessage, loginTime)
	s.sendPreLoginMessage(client, s.loginPrompt, loginTime)

	var callsign string
	for {
		if err := client.setReadDeadline(time.Now().Add(s.loginTimeout)); err != nil {
			log.Printf("Failed to set login deadline for %s: %v", address, err)
			return
		}
		// Read callsign with tight guard rails so a single telnet client cannot
		// consume unbounded memory or smuggle control characters during login. The
		// limit is configurable but defaults to 32 bytes which covers every valid
		// callsign, including suffixes such as /QRP or /MM.
		line, err := client.ReadLine(s.loginLineLimit, "login", false, false, false, false)
		if err != nil {
			if isTimeoutErr(err) {
				log.Printf("Login timeout for %s", address)
				return
			}
			var inputErr *InputValidationError
			if errors.As(err, &inputErr) {
				if msg := s.formatInputValidationMessage(inputErr); strings.TrimSpace(msg) != "" {
					_ = client.Send(msg)
				}
				s.sendPreLoginMessage(client, s.loginPrompt, loginTime)
				continue
			}
			log.Printf("Error reading callsign from %s: %v", address, err)
			return
		}

		line = strutil.NormalizeUpper(line)
		if line == "" {
			s.sendPreLoginMessage(client, s.loginEmptyMessage, loginTime)
			s.sendPreLoginMessage(client, s.loginPrompt, loginTime)
			continue
		}
		normalized := spot.NormalizeCallsign(line)
		if !spot.IsValidNormalizedCallsign(normalized) {
			s.sendPreLoginMessage(client, s.loginInvalidMsg, loginTime)
			s.sendPreLoginMessage(client, s.loginPrompt, loginTime)
			continue
		}
		callsign = normalized
		break
	}

	client.callsign = callsign
	log.Printf("Client %s logged in as %s", address, client.callsign)

	if s.reputationGate != nil {
		s.reputationGate.RecordLogin(client.callsign, spotterIP(client.address), time.Now().UTC())
	}

	// Capture the client's IP immediately after login so it is persisted before
	// any other session state mutates.
	record, created, prevLogin, prevIP, err := filter.TouchUserRecordLogin(client.callsign, spotterIP(client.address), loginTime)
	if err == nil {
		client.filter = &record.Filter
		client.recentIPs = record.RecentIPs
		client.dialect = normalizeDialectName(record.Dialect)
		client.setDedupePolicy(s.resolveDedupePolicy(parseDedupePolicy(record.DedupePolicy)))
		client.grid = strings.ToUpper(strings.TrimSpace(record.Grid))
		client.gridDerived = false
		client.gridCell = pathreliability.EncodeCell(client.grid)
		client.gridCoarseCell = pathreliability.EncodeCoarseCell(client.grid)
		client.noiseClass = strings.ToUpper(strings.TrimSpace(record.NoiseClass))
		client.noisePenalty = s.noisePenaltyForClass(client.noiseClass)
		client.setSolarSummaryMinutes(record.SolarSummaryMinutes, loginTime)
		if created {
			log.Printf("Created default filter for %s", client.callsign)
		} else {
			log.Printf("Loaded saved filter for %s", client.callsign)
		}
	} else {
		client.filter = filter.NewFilter()
		client.recentIPs = filter.UpdateRecentIPs(nil, spotterIP(client.address))
		client.dialect = s.filterEngine.defaultDialect
		client.setDedupePolicy(s.resolveDedupePolicy(dedupePolicyMed))
		client.gridCell = pathreliability.InvalidCell
		client.gridCoarseCell = pathreliability.InvalidCell
		client.gridDerived = false
		client.noiseClass = "QUIET"
		client.noisePenalty = s.noisePenaltyForClass(client.noiseClass)
		log.Printf("Warning: failed to load user record for %s: %v", client.callsign, err)
		if err := client.saveFilter(); err != nil {
			log.Printf("Warning: failed to save user record for %s: %v", client.callsign, err)
		}
	}

	// Seed grid from lookup when none is stored.
	if strings.TrimSpace(client.grid) == "" && s.gridLookup != nil {
		if g, derived, ok := s.gridLookup(client.callsign); ok {
			client.grid = strings.ToUpper(strings.TrimSpace(g))
			client.gridDerived = derived
			client.gridCell = pathreliability.EncodeCell(client.grid)
			client.gridCoarseCell = pathreliability.EncodeCoarseCell(client.grid)
		}
	}
	// Normalize noise defaults when absent.
	if strings.TrimSpace(client.noiseClass) == "" {
		client.noiseClass = "QUIET"
		client.noisePenalty = s.noisePenaltyForClass(client.noiseClass)
	}

	nearbyWarning, nearbyChanged := applyNearbyLoginState(client, s.nearbyLoginWarning)
	if nearbyChanged {
		if err := client.saveFilter(); err != nil {
			log.Printf("Warning: failed to persist NEARBY state for %s: %v", client.callsign, err)
		}
	}

	// Register client
	s.registerClient(client)
	registered = true

	// Send login confirmation
	dialectSource := s.dialectSourceLabel(client.dialect, created, err, s.filterEngine.defaultDialect)
	dialectDefault := strings.ToUpper(string(s.filterEngine.defaultDialect))
	greeting := formatGreeting(s.greetingTemplate, s.postLoginTemplateData(loginTime, client, prevLogin, prevIP, dialectSource, dialectDefault))
	if strings.TrimSpace(nearbyWarning) != "" {
		if strings.TrimSpace(greeting) == "" {
			greeting = nearbyWarning
		} else {
			if !strings.HasSuffix(greeting, "\n") {
				greeting += "\n"
			}
			greeting += nearbyWarning
		}
	}
	if strings.TrimSpace(greeting) != "" {
		client.Send(greeting)
	}
	dialectMsg := formatDialectWelcome(s.dialectWelcomeMsg, dialectTemplateData{
		dialect:        strings.ToUpper(string(client.dialect)),
		source:         dialectSource,
		defaultDialect: dialectDefault,
	})
	if strings.TrimSpace(dialectMsg) != "" {
		client.Send(dialectMsg)
	}
	if s.pathPredictor != nil && s.pathDisplay {
		if msg := s.formatPathStatusMessage(client); strings.TrimSpace(msg) != "" {
			client.Send(msg)
		}
	}

	// Read commands from client
	for {
		if err := client.setReadDeadline(time.Now().Add(s.readIdleTimeout)); err != nil {
			log.Printf("Failed to set read deadline for %s: %v", client.callsign, err)
			return
		}
		// Commands use the more relaxed limit because filter manipulation can
		// legitimately include several tokens. The limit is still kept small
		// (default 128 bytes) to keep parsing cheap and predictable.
		line, err := client.ReadLine(s.commandLineLimit, "command", true, true, true, true)
		if err != nil {
			if isTimeoutErr(err) {
				continue
			}
			var inputErr *InputValidationError
			if errors.As(err, &inputErr) {
				if msg := s.formatInputValidationMessage(inputErr); strings.TrimSpace(msg) != "" {
					_ = client.Send(msg)
				}
				continue
			}
			log.Printf("Client %s disconnected: %v", client.callsign, err)
			return
		}

		// Treat blank lines as client keepalives: echo CRLF so idle clients see traffic.
		if strings.TrimSpace(line) == "" {
			_ = client.Send("\r\n")
			continue
		}

		// Dialect selection is handled before filter commands.
		if resp, handled := s.handleDialectCommand(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		if resp, handled := s.handlePathSettingsCommand(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		if resp, handled := s.handleDedupeCommand(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		if resp, handled := s.handleDiagCommand(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		if resp, handled := s.handleSolarCommand(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		// Check for filter commands under the active dialect.
		if resp, handled := s.filterEngine.Handle(client, line); handled {
			if resp != "" {
				client.Send(resp)
			}
			continue
		}

		// Process other commands
		filterFn := func(spotEntry *spot.Spot) bool {
			if spotEntry == nil {
				return false
			}
			if strings.EqualFold(spotEntry.DXCall, client.callsign) {
				return true
			}
			pathClass := filter.PathClassInsufficient
			client.filterMu.RLock()
			f := client.filter
			pathActive := f != nil && f.PathFilterActive()
			pathBlockAll := f != nil && f.BlockAllPathClasses
			client.filterMu.RUnlock()
			if pathActive && !pathBlockAll {
				pathClass = s.pathClassForClient(client, spotEntry)
			}
			client.filterMu.RLock()
			matches := f != nil && f.MatchesWithPath(spotEntry, pathClass)
			client.filterMu.RUnlock()
			return matches
		}
		response := s.processor.ProcessCommandForClient(line, client.callsign, spotterIP(client.address), filterFn, string(client.dialect))

		// Check for disconnect signal
		if response == "BYE" {
			_ = client.SendAndClose("73!\n")
			closeOnExit = false
			log.Printf("Client %s logged out", client.callsign)
			return
		}

		// Send response
		if response != "" {
			client.Send(response)
		}
	}
}

// negotiateTelnet performs minimal option negotiation to keep echo behavior
// predictable across telnet clients. It writes directly to the raw connection
// to avoid IAC escaping by higher-level telnet transports.
func (s *Server) negotiateTelnet(client *Client) {
	if s.skipHandshake || client == nil || client.conn == nil {
		return
	}
	conn := client.conn
	// Prefer full-duplex sessions by suppressing go-ahead.
	sendTelnetOption(conn, WILL, 3)
	sendTelnetOption(conn, DO, 3)

	switch s.echoMode {
	case telnetEchoServer:
		// Server will echo input; ask the client to disable local echo.
		sendTelnetOption(conn, WILL, 1)
		sendTelnetOption(conn, DONT, 1)
	case telnetEchoLocal:
		// Server will not echo; most clients enable local echo in response.
		sendTelnetOption(conn, WONT, 1)
	case telnetEchoOff:
		// Best-effort: request no echo from either side.
		sendTelnetOption(conn, WONT, 1)
		sendTelnetOption(conn, DONT, 1)
	default:
		// Fall back to server echo if the mode is unknown.
		sendTelnetOption(conn, WILL, 1)
		sendTelnetOption(conn, DONT, 1)
	}
}

func sendTelnetOption(conn net.Conn, command, option byte) {
	if conn == nil {
		return
	}
	if err := conn.SetWriteDeadline(time.Now().UTC().Add(defaultSendDeadline)); err != nil {
		return
	}
	_, _ = conn.Write([]byte{IAC, command, option})
	_ = conn.SetWriteDeadline(time.Time{})
}

// templateData holds contextual values that can be substituted into operator-configured templates.
type templateData struct {
	now            time.Time
	startTime      time.Time
	userCount      int
	callsign       string
	cluster        string
	lastLogin      time.Time
	lastIP         string
	dialect        string
	dialectSource  string
	dialectDefault string
	dedupePolicy   string
	grid           string
	noiseClass     string
}

// formatGreeting replaces placeholders using the provided template data.
func formatGreeting(tmpl string, data templateData) string {
	if tmpl == "" {
		return ""
	}
	return applyTemplateTokens(tmpl, data)
}

func (s *Server) noisePenaltyForClass(class string) float64 {
	if s == nil {
		return 0
	}
	key := strings.ToUpper(strings.TrimSpace(class))
	if val, ok := s.noiseOffsets[key]; ok {
		return val
	}
	return 0
}

// formatSpotForClient renders a spot with optional reliability glyphs.
func (s *Server) formatSpotForClient(client *Client, sp *spot.Spot) string {
	if sp == nil {
		return ""
	}
	if client != nil && client.diagEnabled.Load() {
		return s.formatSpotForClientWithDiag(client, sp)
	}
	base := sp.FormatDXCluster()
	if s == nil || s.pathPredictor == nil || !s.pathDisplay {
		return base + "\n"
	}
	glyphs := s.pathGlyphsForClient(client, sp)
	if glyphs != "" {
		base = injectGlyphs(base, glyphs)
	}
	return base + "\n"
}

func (s *Server) formatSpotForClientWithDiag(client *Client, sp *spot.Spot) string {
	if sp == nil {
		return ""
	}
	clone := sp.CloneWithComment(diagTagForSpot(client, sp))
	if clone == nil {
		return ""
	}

	base := clone.FormatDXCluster()
	if s == nil || s.pathPredictor == nil || !s.pathDisplay {
		return base + "\n"
	}
	glyphs := s.pathGlyphsForClient(client, sp)
	if glyphs != "" {
		base = injectGlyphs(base, glyphs)
	}
	return base + "\n"
}

func (s *Server) pathGlyphsForClient(client *Client, sp *spot.Spot) string {
	if client == nil || sp == nil {
		return ""
	}
	cfg := s.pathPredictor.Config()
	if !cfg.Enabled {
		return ""
	}
	state := client.pathSnapshot()
	userCell := state.gridCell
	grid := strings.TrimSpace(state.grid)
	noisePenalty := state.noisePenalty
	if userCell == pathreliability.InvalidCell && grid != "" {
		cell := pathreliability.EncodeCell(grid)
		if cell != pathreliability.InvalidCell {
			client.pathMu.Lock()
			if client.gridCell == pathreliability.InvalidCell && strings.EqualFold(strings.TrimSpace(client.grid), grid) {
				client.gridCell = cell
			}
			userCell = client.gridCell
			client.pathMu.Unlock()
		}
	}
	if userCell == pathreliability.InvalidCell {
		return ""
	}
	dxCell := pathreliability.InvalidCell
	if sp.DXCellID != 0 {
		dxCell = pathreliability.CellID(sp.DXCellID)
	}
	if dxCell == pathreliability.InvalidCell {
		dxCell = pathreliability.EncodeCell(sp.DXMetadata.Grid)
	}
	if dxCell == pathreliability.InvalidCell {
		return ""
	}
	userCoarse := pathreliability.EncodeCoarseCell(grid)
	dxCoarse := pathreliability.EncodeCoarseCell(sp.DXMetadata.Grid)

	band := sp.BandNorm
	if strings.TrimSpace(band) == "" {
		band = sp.Band
	}
	mode := sp.ModeNorm
	if strings.TrimSpace(mode) == "" {
		mode = sp.Mode
	}
	now := time.Now().UTC()
	res := s.pathPredictor.Predict(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, now)
	s.recordPathPrediction(res, state.gridDerived, sp.DXMetadata.GridDerived)
	if res.Source == pathreliability.SourceInsufficient {
		return res.Glyph
	}
	g := res.Glyph
	if s.solarWeather != nil {
		override, kind := s.solarWeather.OverrideGlyph(now, solarweather.PathInput{
			UserGrid: grid,
			DXGrid:   sp.DXMetadata.Grid,
			UserCell: userCell,
			DXCell:   dxCell,
			Band:     band,
		})
		if kind != solarweather.OverrideNone && override != "" {
			g = override
			switch kind {
			case solarweather.OverrideR:
				s.pathPredOverrideR.Add(1)
			case solarweather.OverrideG:
				s.pathPredOverrideG.Add(1)
			}
		}
	}
	return g
}

func (s *Server) pathClassForClient(client *Client, sp *spot.Spot) string {
	if s == nil || client == nil || sp == nil {
		return filter.PathClassInsufficient
	}
	if s.pathPredictor == nil {
		return filter.PathClassInsufficient
	}
	cfg := s.pathPredictor.Config()
	if !cfg.Enabled {
		return filter.PathClassInsufficient
	}
	state := client.pathSnapshot()
	userCell := state.gridCell
	grid := strings.TrimSpace(state.grid)
	noisePenalty := state.noisePenalty
	if userCell == pathreliability.InvalidCell && grid != "" {
		cell := pathreliability.EncodeCell(grid)
		if cell != pathreliability.InvalidCell {
			client.pathMu.Lock()
			if client.gridCell == pathreliability.InvalidCell && strings.EqualFold(strings.TrimSpace(client.grid), grid) {
				client.gridCell = cell
			}
			userCell = client.gridCell
			client.pathMu.Unlock()
		}
	}
	if userCell == pathreliability.InvalidCell {
		return filter.PathClassInsufficient
	}
	dxCell := pathreliability.InvalidCell
	if sp.DXCellID != 0 {
		dxCell = pathreliability.CellID(sp.DXCellID)
	}
	if dxCell == pathreliability.InvalidCell {
		dxCell = pathreliability.EncodeCell(sp.DXMetadata.Grid)
	}
	if dxCell == pathreliability.InvalidCell {
		return filter.PathClassInsufficient
	}
	userCoarse := pathreliability.EncodeCoarseCell(grid)
	dxCoarse := pathreliability.EncodeCoarseCell(sp.DXMetadata.Grid)

	band := strings.TrimSpace(sp.BandNorm)
	if band == "" {
		band = strings.TrimSpace(sp.Band)
	}
	if band == "" {
		band = spot.FreqToBand(sp.Frequency)
	}
	band = strings.TrimSpace(spot.NormalizeBand(band))

	mode := strings.TrimSpace(sp.ModeNorm)
	if mode == "" {
		mode = strings.TrimSpace(sp.Mode)
	}
	now := time.Now().UTC()
	res := s.pathPredictor.Predict(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, now)
	if res.Source == pathreliability.SourceInsufficient {
		return filter.PathClassInsufficient
	}
	return pathreliability.ClassForPower(res.Value, mode, cfg)
}

func diagTagForSpot(client *Client, sp *spot.Spot) string {
	if client == nil || sp == nil {
		return ""
	}
	source := diagSourceToken(sp)
	dedxcc := " "
	if sp.DEMetadata.ADIF > 0 {
		dedxcc = strconv.Itoa(sp.DEMetadata.ADIF)
	}
	keyToken := diagDedupeKeyToken(client.getDedupePolicy(), sp)
	if keyToken == "" {
		keyToken = " "
	}
	band := diagBandToken(sp)
	if band == "" {
		band = " "
	}
	policy := diagPolicyToken(client.getDedupePolicy())

	var b strings.Builder
	b.Grow(len(source) + len(dedxcc) + len(keyToken) + len(band) + len(policy))
	b.WriteString(source)
	b.WriteString(dedxcc)
	b.WriteString(keyToken)
	b.WriteString(band)
	b.WriteString(policy)
	return b.String()
}

func diagSourceToken(sp *spot.Spot) string {
	switch sp.SourceType {
	case spot.SourceRBN, spot.SourceFT8, spot.SourceFT4:
		return "R"
	case spot.SourcePSKReporter:
		return "P"
	case spot.SourceUpstream, spot.SourcePeer, spot.SourceManual:
		return "H"
	default:
		return "H"
	}
}

func diagDedupeKeyToken(policy dedupePolicy, sp *spot.Spot) string {
	if policy == dedupePolicySlow {
		return diagDECQZone(sp)
	}
	return diagDEGrid2(sp)
}

func diagDEGrid2(sp *spot.Spot) string {
	grid2 := sp.DEGrid2
	if grid2 == "" {
		grid := sp.DEGridNorm
		if grid == "" {
			grid = sp.DEMetadata.Grid
		}
		if len(grid) >= 2 {
			grid2 = grid[:2]
		}
	}
	grid2 = strings.ToUpper(strings.TrimSpace(grid2))
	if len(grid2) > 2 {
		grid2 = grid2[:2]
	}
	if len(grid2) < 2 {
		return ""
	}
	return grid2
}

func diagDECQZone(sp *spot.Spot) string {
	if sp == nil {
		return ""
	}
	zone := sp.DEMetadata.CQZone
	if zone <= 0 {
		return ""
	}
	return fmt.Sprintf("%02d", zone)
}

func diagBandToken(sp *spot.Spot) string {
	band := strings.TrimSpace(sp.BandNorm)
	if band == "" {
		band = strings.TrimSpace(sp.Band)
	}
	if band == "" {
		band = spot.FreqToBand(sp.Frequency)
	}
	band = strings.TrimSpace(spot.NormalizeBand(band))
	if band == "" || band == "???" {
		return ""
	}
	if strings.HasSuffix(band, "m") && !strings.HasSuffix(band, "cm") {
		band = strings.TrimSuffix(band, "m")
	}
	return band
}

func diagPolicyToken(policy dedupePolicy) string {
	if policy == dedupePolicySlow {
		return "S"
	}
	if policy == dedupePolicyMed {
		return "M"
	}
	return "F"
}

func injectGlyphs(base string, glyph string) string {
	layout := spot.CurrentDXClusterLayout()
	if layout.LineLength <= 0 || len(glyph) < 1 || len(base) < layout.LineLength {
		return base
	}
	// Glyph column is anchored to the configured tail layout.
	pos := layout.GlyphColumn - 1
	if pos < 0 || pos >= len(base) {
		return base
	}
	b := []byte(base)
	asciiGlyph := firstPrintableASCIIOrQuestion(glyph)
	if pos-1 >= 0 && pos-1 < len(b) {
		b[pos-1] = ' '
	}
	b[pos] = asciiGlyph
	return string(b)
}

// Purpose: Return the first printable ASCII byte or '?' for non-ASCII.
// Key aspects: Enforces ASCII-only telnet output for glyph injection.
// Upstream: injectGlyphs.
// Downstream: None.
func firstPrintableASCIIOrQuestion(s string) byte {
	for _, r := range s {
		if r >= 0x20 && r <= 0x7e {
			return byte(r)
		}
		return '?'
	}
	return '?'
}

// applyWelcomeTokens remains for compatibility; it now delegates to the full token replacer.
func applyWelcomeTokens(msg string, now time.Time) string {
	return applyTemplateTokens(msg, templateData{now: now})
}

// applyTemplateTokens replaces supported placeholders in operator-provided templates.
// Tokens:
//
//	<CALL>        -> client callsign
//	<CLUSTER>     -> cluster/node ID
//	<DATE>        -> DD-Mon-YYYY (UTC)
//	<TIME>        -> HH:MM:SS (UTC)
//	<DATETIME>    -> DD-Mon-YYYY HH:MM:SS UTC
//	<UPTIME>      -> uptime since server start (e.g., 3d 04:18:22 or 00:03:05)
//	<USER_COUNT>  -> current connected user count
//	<LAST_LOGIN>  -> previous login timestamp or "(first login)"
//	<LAST_IP>     -> previous login IP or "(unknown)"
//	<DEDUPE>      -> active dedupe policy (FAST/MED/SLOW)
func applyTemplateTokens(msg string, data templateData) string {
	if msg == "" {
		return msg
	}
	now := data.now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	date := now.Format("02-Jan-2006")
	tm := now.Format("15:04:05")
	datetime := now.Format("02-Jan-2006 15:04:05 UTC")

	uptime := formatUptime(now, data.startTime)
	if uptime == "" {
		uptime = "unknown"
	}
	userCount := "0"
	if data.userCount > 0 {
		userCount = strconv.Itoa(data.userCount)
	}
	lastLogin := "(first login)"
	if !data.lastLogin.IsZero() {
		lastLogin = data.lastLogin.UTC().Format("02-Jan-2006 15:04:05 UTC")
	}
	lastIP := strings.TrimSpace(data.lastIP)
	if lastIP == "" {
		lastIP = "(unknown)"
	}

	replacer := strings.NewReplacer(
		"<CALL>", data.callsign,
		"<CLUSTER>", data.cluster,
		"<DATETIME>", datetime,
		"<DATE>", date,
		"<TIME>", tm,
		"<UPTIME>", uptime,
		"<USER_COUNT>", userCount,
		"<LAST_LOGIN>", lastLogin,
		"<LAST_IP>", lastIP,
		"<DIALECT>", data.dialect,
		"<DIALECT_SOURCE>", data.dialectSource,
		"<DIALECT_DEFAULT>", data.dialectDefault,
		"<DEDUPE>", data.dedupePolicy,
		"<GRID>", data.grid,
		"<NOISE>", data.noiseClass,
	)
	return replacer.Replace(msg)
}

type inputTemplateData struct {
	context string
	maxLen  int
	allowed string
}

func applyInputTemplateTokens(msg string, data inputTemplateData) string {
	if msg == "" {
		return msg
	}
	maxLen := ""
	if data.maxLen > 0 {
		maxLen = strconv.Itoa(data.maxLen)
	}
	replacer := strings.NewReplacer(
		"<CONTEXT>", data.context,
		"<MAX_LEN>", maxLen,
		"<ALLOWED>", data.allowed,
	)
	return replacer.Replace(msg)
}

func (s *Server) formatInputValidationMessage(err *InputValidationError) string {
	if s == nil || err == nil {
		return ""
	}
	var template string
	switch err.kind {
	case inputErrorTooLong:
		template = s.inputTooLongMsg
	case inputErrorInvalidChar:
		template = s.inputInvalidMsg
	default:
		return ""
	}
	if strings.TrimSpace(template) == "" {
		return ""
	}
	data := inputTemplateData{
		context: friendlyContextLabel(err.context),
		maxLen:  err.maxLen,
		allowed: err.allowed,
	}
	return applyInputTemplateTokens(template, data)
}

func formatUptime(now, start time.Time) string {
	if start.IsZero() || now.Before(start) {
		return ""
	}
	dur := now.Sub(start).Round(time.Second)
	days := dur / (24 * time.Hour)
	dur -= days * 24 * time.Hour
	hours := dur / time.Hour
	dur -= hours * time.Hour
	minutes := dur / time.Minute
	dur -= minutes * time.Minute
	seconds := dur / time.Second
	if days > 0 {
		return fmt.Sprintf("%dd %02d:%02d:%02d", days, hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

func (s *Server) preLoginTemplateData(now time.Time) templateData {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	dialect := ""
	defaultDialect := ""
	if s != nil && s.filterEngine != nil {
		dialect = strings.ToUpper(string(s.filterEngine.defaultDialect))
		defaultDialect = strings.ToUpper(string(s.filterEngine.defaultDialect))
	}
	return templateData{
		now:            now,
		startTime:      s.startTime,
		userCount:      s.GetClientCount(),
		cluster:        s.clusterCall,
		dialect:        dialect,
		dialectSource:  s.dialectSourceDef,
		dialectDefault: defaultDialect,
		dedupePolicy:   filter.DedupePolicyMed,
	}
}

func (s *Server) sendPreLoginMessage(client *Client, template string, now time.Time) {
	if client == nil || s == nil {
		return
	}
	msg := applyTemplateTokens(template, s.preLoginTemplateData(now))
	if strings.TrimSpace(msg) == "" {
		return
	}
	_ = client.Send(msg)
}

func (s *Server) postLoginTemplateData(now time.Time, client *Client, prevLogin time.Time, prevIP, dialectSource, dialectDefault string) templateData {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	callsign := ""
	grid := ""
	noise := ""
	dialect := ""
	dedupePolicy := filter.DedupePolicyMed
	if client != nil {
		callsign = client.callsign
		state := client.pathSnapshot()
		grid = displayGrid(state.grid, state.gridDerived)
		if grid == "" {
			grid = "unset"
		}
		noise = strings.TrimSpace(state.noiseClass)
		dialect = strings.ToUpper(string(client.dialect))
		dedupePolicy = client.getDedupePolicy().label()
	}
	return templateData{
		now:            now,
		startTime:      s.startTime,
		userCount:      s.GetClientCount(),
		callsign:       callsign,
		cluster:        s.clusterCall,
		lastLogin:      prevLogin,
		lastIP:         prevIP,
		dialect:        dialect,
		dialectSource:  dialectSource,
		dialectDefault: dialectDefault,
		dedupePolicy:   dedupePolicy,
		grid:           grid,
		noiseClass:     noise,
	}
}

func applyNearbyLoginState(client *Client, warning string) (warningLine string, changed bool) {
	if client == nil || client.filter == nil {
		return "", false
	}
	if !client.filter.NearbyActive() {
		return "", false
	}
	state := client.pathSnapshot()
	grid := strings.TrimSpace(state.grid)
	if grid == "" {
		return nearbyLoginInactiveMsg, false
	}
	userFine := state.gridCell
	if userFine == pathreliability.InvalidCell {
		userFine = pathreliability.EncodeCell(grid)
	}
	userCoarse := state.gridCoarseCell
	if userCoarse == pathreliability.InvalidCell {
		userCoarse = pathreliability.EncodeCoarseCell(grid)
	}
	if userFine == pathreliability.InvalidCell || userCoarse == pathreliability.InvalidCell {
		return nearbyLoginInactiveMsg, false
	}
	var enableErr error
	client.updateFilter(func(f *filter.Filter) {
		enableErr = f.EnableNearby(userFine, userCoarse)
	})
	if enableErr != nil {
		return nearbyLoginInactiveMsg, false
	}
	return normalizeWarningLine(warning), false
}

func normalizeWarningLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return ""
	}
	if strings.HasSuffix(trimmed, "\n") {
		return trimmed
	}
	return trimmed + "\n"
}

// writerLoop serializes all outbound traffic to the client and enforces
// control-before-spot priority. It exits when the client is closed.
func (c *Client) writerLoop() {
	if c == nil {
		return
	}
	controlCh := c.controlChan
	spotCh := c.spotChan
	for {
		// Drain all pending control messages before considering spots.
		for {
			select {
			case <-c.done:
				return
			case msg := <-controlCh:
				if !c.handleControl(msg) {
					return
				}
				continue
			default:
			}
			break
		}

		select {
		case <-c.done:
			return
		case msg := <-controlCh:
			if !c.handleControl(msg) {
				return
			}
		case env := <-spotCh:
			if !c.handleSpot(env) {
				return
			}
		}
	}
}

func (c *Client) handleControl(msg controlMessage) bool {
	if msg.raw == nil && strings.TrimSpace(msg.line) == "" {
		return true
	}
	start := time.Now().UTC()
	var err error
	if len(msg.raw) > 0 {
		err = c.writeRaw(msg.raw)
	} else {
		err = c.writeLine(msg.line)
	}
	if err != nil {
		c.logWriterFailure("control", err)
		c.close("writer failure")
		return false
	}
	if c.server != nil {
		c.server.observeWriteStallLatency(time.Since(start))
	}
	if msg.closeAfter {
		c.close("control close")
		return false
	}
	return true
}

func (c *Client) handleSpot(env *spotEnvelope) bool {
	if env == nil || env.spot == nil {
		return true
	}
	if c.server != nil && !env.enqueueAt.IsZero() {
		c.server.observeFirstByteLatency(time.Since(env.enqueueAt))
	}
	formatted := env.spot.FormatDXCluster() + "\n"
	if c.server != nil {
		formatted = c.server.formatSpotForClient(c, env.spot)
	}
	start := time.Now().UTC()
	if err := c.writeLine(formatted); err != nil {
		c.logWriterFailure("spot", err)
		c.close("writer failure")
		return false
	}
	if c.server != nil {
		c.server.observeWriteStallLatency(time.Since(start))
	}
	return true
}

// enqueueControl pushes a control/bulletin message; a full queue disconnects the client.
func (c *Client) enqueueControl(msg controlMessage) error {
	if c == nil {
		return errors.New("nil client")
	}
	if msg.raw == nil && strings.TrimSpace(msg.line) == "" {
		return nil
	}
	select {
	case <-c.done:
		return errors.New("client closed")
	default:
	}
	select {
	case c.controlChan <- msg:
		return nil
	default:
		if c.server != nil {
			drops := atomic.AddUint64(&c.server.metrics.clientDrops, 1)
			if _, ok := c.server.clientDropLog.Inc(); ok {
				log.Printf("Client %s control queue full (%d/%d), disconnecting (total drops=%d)", c.identity(), len(c.controlChan), cap(c.controlChan), drops)
			}
		}
		c.close("control queue full")
		return errors.New("control queue full")
	}
}

// enqueueSpot queues a spot delivery and updates drop metrics + extreme drop detection.
func (c *Client) enqueueSpot(env *spotEnvelope) bool {
	if c == nil || env == nil || env.spot == nil {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	dropped := false
	select {
	case c.spotChan <- env:
		if c.server != nil && !env.enqueueAt.IsZero() {
			c.server.observeEnqueueLatency(time.Since(env.enqueueAt))
		}
	default:
		dropped = true
		if c.server != nil {
			drops := atomic.AddUint64(&c.server.metrics.clientDrops, 1)
			clientDrops := atomic.AddUint64(&c.dropCount, 1)
			if _, ok := c.server.clientDropLog.Inc(); ok {
				log.Printf("Client %s spot queue full (%d/%d), dropping spot (client drops=%d total=%d)", c.identity(), len(c.spotChan), cap(c.spotChan), clientDrops, drops)
			}
		}
	}

	if c.server != nil && c.server.dropExtremeRate > 0 && c.server.dropExtremeWindow > 0 {
		minAttempts := c.server.dropExtremeMinAtt
		if minAttempts <= 0 {
			minAttempts = defaultDropExtremeMinAttempts
		}
		attempts, drops := c.dropWindow.record(time.Now().UTC(), dropped)
		if attempts > 0 && attempts >= uint64(minAttempts) {
			rate := float64(drops) / float64(attempts)
			if rate >= c.server.dropExtremeRate {
				log.Printf("Disconnecting %s due to extreme drop rate %.1f%% (%d/%d over %s)", c.identity(), rate*100, drops, attempts, c.server.dropExtremeWindow)
				c.close("extreme drop rate")
			}
		}
	}

	return !dropped
}

func (c *Client) close(reason string) {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
		if strings.TrimSpace(reason) != "" {
			log.Printf("Disconnected client %s: %s", c.identity(), reason)
		}
	})
}

func (c *Client) identity() string {
	if c == nil {
		return "unknown"
	}
	if strings.TrimSpace(c.callsign) != "" {
		return c.callsign
	}
	if strings.TrimSpace(c.address) != "" {
		return c.address
	}
	return "unknown"
}

func (c *Client) logWriterFailure(kind string, err error) {
	if c == nil || err == nil {
		return
	}
	if c.server != nil {
		c.server.recordSenderFailure()
	}
	log.Printf("Writer failure for %s (%s): %v", c.identity(), kind, err)
}

func (c *Client) writeLine(message string) error {
	if c == nil {
		return errors.New("nil client")
	}
	normalized := strings.ReplaceAll(message, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\n", "\r\n")
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.setWriteDeadline(time.Now().UTC().Add(defaultSendDeadline)); err != nil {
		return err
	}
	defer c.clearWriteDeadline()
	if _, err := c.writer.WriteString(normalized); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *Client) writeRaw(data []byte) error {
	if c == nil {
		return errors.New("nil client")
	}
	if len(data) == 0 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.setWriteDeadline(time.Now().UTC().Add(defaultSendDeadline)); err != nil {
		return err
	}
	defer c.clearWriteDeadline()
	if _, err := c.writer.Write(data); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *Client) setWriteDeadline(deadline time.Time) error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.SetWriteDeadline(deadline)
}

func (c *Client) clearWriteDeadline() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.SetWriteDeadline(time.Time{})
}

func (c *Client) setReadDeadline(deadline time.Time) error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.SetReadDeadline(deadline)
}

func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// registerClient adds a client to the active clients list
func (s *Server) registerClient(client *Client) {
	var evicted *Client
	s.clientsMutex.Lock()
	if existing, ok := s.clients[client.callsign]; ok {
		evicted = existing
		delete(s.clients, client.callsign)
	}
	s.clients[client.callsign] = client
	total := len(s.clients)
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	s.notifyClientListChange()

	if evicted != nil {
		msg := strings.TrimSpace(s.duplicateLoginMsg)
		if msg != "" {
			if !strings.HasSuffix(msg, "\n") {
				msg += "\n"
			}
			_ = evicted.SendAndClose(msg)
		} else {
			evicted.close("duplicate login")
		}
		log.Printf("Evicted existing session for %s due to duplicate login", client.callsign)
	}
	log.Printf("Registered client: %s (total: %d)", client.callsign, total)
}

// unregisterClient removes a client from the active clients list
func (s *Server) unregisterClient(client *Client) {
	s.clientsMutex.Lock()
	current, ok := s.clients[client.callsign]
	if ok && current == client {
		delete(s.clients, client.callsign)
	}
	total := len(s.clients)
	s.shardsDirty.Store(true)
	s.clientsMutex.Unlock()
	s.notifyClientListChange()

	client.saveFilter()
	log.Printf("Unregistered client: %s (total: %d)", client.callsign, total)
}

// GetClientCount returns the number of connected clients
func (s *Server) GetClientCount() int {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	return len(s.clients)
}

// SetClientListListener installs a callback invoked on client connect/disconnect.
func (s *Server) SetClientListListener(fn func()) {
	if s == nil {
		return
	}
	if fn == nil {
		s.clientListListener.Store((func())(nil))
		return
	}
	s.clientListListener.Store(fn)
}

// ListClientCallsigns returns a sorted snapshot of connected client callsigns.
func (s *Server) ListClientCallsigns() []string {
	if s == nil {
		return nil
	}
	s.clientsMutex.RLock()
	calls := make([]string, 0, len(s.clients))
	for call := range s.clients {
		calls = append(calls, call)
	}
	s.clientsMutex.RUnlock()
	sort.Strings(calls)
	return calls
}

func (s *Server) notifyClientListChange() {
	if s == nil {
		return
	}
	if v := s.clientListListener.Load(); v != nil {
		if fn, ok := v.(func()); ok && fn != nil {
			fn()
		}
	}
}

func (s *Server) observeEnqueueLatency(d time.Duration) {
	if s == nil {
		return
	}
	s.latency.enqueue.Observe(d)
}

func (s *Server) observeFirstByteLatency(d time.Duration) {
	if s == nil {
		return
	}
	s.latency.firstByte.Observe(d)
}

func (s *Server) observeWriteStallLatency(d time.Duration) {
	if s == nil {
		return
	}
	s.latency.writeStall.Observe(d)
}

// LatencySnapshots returns p50/p99 snapshots for enqueue, first byte, and write stall.
func (s *Server) LatencySnapshots() (enqueue, firstByte, writeStall LatencySnapshot) {
	if s == nil {
		return LatencySnapshot{}, LatencySnapshot{}, LatencySnapshot{}
	}
	return s.latency.snapshot()
}

// Stop shuts down the telnet server
func (s *Server) Stop() {
	log.Println("Stopping telnet server...")
	close(s.shutdown)
	if s.listener != nil {
		s.listener.Close()
	}

	// Disconnect all clients
	s.clientsMutex.Lock()
	for _, client := range s.clients {
		client.close("server shutdown")
	}
	s.clientsMutex.Unlock()
}

// SendRaw sends raw bytes to the client
func (c *Client) SendRaw(data []byte) error {
	return c.enqueueControl(controlMessage{raw: data})
}

// Send writes a message to the client with proper line endings
func (c *Client) Send(message string) error {
	return c.enqueueControl(controlMessage{line: message})
}

// SendAndClose enqueues a control message and closes the connection after it is written.
func (c *Client) SendAndClose(message string) error {
	if strings.TrimSpace(message) == "" {
		c.close("send and close")
		return nil
	}
	return c.enqueueControl(controlMessage{line: message, closeAfter: true})
}

// ReadLine reads a single logical line from the telnet client while enforcing
// three invariants:
//  1. Telnet IAC negotiations are consumed without leaking into user input,
//     including subnegotiation payloads (IAC SB ... IAC SE).
//  2. User-supplied characters are bounded to maxLen bytes to prevent
//     unbounded growth (e.g., 32 bytes for login, 128 for commands).
//  3. Only the whitelisted character set (letters, digits, space, '/', '#',
//     '@', and '-') is accepted. Command contexts can optionally allow commas,
//     wildcards (*), and the '?' confidence glyph so filter commands retain
//     their legacy syntax. Any other
//     character is immediately rejected, logged, and returned as an error so
//     the caller can tear down the session before state is mutated.
//
// The CRLF terminator is always allowed: '\r' ends the line and a following
// '\n' (or NUL) is consumed per RFC 854. Editing controls are handled inline:
// BS/DEL remove one byte, Ctrl+U clears the line, and Ctrl+W removes the last
// word. maxLen is measured in bytes because telnet input is ASCII-oriented.
func (c *Client) ReadLine(maxLen int, context string, allowComma, allowWildcard, allowConfidence, allowDot bool) (string, error) {
	if maxLen <= 0 {
		maxLen = defaultCommandLineLimit
	}
	if context == "" {
		context = "command"
	}

	var line []byte

	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return "", err
		}

		// Consume the LF/NUL byte that may follow a CR terminator (RFC 854).
		if c.skipNextEOL {
			c.skipNextEOL = false
			if b == '\n' || b == 0x00 {
				continue
			}
		}

		// Always consume telnet IAC sequences so negotiation bytes never reach
		// the input validator. This keeps behavior consistent across transports.
		if b == IAC {
			if err := c.consumeIACSequence(); err != nil {
				return "", err
			}
			continue
		}

		// End of line once LF is observed.
		if b == '\n' {
			if c.echoInput {
				if err := c.writeRaw([]byte("\r\n")); err != nil {
					return "", err
				}
			}
			break
		}
		if b == '\r' {
			if c.echoInput {
				if err := c.writeRaw([]byte("\r\n")); err != nil {
					return "", err
				}
			}
			c.skipNextEOL = true
			break
		}

		switch b {
		case 0x08, 0x7f: // BS or DEL
			if len(line) > 0 {
				line = line[:len(line)-1]
				if err := c.echoErase(1); err != nil {
					return "", err
				}
			}
			continue
		case 0x15: // Ctrl+U (line kill)
			if len(line) > 0 {
				erased := len(line)
				line = line[:0]
				if err := c.echoErase(erased); err != nil {
					return "", err
				}
			}
			continue
		case 0x17: // Ctrl+W (word erase)
			erased := wordEraseCount(line)
			if erased > 0 {
				line = line[:len(line)-erased]
				if err := c.echoErase(erased); err != nil {
					return "", err
				}
			}
			continue
		}

		if len(line) >= maxLen {
			c.logRejectedInput(context, fmt.Sprintf("exceeded %d-byte limit", maxLen))
			allowed := allowedCharacterList(allowComma, allowWildcard, allowConfidence, allowDot)
			return "", newInputTooLongError(context, maxLen, allowed)
		}
		if !isAllowedInputByte(b, allowComma, allowWildcard, allowConfidence, allowDot) {
			c.logRejectedInput(context, fmt.Sprintf("forbidden byte 0x%02X", b))
			allowed := allowedCharacterList(allowComma, allowWildcard, allowConfidence, allowDot)
			return "", newInputInvalidCharError(context, maxLen, allowed, b)
		}

		normalized := b
		if normalized >= 'a' && normalized <= 'z' {
			normalized -= 'a' - 'A'
		}
		if c.echoInput {
			var echoBuf [1]byte
			echoBuf[0] = normalized
			if err := c.writeRaw(echoBuf[:]); err != nil {
				return "", err
			}
		}
		line = append(line, normalized)
	}

	return string(line), nil
}

// consumeIACSequence drains a single telnet IAC sequence. Data bytes embedded
// in negotiations are discarded so they cannot trip input validation.
func (c *Client) consumeIACSequence() error {
	cmd, err := c.reader.ReadByte()
	if err != nil {
		return err
	}
	switch cmd {
	case IAC:
		// Escaped 0xFF byte; ignore because telnet input is ASCII-only here.
		return nil
	case DO, DONT, WILL, WONT:
		_, err = c.reader.ReadByte()
		return err
	case SB:
		return c.consumeSubnegotiation()
	default:
		// Single-byte command; ignore.
		return nil
	}
}

// consumeSubnegotiation drains bytes until IAC SE, honoring IAC escapes.
func (c *Client) consumeSubnegotiation() error {
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		if b != IAC {
			continue
		}
		next, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		if next == IAC {
			continue
		}
		if next == SE {
			return nil
		}
		// Ignore unexpected bytes and continue scanning for IAC SE.
	}
}

func (c *Client) echoErase(count int) error {
	if !c.echoInput || count <= 0 {
		return nil
	}
	return c.writeRaw([]byte(strings.Repeat("\b \b", count)))
}

func wordEraseCount(line []byte) int {
	if len(line) == 0 {
		return 0
	}
	i := len(line)
	for i > 0 && line[i-1] == ' ' {
		i--
	}
	j := i
	for j > 0 && line[j-1] != ' ' {
		j--
	}
	return len(line) - j
}

// isAllowedInputByte reports whether the byte is part of the strict ingress
// safe list (letters, digits, space, '/', '#', '@', '-'). When allowComma is
// true, comma is also accepted to preserve legacy comma-delimited filter syntax.
// allowDot permits '.' for numeric inputs and spot comments. CRLF is handled
// separately by ReadLine.
func newInputTooLongError(context string, maxLen int, allowed string) error {
	return &InputValidationError{
		reason:  fmt.Sprintf("%s input exceeds %d-byte limit", context, maxLen),
		context: context,
		kind:    inputErrorTooLong,
		maxLen:  maxLen,
		allowed: allowed,
	}
}

func newInputInvalidCharError(context string, maxLen int, allowed string, b byte) error {
	return &InputValidationError{
		reason:  fmt.Sprintf("%s input contains forbidden byte 0x%02X", context, b),
		context: context,
		kind:    inputErrorInvalidChar,
		maxLen:  maxLen,
		allowed: allowed,
	}
}

func isAllowedInputByte(b byte, allowComma, allowWildcard, allowConfidence, allowDot bool) bool {
	switch {
	case b >= 'A' && b <= 'Z':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == ' ':
		return true
	case b == '/':
		return true
	case b == '#':
		return true
	case b == '@':
		return true
	case b == '-':
		return true
	case allowDot && b == '.':
		return true
	case allowComma && b == ',':
		return true
	case allowWildcard && b == '*':
		return true
	case allowConfidence && b == '?':
		return true
	default:
		return false
	}
}

func allowedCharacterList(allowComma, allowWildcard, allowConfidence, allowDot bool) string {
	base := "letters, numbers, space, '/', '#', '@', '-'"
	if allowComma {
		base += ", ','"
	}
	if allowWildcard {
		base += ", '*'"
	}
	if allowConfidence {
		base += ", '?'"
	}
	if allowDot {
		base += ", '.'"
	}
	return base
}

func spotterIP(address string) string {
	if address == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return strings.TrimSpace(address)
	}
	return host
}

func friendlyContextLabel(context string) string {
	context = strings.TrimSpace(context)
	if context == "" {
		return "Input"
	}
	if len(context) == 1 {
		return strings.ToUpper(context)
	}
	return strings.ToUpper(context[:1]) + context[1:]
}

// logRejectedInput emits a consistent, high-signal log entry whenever the
// ingress guardrail rejects input. This makes debugging user issues easier and
// provides an audit trail when a hostile client repeatedly violates the
// policy. The helper deliberately prefers the callsign when known, falling
// back to the remote address prior to login.
func (c *Client) logRejectedInput(context, reason string) {
	id := strings.TrimSpace(c.callsign)
	if id == "" {
		id = c.address
	}
	log.Printf("Rejected %s input from %s: %s", context, id, reason)
}
