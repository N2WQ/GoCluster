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
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
)

// Server represents a multi-client telnet server for DX Cluster connections.
//
// The server maintains a map of connected clients and broadcasts spots to all clients
// in real-time. Each client has its own goroutine for handling commands and receiving spots.
//
// Fields:
//   - port: TCP port to listen on (typically 7300)
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
	port              int                  // TCP port to listen on
	welcomeMessage    string               // Welcome message for new connections
	maxConnections    int                  // Maximum concurrent client connections
	duplicateLoginMsg string               // Message sent to evicted duplicate session
	listener          net.Listener         // TCP listener
	clients           map[string]*Client   // Map of callsign → Client
	clientsMutex      sync.RWMutex         // Protects clients map
	shutdown          chan struct{}        // Shutdown coordination channel
	broadcast         chan *spot.Spot      // Broadcast channel for spots (buffered, configurable)
	broadcastWorkers  int                  // Number of goroutines delivering spots
	workerQueues      []chan *broadcastJob // Per-worker job queues
	workerQueueSize   int                  // Capacity of each worker's queue
	metrics           broadcastMetrics     // Broadcast metrics counters
	processor         *commands.Processor  // Command processor for user commands
	skipHandshake     bool                 // When true, omit Telnet IAC negotiation
	clientBufferSize  int                  // Per-client spot channel capacity
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
	conn      net.Conn        // TCP connection to client
	reader    *bufio.Reader   // Buffered reader for client input
	writer    *bufio.Writer   // Buffered writer for client output
	callsign  string          // Client's amateur radio callsign
	connected time.Time       // Timestamp when client connected
	address   string          // Client's IP address
	spotChan  chan *spot.Spot // Buffered channel for spot delivery (configurable capacity)
	filter    *filter.Filter  // Personal spot filter (band, mode, callsign)
}

func (c *Client) saveFilter() error {
	if c == nil || c.filter == nil {
		return nil
	}
	callsign := strings.TrimSpace(c.callsign)
	if callsign == "" {
		return nil
	}
	if err := filter.SaveUserFilter(callsign, c.filter); err != nil {
		log.Printf("Warning: failed to save filter for %s: %v", callsign, err)
		return err
	}
	log.Printf("Saved filter for %s", callsign)
	return nil
}

type broadcastJob struct {
	spot    *spot.Spot
	clients []*Client
}

type broadcastMetrics struct {
	queueDrops  uint64
	clientDrops uint64
}

func (m *broadcastMetrics) snapshot() (queueDrops, clientDrops uint64) {
	queueDrops = atomic.LoadUint64(&m.queueDrops)
	clientDrops = atomic.LoadUint64(&m.clientDrops)
	return
}

func shouldLogQueueDrop(total uint64) bool {
	return total == 1 || total%100 == 0
}

// Telnet protocol IAC (Interpret As Command) constants.
//
// These constants are used for telnet protocol negotiation:
//   - IAC: Introduces a telnet command sequence
//   - DO/DONT: Request client to enable/disable an option
//   - WILL/WONT: Client agrees/refuses to enable an option
//
// The server sends these sequences to negotiate terminal settings
// and handle special characters properly.
const (
	IAC  = 255 // Interpret As Command - starts telnet command sequence
	DONT = 254 // Request client to disable an option
	DO   = 253 // Request client to enable an option
	WONT = 252 // Client refuses to enable an option
	WILL = 251 // Client agrees to enable an option
)

const (
	defaultBroadcastQueueSize    = 2048
	defaultClientBufferSize      = 128
	defaultWorkerQueueSize       = 128
	defaultDuplicateLoginMessage = "Another login for your callsign connected. This session is being closed (multiple logins are not allowed)."
)

const (
	setFilterUsageMsg   = "Usage: SET/FILTER BAND <band>[,<band>...] | SET/FILTER MODE <mode>[,<mode>...] | SET/FILTER CALL <pattern> | SET/FILTER CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | SET/FILTER BEACON\n"
	unsetFilterUsageMsg = "Usage: UNSET/FILTER ALL | UNSET/FILTER BAND <band>[,<band>...] | UNSET/FILTER MODE <mode>[,<mode>...] | UNSET/FILTER CALL | UNSET/FILTER CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | UNSET/FILTER BEACON\n"
)

// ServerOptions configures the telnet server instance.
type ServerOptions struct {
	Port              int
	WelcomeMessage    string
	DuplicateLoginMsg string
	MaxConnections    int
	BroadcastWorkers  int
	BroadcastQueue    int
	WorkerQueue       int
	ClientBuffer      int
	SkipHandshake     bool
}

// NewServer creates a new telnet server
func NewServer(opts ServerOptions, processor *commands.Processor) *Server {
	config := normalizeServerOptions(opts)
	return &Server{
		port:              config.Port,
		welcomeMessage:    config.WelcomeMessage,
		maxConnections:    config.MaxConnections,
		duplicateLoginMsg: config.DuplicateLoginMsg,
		clients:           make(map[string]*Client),
		shutdown:          make(chan struct{}),
		broadcast:         make(chan *spot.Spot, config.BroadcastQueue),
		broadcastWorkers:  config.BroadcastWorkers,
		workerQueueSize:   config.WorkerQueue,
		clientBufferSize:  config.ClientBuffer,
		skipHandshake:     config.SkipHandshake,
		processor:         processor,
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
	if strings.TrimSpace(config.DuplicateLoginMsg) == "" {
		config.DuplicateLoginMsg = defaultDuplicateLoginMessage
	}
	return config
}

// Start begins listening for telnet connections
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start telnet server: %w", err)
	}

	s.listener = listener
	log.Printf("Telnet server listening on port %d", s.port)

	// Prepare worker pool before handling spots
	s.startWorkerPool()

	// Start broadcast handler
	go s.handleBroadcasts()

	// Accept connections in a goroutine
	go s.acceptConnections()

	return nil
}

// handleBroadcasts sends spots to all connected clients
func (s *Server) handleBroadcasts() {
	for {
		select {
		case <-s.shutdown:
			return
		case spot := <-s.broadcast:
			s.broadcastSpot(spot)
		}
	}
}

// BroadcastSpot sends a spot to all connected clients
func (s *Server) BroadcastSpot(spot *spot.Spot) {
	select {
	case s.broadcast <- spot:
	default:
		drops := atomic.AddUint64(&s.metrics.queueDrops, 1)
		if shouldLogQueueDrop(drops) {
			log.Printf("Broadcast channel full (%d/%d buffered), dropping spot (total queue drops=%d)", len(s.broadcast), cap(s.broadcast), drops)
		}
	}
}

// broadcastSpot segments clients into shards and enqueues jobs for workers
func (s *Server) broadcastSpot(spot *spot.Spot) {
	shards := s.snapshotClientShards()
	s.dispatchSpotToWorkers(spot, shards)
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

func (s *Server) dispatchSpotToWorkers(spot *spot.Spot, shards [][]*Client) {
	for i, clients := range shards {
		if len(clients) == 0 {
			continue
		}
		job := &broadcastJob{spot: spot, clients: clients}
		select {
		case s.workerQueues[i] <- job:
		default:
			drops := atomic.AddUint64(&s.metrics.queueDrops, 1)
			if shouldLogQueueDrop(drops) {
				log.Printf("Worker %d queue full (%d pending jobs), dropping %d-client shard (total queue drops=%d)", i, len(s.workerQueues[i]), len(clients), drops)
			}
		}
	}
}

func (s *Server) snapshotClientShards() [][]*Client {
	workers := s.broadcastWorkers
	if workers <= 0 {
		workers = 1
	}
	shards := make([][]*Client, workers)
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	idx := 0
	for _, client := range s.clients {
		shard := idx % workers
		shards[shard] = append(shards[shard], client)
		idx++
	}
	return shards
}

func (s *Server) broadcastWorker(id int, jobs <-chan *broadcastJob) {
	log.Printf("Broadcast worker %d started", id)
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

func (s *Server) deliverJob(job *broadcastJob) {
	for _, client := range job.clients {
		if !client.filter.Matches(job.spot) {
			continue
		}
		select {
		case client.spotChan <- job.spot:
		default:
			drops := atomic.AddUint64(&s.metrics.clientDrops, 1)
			log.Printf("Client %s spot channel full, dropping spot (total client drops=%d)", client.callsign, drops)
		}
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

func (s *Server) BroadcastMetricSnapshot() (queueDrops, clientDrops uint64) {
	return s.metrics.snapshot()
}

func defaultBroadcastWorkers() int {
	workers := runtime.NumCPU() / 2
	if workers < 2 {
		workers = 2
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

		// Handle this client in a new goroutine
		go s.handleClient(conn)
	}
}

// handleClient manages a single client connection
func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()

	address := conn.RemoteAddr().String()
	log.Printf("New connection from %s", address)

	spotQueueSize := s.clientBufferSize
	if spotQueueSize <= 0 {
		spotQueueSize = defaultClientBufferSize
	}

	// Create client object
	client := &Client{
		conn:      conn,
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		connected: time.Now(),
		address:   address,
		spotChan:  make(chan *spot.Spot, spotQueueSize),
		filter:    filter.NewFilter(), // Start with no filters (accept all)
	}

	// Send telnet options to suppress advanced features unless disabled
	if !s.skipHandshake {
		client.SendRaw([]byte{IAC, DONT, 1}) // Don't echo
		client.SendRaw([]byte{IAC, DONT, 3}) // Don't suppress go-ahead
		client.SendRaw([]byte{IAC, WONT, 1}) // Won't echo
		client.SendRaw([]byte{IAC, WILL, 3}) // Will suppress go-ahead
	}

	// Send welcome message
	client.Send(s.welcomeMessage)
	client.Send("Enter your callsign: ")

	// Read callsign
	callsign, err := client.ReadLine()
	if err != nil {
		log.Printf("Error reading callsign from %s: %v", address, err)
		return
	}

	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if callsign == "" {
		client.Send("Invalid callsign, disconnecting.\n")
		log.Printf("Client %s provided empty callsign", address)
		return
	}
	client.callsign = callsign
	log.Printf("Client %s logged in as %s", address, client.callsign)

	// Attempt to load saved filter for this callsign; fallback to new Filter
	if f, err := filter.LoadUserFilter(client.callsign); err == nil {
		client.filter = f
		log.Printf("Loaded saved filter for %s", client.callsign)
	} else {
		client.filter = filter.NewFilter()
		if errors.Is(err, os.ErrNotExist) {
			client.saveFilter()
			log.Printf("Created default filter for %s", client.callsign)
		} else {
			// Non-critical load error; log it.
			log.Printf("Warning: failed to load filter for %s: %v", client.callsign, err)
		}
	}

	// Register client
	s.registerClient(client)
	defer s.unregisterClient(client)

	// Send login confirmation
	client.Send(fmt.Sprintf("Hello %s, you are now connected.\n", client.callsign))
	client.Send("Type HELP for available commands.\n")

	// Start spot sender goroutine
	go client.spotSender()

	// Read commands from client
	for {
		line, err := client.ReadLine()
		if err != nil {
			log.Printf("Client %s disconnected: %v", client.callsign, err)
			return
		}

		// Skip empty lines
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Check for filter commands first
		if strings.HasPrefix(strings.ToLower(line), "set/filter") ||
			strings.HasPrefix(strings.ToLower(line), "unset/filter") ||
			strings.HasPrefix(strings.ToLower(line), "show/filter") {
			response := client.handleFilterCommand(line)
			client.Send(response)
			continue
		}

		// Process other commands
		response := s.processor.ProcessCommand(line)

		// Check for disconnect signal
		if response == "BYE" {
			client.Send("73!\n")
			log.Printf("Client %s logged out", client.callsign)
			return
		}

		// Send response
		if response != "" {
			client.Send(response)
		}
	}
}

// handleFilterCommand processes filter-related commands
func (c *Client) handleFilterCommand(cmd string) string {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "Invalid filter command\n"
	}

	command := strings.ToLower(parts[0])

	switch command {
	case "show/filter", "sh/filter":
		// If user asked for modes specifically, show supported modes and enabled state
		if len(parts) > 1 {
			arg := strings.ToLower(parts[1])
			switch arg {
			case "modes":
				var b strings.Builder
				for i, mode := range filter.SupportedModes {
					enabled := "DISABLED"
					if c.filter.AllModes || c.filter.Modes[mode] {
						enabled = "ENABLED"
					}
					b.WriteString(fmt.Sprintf("%s=%s", mode, enabled))
					if i < len(filter.SupportedModes)-1 {
						b.WriteString(", ")
					}
				}
				return fmt.Sprintf("Supported modes: %s\n", b.String())
			case "bands":
				return fmt.Sprintf("Supported bands: %s\n", strings.Join(spot.SupportedBandNames(), ", "))
			case "confidence":
				var b strings.Builder
				for i, symbol := range filter.SupportedConfidenceSymbols {
					enabled := "DISABLED"
					if c.filter.AllConfidence || c.filter.ConfidenceSymbolEnabled(symbol) {
						enabled = "ENABLED"
					}
					b.WriteString(fmt.Sprintf("%s=%s", symbol, enabled))
					if i < len(filter.SupportedConfidenceSymbols)-1 {
						b.WriteString(", ")
					}
				}
				return fmt.Sprintf("Confidence symbols: %s\n", b.String())
			case "beacon":
				status := "ENABLED"
				if !c.filter.BeaconsEnabled() {
					status = "DISABLED"
				}
				return fmt.Sprintf("Beacon spots: %s\n", status)
			}
		}

		return fmt.Sprintf("Current filters: %s\n", c.filter.String())

	case "set/filter":
		if len(parts) < 2 {
			return setFilterUsageMsg
		}

		filterType := strings.ToUpper(parts[1])
		if filterType != "BEACON" && len(parts) < 3 {
			return setFilterUsageMsg
		}
		switch filterType {
		case "BAND":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return setFilterUsageMsg
			}
			if strings.EqualFold(value, "ALL") {
				c.filter.ResetBands()
				c.saveFilter()
				return "All bands enabled\n"
			}
			rawBands := parseBandList(value)
			if len(rawBands) == 0 {
				return setFilterUsageMsg
			}
			normalizedBands := make([]string, 0, len(rawBands))
			seen := make(map[string]bool)
			invalid := make([]string, 0)
			for _, candidate := range rawBands {
				norm := spot.NormalizeBand(candidate)
				if norm == "" || !spot.IsValidBand(norm) {
					invalid = append(invalid, candidate)
					continue
				}
				if !seen[norm] {
					normalizedBands = append(normalizedBands, norm)
					seen[norm] = true
				}
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown band: %s\nSupported bands: %s\n", strings.Join(invalid, ", "), strings.Join(spot.SupportedBandNames(), ", "))
			}
			if len(normalizedBands) == 0 {
				return setFilterUsageMsg
			}
			for _, band := range normalizedBands {
				c.filter.SetBand(band, true)
			}
			c.saveFilter()
			if len(normalizedBands) == 1 {
				return fmt.Sprintf("Filter set: Band %s\n", normalizedBands[0])
			}
			return fmt.Sprintf("Filter set: Bands %s\n", strings.Join(normalizedBands, ", "))
		case "MODE":
			modeArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if strings.EqualFold(modeArgs, "ALL") {
				c.filter.ResetModes()
				c.saveFilter()
				return "All modes enabled\n"
			}
			modes := parseModeList(modeArgs)
			if len(modes) == 0 {
				return "Usage: SET/FILTER MODE <mode>[,<mode>...] (comma or space separated)\n"
			}
			invalid := collectInvalidModes(modes)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", "))
			}
			for _, mode := range modes {
				c.filter.SetMode(mode, true)
			}
			c.saveFilter()
			return fmt.Sprintf("Filter set: Modes %s\n", strings.Join(modes, ", "))
		case "CALL":
			value := strings.ToUpper(parts[2])
			c.filter.AddCallsignPattern(value)
			c.saveFilter()
			return fmt.Sprintf("Filter set: Callsign %s\n", value)
		case "CONFIDENCE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: SET/FILTER CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.filter.ResetConfidence()
				c.saveFilter()
				return "All confidence symbols enabled\n"
			}
			symbols := parseConfidenceList(value)
			if len(symbols) == 0 {
				return "Usage: SET/FILTER CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\n"
			}
			invalid := collectInvalidConfidenceSymbols(symbols)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", "))
			}
			for _, symbol := range symbols {
				c.filter.SetConfidenceSymbol(symbol, true)
			}
			c.saveFilter()
			return fmt.Sprintf("Confidence symbols enabled: %s\n", strings.Join(symbols, ", "))
		case "BEACON":
			c.filter.SetBeaconEnabled(true)
			c.saveFilter()
			return "Beacon spots enabled\n"
		default:
			return "Unknown filter type. Use: BAND, MODE, CALL, CONFIDENCE, or BEACON\n"
		}

	case "unset/filter":
		if len(parts) < 2 {
			return unsetFilterUsageMsg
		}

		filterType := strings.ToUpper(parts[1])

		switch filterType {
		case "ALL":
			c.filter.Reset()
			c.saveFilter()
			return "All filters cleared\n"
		case "BAND":
			bandArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if bandArgs == "" {
				return "Usage: UNSET/FILTER BAND <band>[,<band>...] (comma or space separated, or ALL)\n"
			}
			if strings.EqualFold(bandArgs, "ALL") {
				c.filter.ResetBands()
				c.saveFilter()
				return "Band filters cleared\n"
			}
			bands := parseBandList(bandArgs)
			if len(bands) == 0 {
				return "Usage: UNSET/FILTER BAND <band>[,<band>...] (comma or space separated, or ALL)\n"
			}
			invalid := make([]string, 0)
			normalized := make([]string, 0, len(bands))
			seen := make(map[string]bool)
			for _, b := range bands {
				norm := spot.NormalizeBand(b)
				if norm == "" || !spot.IsValidBand(norm) {
					invalid = append(invalid, b)
					continue
				}
				if !seen[norm] {
					normalized = append(normalized, norm)
					seen[norm] = true
				}
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown band: %s\nSupported bands: %s\n", strings.Join(invalid, ", "), strings.Join(spot.SupportedBandNames(), ", "))
			}
			if len(normalized) == 0 {
				return "Usage: UNSET/FILTER BAND <band>[,<band>...] (comma or space separated, or ALL)\n"
			}
			for _, band := range normalized {
				c.filter.SetBand(band, false)
			}
			c.saveFilter()
			return fmt.Sprintf("Band filters disabled: %s\n", strings.Join(normalized, ", "))
		case "MODE":
			modeArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if modeArgs == "" {
				return "Usage: UNSET/FILTER MODE <mode>[,<mode>...] (comma or space separated, or ALL)\n"
			}
			if strings.EqualFold(modeArgs, "ALL") {
				c.filter.ResetModes()
				c.saveFilter()
				return "Mode filters cleared\n"
			}
			modes := parseModeList(modeArgs)
			if len(modes) == 0 {
				return "Usage: UNSET/FILTER MODE <mode>[,<mode>...] (comma or space separated, or ALL)\n"
			}
			invalid := collectInvalidModes(modes)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", "))
			}
			for _, mode := range modes {
				c.filter.SetMode(mode, false)
			}
			c.saveFilter()
			return fmt.Sprintf("Mode filters disabled: %s\n", strings.Join(modes, ", "))
		case "CALL":
			c.filter.ClearCallsignPatterns()
			c.saveFilter()
			return "Callsign filters cleared\n"
		case "CONFIDENCE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: UNSET/FILTER CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.filter.ResetConfidence()
				c.saveFilter()
				return "Confidence filter cleared\n"
			}
			symbols := parseConfidenceList(value)
			if len(symbols) == 0 {
				return "Usage: UNSET/FILTER CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\n"
			}
			invalid := collectInvalidConfidenceSymbols(symbols)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", "))
			}
			for _, symbol := range symbols {
				c.filter.SetConfidenceSymbol(symbol, false)
			}
			c.saveFilter()
			return fmt.Sprintf("Confidence symbols disabled: %s\n", strings.Join(symbols, ", "))
		case "BEACON":
			c.filter.SetBeaconEnabled(false)
			c.saveFilter()
			return "Beacon spots disabled\n"
		default:
			return "Unknown filter type. Use: ALL, BAND, MODE, CALL, CONFIDENCE, or BEACON\n"
		}

	default:
		return "Unknown filter command\n"
	}
}

// parseBandList normalizes user-supplied band lists so SET/UNSET commands
// share the exact same semantics as their mode counterparts (comma or space
// delimited input, duplicate suppression happens later).
func parseBandList(arg string) []string {
	return splitListValues(arg)
}

// parseModeList converts user input into normalized mode tokens. Band and mode
// commands share this list-handling behavior so `ALL`, comma, and space
// separated values work identically between the two.
func parseModeList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	modes := make([]string, 0, len(values))
	for _, value := range values {
		mode := strings.ToUpper(strings.TrimSpace(value))
		if mode == "" {
			continue
		}
		modes = append(modes, mode)
	}
	return modes
}

func parseConfidenceList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	symbols := make([]string, 0, len(values))
	for _, value := range values {
		symbol := strings.ToUpper(strings.TrimSpace(value))
		if symbol == "" {
			continue
		}
		symbols = append(symbols, symbol)
	}
	return symbols
}

func collectInvalidConfidenceSymbols(symbols []string) []string {
	invalid := make([]string, 0)
	for _, symbol := range symbols {
		if !filter.IsSupportedConfidenceSymbol(symbol) {
			invalid = append(invalid, symbol)
		}
	}
	return invalid
}

// splitListValues turns arbitrary comma/space delimited strings into clean
// tokens. Both band and mode filters rely on this to keep their UX identical.
func splitListValues(arg string) []string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil
	}
	cleaned := strings.ReplaceAll(arg, ",", " ")
	values := strings.Fields(cleaned)
	if len(values) == 0 {
		return nil
	}
	return values
}

func collectInvalidModes(modes []string) []string {
	invalid := make([]string, 0)
	for _, mode := range modes {
		if !filter.IsSupportedMode(mode) {
			invalid = append(invalid, mode)
		}
	}
	return invalid
}

// spotSender sends spots to the client from the spot channel
func (c *Client) spotSender() {
	for spot := range c.spotChan {
		formatted := spot.FormatDXCluster() + "\n"
		err := c.Send(formatted)
		if err != nil {
			log.Printf("Error sending spot to %s: %v", c.callsign, err)
			return
		}
	}
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
	s.clientsMutex.Unlock()

	if evicted != nil {
		msg := strings.TrimSpace(s.duplicateLoginMsg)
		if msg == "" {
			msg = defaultDuplicateLoginMessage
		}
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		_ = evicted.Send(msg)
		evicted.conn.Close()
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
	s.clientsMutex.Unlock()

	client.saveFilter()
	close(client.spotChan)
	log.Printf("Unregistered client: %s (total: %d)", client.callsign, total)
}

// GetClientCount returns the number of connected clients
func (s *Server) GetClientCount() int {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	return len(s.clients)
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
		client.conn.Close()
	}
	s.clientsMutex.Unlock()
}

// SendRaw sends raw bytes to the client
func (c *Client) SendRaw(data []byte) error {
	_, err := c.writer.Write(data)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// Send writes a message to the client with proper line endings
func (c *Client) Send(message string) error {
	// Replace \n with \r\n for proper telnet line endings
	message = strings.ReplaceAll(message, "\n", "\r\n")
	_, err := c.writer.WriteString(message)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// ReadLine reads a line from the client, filtering out telnet control codes
func (c *Client) ReadLine() (string, error) {
	var line []byte

	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return "", err
		}

		// Handle telnet IAC (Interpret As Command) sequences
		if b == IAC {
			// Read next byte
			cmd, err := c.reader.ReadByte()
			if err != nil {
				return "", err
			}

			// If it's DO, DONT, WILL, WONT, read and discard the option byte
			if cmd == DO || cmd == DONT || cmd == WILL || cmd == WONT {
				_, err := c.reader.ReadByte()
				if err != nil {
					return "", err
				}
			}
			// Skip this sequence and continue reading
			continue
		}

		// End of line
		if b == '\n' {
			break
		}

		// Skip carriage return
		if b == '\r' {
			continue
		}

		// Add to line buffer
		line = append(line, b)
	}

	return string(line), nil
}
