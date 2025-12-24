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
	"dxcluster/filter"
	"dxcluster/spot"
)

// Server represents a multi-client telnet server for DX Cluster connections.
//
// The server maintains a map of connected clients and broadcasts spots to all clients
// in real-time. Each client has its own goroutine for handling commands and receiving spots.
//
// Fields:
//   - port: TCP port to listen on (configured via `telnet.port` in `config.yaml`)
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
	greetingTemplate  string               // Post-login greeting with placeholders
	clusterCall       string               // Cluster/node callsign for greeting substitution
	listener          net.Listener         // TCP listener
	clients           map[string]*Client   // Map of callsign → Client
	clientsMutex      sync.RWMutex         // Protects clients map
	shutdown          chan struct{}        // Shutdown coordination channel
	broadcast         chan *spot.Spot      // Broadcast channel for spots (buffered, configurable)
	broadcastWorkers  int                  // Number of goroutines delivering spots
	workerQueues      []chan *broadcastJob // Per-worker job queues
	workerQueueSize   int                  // Capacity of each worker's queue
	batchInterval     time.Duration        // Broadcast batch interval; 0 means immediate
	batchMax          int                  // Max jobs per batch before flush
	metrics           broadcastMetrics     // Broadcast metrics counters
	keepaliveInterval time.Duration        // Optional periodic CRLF to keep idle sessions alive
	clientShards      [][]*Client          // Cached shard layout for broadcasts
	shardsDirty       atomic.Bool          // Flag to rebuild shards on client add/remove
	processor         *commands.Processor  // Command processor for user commands
	skipHandshake     bool                 // When true, omit Telnet IAC negotiation
	clientBufferSize  int                  // Per-client spot channel capacity
	loginLineLimit    int                  // Maximum bytes accepted for login/callsign input
	commandLineLimit  int                  // Maximum bytes accepted for post-login commands
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

	// filterMu guards filter, which is read by telnet broadcast workers while the
	// client session goroutine mutates it in response to PASS/REJECT commands.
	// Without this lock, Go's runtime can terminate the process with:
	// "fatal error: concurrent map read and map write"
	// because Filter contains many maps.
	filterMu  sync.RWMutex
	filter    *filter.Filter // Personal spot filter (band, mode, callsign)
	dropCount uint64         // Count of spots dropped for this client due to backpressure
}

// InputValidationError represents a non-fatal ingress violation (length or character guardrails).
// Returning this error allows the caller to keep the connection open and prompt the user again.
type InputValidationError struct {
	reason      string
	userMessage string
}

func (e *InputValidationError) Error() string {
	return e.reason
}

// UserMessage returns the friendly text that should be sent back to the telnet client.
func (e *InputValidationError) UserMessage() string {
	if e == nil || strings.TrimSpace(e.userMessage) == "" {
		return "Input rejected. Please try again."
	}
	return e.userMessage
}

func (c *Client) saveFilter() error {
	if c == nil || c.filter == nil {
		return nil
	}
	callsign := strings.TrimSpace(c.callsign)
	if callsign == "" {
		return nil
	}
	// Persisting the filter marshals multiple maps; guard with a read lock so it
	// cannot run concurrently with PASS/REJECT updates. Broadcast workers also
	// hold read locks while matching, so persistence does not stall spot delivery.
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	if err := filter.SaveUserFilter(callsign, c.filter); err != nil {
		log.Printf("Warning: failed to save filter for %s: %v", callsign, err)
		return err
	}
	log.Printf("Saved filter for %s", callsign)
	return nil
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

func isFilterCommand(line string) bool {
	lower := strings.ToLower(strings.TrimSpace(line))
	if lower == "" {
		return false
	}
	if strings.HasPrefix(lower, "pass") ||
		strings.HasPrefix(lower, "reject") ||
		strings.HasPrefix(lower, "show filter") {
		return true
	}
	// Catch legacy verbs so we can return the syntax-changed hint.
	return strings.HasPrefix(lower, "set/filter") ||
		strings.HasPrefix(lower, "unset/filter") ||
		strings.HasPrefix(lower, "show/filter") ||
		strings.HasPrefix(lower, "sh/filter")
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
	defaultBroadcastQueueSize     = 2048
	defaultBroadcastBatch         = 512
	defaultBroadcastBatchInterval = 250 * time.Millisecond
	defaultClientBufferSize       = 128
	defaultWorkerQueueSize        = 128
	defaultDuplicateLoginMessage  = "Another login for your callsign connected. This session is being closed (multiple logins are not allowed)."
	defaultSendDeadline           = 2 * time.Second
	defaultLoginLineLimit         = 32
	defaultCommandLineLimit       = 128
)

const (
	passFilterUsageMsg    = "Usage: PASS <type> ...\nPASS BAND <band>[,<band>...] | PASS MODE <mode>[,<mode>...] | PASS SOURCE <HUMAN|SKIMMER|ALL> | PASS DXCALL <pattern> | PASS DECALL <pattern> | PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | PASS BEACON | PASS DXGRID2 <grid>[,<grid>...] (two characters or ALL) | PASS DEGRID2 <grid>[,<grid>...] (two characters or ALL) | PASS DXCONT <cont>[,<cont>...] | PASS DECONT <cont>[,<cont>...] | PASS DXZONE <zone>[,<zone>...] | PASS DEZONE <zone>[,<zone>...] | PASS DXDXCC <code>[,<code>...] | PASS DEDXCC <code>[,<code>...] (PASS = allow list; clears block-all). Supported modes include: CW, LSB, USB, RTTY, FT4, FT8, MSK144, PSK31.\nType HELP for usage.\n"
	rejectFilterUsageMsg  = "Usage: REJECT <type> ...\nREJECT ALL | REJECT BAND <band>[,<band>...] | REJECT MODE <mode>[,<mode>...] | REJECT SOURCE <HUMAN|SKIMMER> | REJECT DXCALL | REJECT DECALL | REJECT CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL) | REJECT BEACON | REJECT DXGRID2 <grid>[,<grid>...] (two characters or ALL) | REJECT DEGRID2 <grid>[,<grid>...] (two characters or ALL) | REJECT DXCONT <cont>[,<cont>...] | REJECT DECONT <cont>[,<cont>...] | REJECT DXZONE <zone>[,<zone>...] | REJECT DEZONE <zone>[,<zone>...] | REJECT DXDXCC <code>[,<code>...] | REJECT DEDXCC <code>[,<code>...] (REJECT = block list; ALL resets to defaults). Supported modes include: CW, LSB, USB, RTTY, FT4, FT8, MSK144, PSK31.\nType HELP for usage.\n"
	legacyFilterSyntaxMsg = "Filter syntax changed: use PASS/REJECT/SHOW FILTER.\nType HELP for usage.\n"
)

// ServerOptions configures the telnet server instance.
type ServerOptions struct {
	Port                   int
	WelcomeMessage         string
	DuplicateLoginMsg      string
	LoginGreeting          string
	ClusterCall            string
	MaxConnections         int
	BroadcastWorkers       int
	BroadcastQueue         int
	WorkerQueue            int
	ClientBuffer           int
	BroadcastBatchInterval time.Duration
	KeepaliveSeconds       int
	SkipHandshake          bool
	LoginLineLimit         int
	CommandLineLimit       int
}

// NewServer creates a new telnet server
func NewServer(opts ServerOptions, processor *commands.Processor) *Server {
	config := normalizeServerOptions(opts)
	return &Server{
		port:              config.Port,
		welcomeMessage:    config.WelcomeMessage,
		maxConnections:    config.MaxConnections,
		duplicateLoginMsg: config.DuplicateLoginMsg,
		greetingTemplate:  config.LoginGreeting,
		clusterCall:       config.ClusterCall,
		clients:           make(map[string]*Client),
		shutdown:          make(chan struct{}),
		broadcast:         make(chan *spot.Spot, config.BroadcastQueue),
		broadcastWorkers:  config.BroadcastWorkers,
		workerQueueSize:   config.WorkerQueue,
		batchInterval:     config.BroadcastBatchInterval,
		batchMax:          defaultBroadcastBatch,
		keepaliveInterval: time.Duration(config.KeepaliveSeconds) * time.Second,
		clientBufferSize:  config.ClientBuffer,
		skipHandshake:     config.SkipHandshake,
		processor:         processor,
		loginLineLimit:    config.LoginLineLimit,
		commandLineLimit:  config.CommandLineLimit,
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
	if config.BroadcastBatchInterval <= 0 {
		config.BroadcastBatchInterval = defaultBroadcastBatchInterval
	}
	if strings.TrimSpace(config.DuplicateLoginMsg) == "" {
		config.DuplicateLoginMsg = defaultDuplicateLoginMessage
	}
	if strings.TrimSpace(config.LoginGreeting) == "" {
		config.LoginGreeting = "Hello <CALL>, you are now connected to <CLUSTER>. Type HELP for available commands."
	}
	if strings.TrimSpace(config.ClusterCall) == "" {
		config.ClusterCall = "DXC"
	}
	if config.LoginLineLimit <= 0 {
		config.LoginLineLimit = defaultLoginLineLimit
	}
	if config.CommandLineLimit <= 0 {
		config.CommandLineLimit = defaultCommandLineLimit
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
		case spot := <-s.broadcast:
			s.broadcastSpot(spot)
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

// broadcastSpot segments clients into shards and enqueues jobs for workers
func (s *Server) broadcastSpot(spot *spot.Spot) {
	shards := s.cachedClientShards()
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

func (s *Server) deliverJob(job *broadcastJob) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("telnet: panic delivering broadcast job: %v", r)
		}
	}()
	for _, client := range job.clients {
		// Always deliver spots where the DX callsign matches the logged-in user,
		// regardless of filters (self-spots should not be filtered out).
		if !strings.EqualFold(job.spot.DXCall, client.callsign) {
			client.filterMu.RLock()
			matches := client.filter.Matches(job.spot)
			client.filterMu.RUnlock()
			if !matches {
				continue
			}
		}
		select {
		case client.spotChan <- job.spot:
		default:
			drops := atomic.AddUint64(&s.metrics.clientDrops, 1)
			clientDrops := atomic.AddUint64(&client.dropCount, 1)
			if shouldLogQueueDrop(drops) {
				log.Printf("Client %s spot channel full, dropping spot (client drops=%d total=%d)", client.callsign, clientDrops, drops)
			}
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
	now := time.Now().UTC()
	client.Send(applyWelcomeTokens(s.welcomeMessage, now))
	client.Send("\r\nEnter your callsign:\r\n")

	var callsign string
	for {
		// Read callsign with tight guard rails so a single telnet client cannot
		// consume unbounded memory or smuggle control characters during login. The
		// limit is configurable but defaults to 32 bytes which covers every valid
		// callsign, including suffixes such as /QRP or /MM.
		line, err := client.ReadLine(s.loginLineLimit, "login", false, false, false)
		if err != nil {
			var inputErr *InputValidationError
			if errors.As(err, &inputErr) {
				client.Send(inputErr.UserMessage() + "\n")
				client.Send("Enter your callsign:\r\n")
				continue
			}
			log.Printf("Error reading callsign from %s: %v", address, err)
			return
		}

		line = strings.ToUpper(strings.TrimSpace(line))
		if line == "" {
			client.Send("Callsign cannot be empty. Please try again.\n")
			client.Send("Enter your callsign:\r\n")
			continue
		}
		callsign = line
		break
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
	greeting := formatGreeting(s.greetingTemplate, client.callsign, s.clusterCall)
	if strings.TrimSpace(greeting) == "" {
		greeting = fmt.Sprintf("Hello %s, you are now connected.", client.callsign)
	}
	client.Send(greeting + "\n")

	// Start spot sender goroutine
	go client.spotSender()

	// Read commands from client
	for {
		// Commands use the more relaxed limit because filter manipulation can
		// legitimately include several tokens. The limit is still kept small
		// (default 128 bytes) to keep parsing cheap and predictable.
		line, err := client.ReadLine(s.commandLineLimit, "command", true, true, true)
		if err != nil {
			var inputErr *InputValidationError
			if errors.As(err, &inputErr) {
				client.Send(inputErr.UserMessage() + "\n")
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

		// Check for filter commands first
		if isFilterCommand(line) {
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
		return "Invalid filter command. Type HELP for usage.\n"
	}

	command := strings.ToLower(parts[0])

	// Explicitly catch legacy verbs to provide a clearer error.
	switch command {
	case "set/filter", "unset/filter", "show/filter", "sh/filter":
		return legacyFilterSyntaxMsg
	}

	switch command {
	case "show":
		if len(parts) < 2 || strings.ToLower(parts[1]) != "filter" {
			return "Invalid filter command. Type HELP for usage.\n"
		}
		// If user asked for modes specifically, show supported modes and enabled state
		if len(parts) > 2 {
			arg := strings.ToLower(parts[2])
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
			case "dxcont":
				return fmt.Sprintf("DX continents: %s\n", formatAllowBlockStrings(c.filter.AllDXContinents, c.filter.BlockAllDXContinents, c.filter.DXContinents, c.filter.BlockDXContinents))
			case "decont":
				return fmt.Sprintf("DE continents: %s\n", formatAllowBlockStrings(c.filter.AllDEContinents, c.filter.BlockAllDEContinents, c.filter.DEContinents, c.filter.BlockDEContinents))
			case "dxzone":
				return fmt.Sprintf("DX zones: %s\n", formatAllowBlockInts(c.filter.AllDXZones, c.filter.BlockAllDXZones, c.filter.DXZones, c.filter.BlockDXZones))
			case "dezone":
				return fmt.Sprintf("DE zones: %s\n", formatAllowBlockInts(c.filter.AllDEZones, c.filter.BlockAllDEZones, c.filter.DEZones, c.filter.BlockDEZones))
			case "dxgrid2":
				return fmt.Sprintf("DX 2-character grids: %s\n", formatAllowBlockStrings(c.filter.AllDXGrid2, c.filter.BlockAllDXGrid2, c.filter.DXGrid2Prefixes, c.filter.BlockDXGrid2))
			case "degrid2":
				return fmt.Sprintf("DE 2-character grids: %s\n", formatAllowBlockStrings(c.filter.AllDEGrid2, c.filter.BlockAllDEGrid2, c.filter.DEGrid2Prefixes, c.filter.BlockDEGrid2))
			case "dxdxcc":
				return fmt.Sprintf("DX DXCC: %s\n", formatAllowBlockInts(c.filter.AllDXDXCC, c.filter.BlockAllDXDXCC, c.filter.DXDXCC, c.filter.BlockDXDXCC))
			case "dedxcc":
				return fmt.Sprintf("DE DXCC: %s\n", formatAllowBlockInts(c.filter.AllDEDXCC, c.filter.BlockAllDEDXCC, c.filter.DEDXCC, c.filter.BlockDEDXCC))
			}
		}

		return fmt.Sprintf("Current filters: %s\n", c.filter.String())

	case "pass":
		if len(parts) < 2 {
			return passFilterUsageMsg
		}

		filterType := strings.ToUpper(parts[1])
		if filterType != "BEACON" && len(parts) < 3 {
			return passFilterUsageMsg
		}
		switch filterType {
		case "BAND":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return passFilterUsageMsg
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetBands()
				})
				c.saveFilter()
				return "All bands enabled\n"
			}
			rawBands := parseBandList(value)
			if len(rawBands) == 0 {
				return passFilterUsageMsg
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
				return passFilterUsageMsg
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, band := range normalizedBands {
					f.SetBand(band, true)
				}
			})
			c.saveFilter()
			if len(normalizedBands) == 1 {
				return fmt.Sprintf("Filter set: Band %s\n", normalizedBands[0])
			}
			return fmt.Sprintf("Filter set: Bands %s\n", strings.Join(normalizedBands, ", "))
		case "MODE":
			modeArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if strings.EqualFold(modeArgs, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetModes()
				})
				c.saveFilter()
				return "All modes enabled\n"
			}
			modes := parseModeList(modeArgs)
			if len(modes) == 0 {
				return "Usage: PASS MODE <mode>[,<mode>...] (comma or space separated)\nType HELP for usage.\n"
			}
			invalid := collectInvalidModes(modes)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, mode := range modes {
					f.SetMode(mode, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: Modes %s\n", strings.Join(modes, ", "))
		case "SOURCE":
			value := strings.ToUpper(strings.TrimSpace(strings.Join(parts[2:], " ")))
			if value == "" {
				return "Usage: PASS SOURCE <HUMAN|SKIMMER|ALL>\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetSources()
				})
				c.saveFilter()
				return "Source filtering disabled\n"
			}
			if !filter.IsSupportedSource(value) {
				return fmt.Sprintf("Unknown source: %s\nValid sources: %s\n", value, strings.Join(filter.SupportedSources, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				f.SetSource(value, true)
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: Source %s\n", value)
		case "DXCALL":
			value := strings.ToUpper(parts[2])
			c.updateFilter(func(f *filter.Filter) {
				f.AddDXCallsignPattern(value)
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DX callsign %s\n", value)
		case "DECALL":
			value := strings.ToUpper(parts[2])
			c.updateFilter(func(f *filter.Filter) {
				f.AddDECallsignPattern(value)
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DE callsign %s\n", value)
		case "CONFIDENCE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetConfidence()
				})
				c.saveFilter()
				return "All confidence symbols enabled\n"
			}
			symbols := parseConfidenceList(value)
			if len(symbols) == 0 {
				return "Usage: PASS CONFIDENCE <symbol>[,<symbol>...] (symbols: ?,S,C,P,V,B or ALL)\nType HELP for usage.\n"
			}
			invalid := collectInvalidConfidenceSymbols(symbols)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, symbol := range symbols {
					f.SetConfidenceSymbol(symbol, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Confidence symbols enabled: %s\n", strings.Join(symbols, ", "))
		case "BEACON":
			c.updateFilter(func(f *filter.Filter) {
				f.SetBeaconEnabled(true)
			})
			c.saveFilter()
			return "Beacon spots enabled\n"
		case "DXCONT":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DXCONT <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXContinents()
				})
				c.saveFilter()
				return "All DX continents enabled\n"
			}
			continents := parseContinentList(value)
			if len(continents) == 0 {
				return "Usage: PASS DXCONT <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
				return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, cont := range continents {
					f.SetDXContinent(cont, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DX continents %s\n", strings.Join(continents, ", "))
		case "DECONT":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DECONT <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEContinents()
				})
				c.saveFilter()
				return "All DE continents enabled\n"
			}
			continents := parseContinentList(value)
			if len(continents) == 0 {
				return "Usage: PASS DECONT <cont>[,<cont>...] (continents: AF, AN, AS, EU, NA, OC, SA, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
				return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, cont := range continents {
					f.SetDEContinent(cont, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DE continents %s\n", strings.Join(continents, ", "))
		case "DXZONE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DXZONE <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXZones()
				})
				c.saveFilter()
				return "All DX zones enabled\n"
			}
			zones := parseZoneList(value)
			if len(zones) == 0 {
				return "Usage: PASS DXZONE <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidZones(zones); len(invalid) > 0 {
				return fmt.Sprintf("Unknown CQ zone: %v\nValid zones: %d-%d\n", invalid, filter.MinCQZone(), filter.MaxCQZone())
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, zone := range zones {
					f.SetDXZone(zone, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DX zones %s\n", joinZones(zones))
		case "DEZONE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DEZONE <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEZones()
				})
				c.saveFilter()
				return "All DE zones enabled\n"
			}
			zones := parseZoneList(value)
			if len(zones) == 0 {
				return "Usage: PASS DEZONE <zone>[,<zone>...] (1-40, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidZones(zones); len(invalid) > 0 {
				return fmt.Sprintf("Unknown CQ zone: %v\nValid zones: %d-%d\n", invalid, filter.MinCQZone(), filter.MaxCQZone())
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, zone := range zones {
					f.SetDEZone(zone, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DE zones %s\n", joinZones(zones))
		case "DXDXCC":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DXDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXDXCC()
				})
				c.saveFilter()
				return "All DX DXCCs enabled\n"
			}
			codes, invalid := parseDXCCList(value)
			if len(codes) == 0 {
				return "Usage: PASS DXDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, code := range codes {
					f.SetDXDXCC(code, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DX DXCC %s\n", joinZones(codes))
		case "DEDXCC":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DEDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEDXCC()
				})
				c.saveFilter()
				return "All DE DXCCs enabled\n"
			}
			codes, invalid := parseDXCCList(value)
			if len(codes) == 0 {
				return "Usage: PASS DEDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, code := range codes {
					f.SetDEDXCC(code, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DE DXCC %s\n", joinZones(codes))
		case "DXGRID2":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DXGRID2 <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXGrid2()
				})
				c.saveFilter()
				return "All DX 2-character grids enabled\n"
			}
			gridList, invalidTokens := parseGrid2List(value)
			if len(gridList) == 0 {
				return "Usage: PASS DXGRID2 <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n"
			}
			if len(invalidTokens) > 0 {
				return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, grid := range gridList {
					f.SetDXGrid2Prefix(grid, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DX 2-character grids %s\n", strings.Join(gridList, ", "))
		case "DEGRID2":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: PASS DEGRID2 <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEGrid2()
				})
				c.saveFilter()
				return "All DE 2-character grids enabled\n"
			}
			gridList, invalidTokens := parseGrid2List(value)
			if len(gridList) == 0 {
				return "Usage: PASS DEGRID2 <grid>[,<grid>...] (two characters, or ALL)\nType HELP for usage.\n"
			}
			if len(invalidTokens) > 0 {
				return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, grid := range gridList {
					f.SetDEGrid2Prefix(grid, true)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Filter set: DE 2-character grids %s\n", strings.Join(gridList, ", "))
		default:
			return "Unknown filter type. Use: BAND, MODE, SOURCE, DXCALL, DECALL, CONFIDENCE, BEACON, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE, DXDXCC, or DEDXCC\nType HELP for usage.\n"
		}

	case "reject":
		if len(parts) < 2 {
			return rejectFilterUsageMsg
		}

		filterType := strings.ToUpper(parts[1])

		switch filterType {
		case "ALL":
			c.updateFilter(func(f *filter.Filter) {
				f.Reset()
			})
			c.saveFilter()
			return "All filters cleared\n"
		case "BAND":
			bandArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if bandArgs == "" {
				return "Usage: REJECT BAND <band>[,<band>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(bandArgs, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetBands()
					f.BlockAllBands = true
					f.AllBands = false
				})
				c.saveFilter()
				return "All bands blocked\n"
			}
			bands := parseBandList(bandArgs)
			if len(bands) == 0 {
				return "Usage: REJECT BAND <band>[,<band>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
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
				return "Usage: REJECT BAND <band>[,<band>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, band := range normalized {
					f.SetBand(band, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Band filters disabled: %s\n", strings.Join(normalized, ", "))
		case "MODE":
			modeArgs := strings.TrimSpace(strings.Join(parts[2:], " "))
			if modeArgs == "" {
				return "Usage: REJECT MODE <mode>[,<mode>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(modeArgs, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetModes()
					f.BlockAllModes = true
					f.AllModes = false
				})
				c.saveFilter()
				return "All modes blocked\n"
			}
			modes := parseModeList(modeArgs)
			if len(modes) == 0 {
				return "Usage: REJECT MODE <mode>[,<mode>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			invalid := collectInvalidModes(modes)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown mode: %s\nSupported modes: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedModes, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, mode := range modes {
					f.SetMode(mode, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Mode filters disabled: %s\n", strings.Join(modes, ", "))
		case "SOURCE":
			value := strings.ToUpper(strings.TrimSpace(strings.Join(parts[2:], " ")))
			if value == "" {
				return "Usage: REJECT SOURCE <HUMAN|SKIMMER>\nType HELP for usage.\n"
			}
			if !filter.IsSupportedSource(value) {
				return fmt.Sprintf("Unknown source: %s\nValid sources: %s\n", value, strings.Join(filter.SupportedSources, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				f.SetSource(value, false)
			})
			c.saveFilter()
			return fmt.Sprintf("Source filters disabled: %s\n", value)
		case "DXCALL":
			c.updateFilter(func(f *filter.Filter) {
				f.ClearDXCallsignPatterns()
			})
			c.saveFilter()
			return "DX callsign filters cleared\n"
		case "DECALL":
			c.updateFilter(func(f *filter.Filter) {
				f.ClearDECallsignPatterns()
			})
			c.saveFilter()
			return "DE callsign filters cleared\n"
		case "CONFIDENCE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetConfidence()
					f.BlockAllConfidence = true
					f.AllConfidence = false
				})
				c.saveFilter()
				return "All confidence symbols blocked (non-exempt modes)\n"
			}
			symbols := parseConfidenceList(value)
			if len(symbols) == 0 {
				return "Usage: REJECT CONFIDENCE <symbol>[,<symbol>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			invalid := collectInvalidConfidenceSymbols(symbols)
			if len(invalid) > 0 {
				return fmt.Sprintf("Unknown confidence symbol: %s\nSupported symbols: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedConfidenceSymbols, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, symbol := range symbols {
					f.SetConfidenceSymbol(symbol, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("Confidence symbols disabled: %s\n", strings.Join(symbols, ", "))
		case "BEACON":
			c.updateFilter(func(f *filter.Filter) {
				f.SetBeaconEnabled(false)
			})
			c.saveFilter()
			return "Beacon spots disabled\n"
		case "DXCONT":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DXCONT <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXContinents()
					f.BlockAllDXContinents = true
					f.AllDXContinents = false
				})
				c.saveFilter()
				return "All DX continents blocked\n"
			}
			continents := parseContinentList(value)
			if len(continents) == 0 {
				return "Usage: REJECT DXCONT <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
				return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, cont := range continents {
					f.SetDXContinent(cont, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DX continent filters disabled: %s\n", strings.Join(continents, ", "))
		case "DECONT":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DECONT <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEContinents()
					f.BlockAllDEContinents = true
					f.AllDEContinents = false
				})
				c.saveFilter()
				return "All DE continents blocked\n"
			}
			continents := parseContinentList(value)
			if len(continents) == 0 {
				return "Usage: REJECT DECONT <cont>[,<cont>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidContinents(continents); len(invalid) > 0 {
				return fmt.Sprintf("Unknown continent: %s\nSupported continents: %s\n", strings.Join(invalid, ", "), strings.Join(filter.SupportedContinents, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, cont := range continents {
					f.SetDEContinent(cont, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DE continent filters disabled: %s\n", strings.Join(continents, ", "))
		case "DXZONE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DXZONE <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXZones()
					f.BlockAllDXZones = true
					f.AllDXZones = false
				})
				c.saveFilter()
				return "All DX zones blocked\n"
			}
			zones := parseZoneList(value)
			if len(zones) == 0 {
				return "Usage: REJECT DXZONE <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidZones(zones); len(invalid) > 0 {
				return fmt.Sprintf("Unknown CQ zone: %v\nValid zones: %d-%d\n", invalid, filter.MinCQZone(), filter.MaxCQZone())
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, zone := range zones {
					f.SetDXZone(zone, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DX zone filters disabled: %s\n", joinZones(zones))
		case "DEZONE":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DEZONE <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEZones()
					f.BlockAllDEZones = true
					f.AllDEZones = false
				})
				c.saveFilter()
				return "All DE zones blocked\n"
			}
			zones := parseZoneList(value)
			if len(zones) == 0 {
				return "Usage: REJECT DEZONE <zone>[,<zone>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if invalid := collectInvalidZones(zones); len(invalid) > 0 {
				return fmt.Sprintf("Unknown CQ zone: %v\nValid zones: %d-%d\n", invalid, filter.MinCQZone(), filter.MaxCQZone())
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, zone := range zones {
					f.SetDEZone(zone, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DE zone filters disabled: %s\n", joinZones(zones))
		case "DXDXCC":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DXDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXDXCC()
					f.BlockAllDXDXCC = true
					f.AllDXDXCC = false
				})
				c.saveFilter()
				return "All DX DXCCs blocked\n"
			}
			codes, invalid := parseDXCCList(value)
			if len(codes) == 0 {
				return "Usage: REJECT DXDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, code := range codes {
					f.SetDXDXCC(code, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DX DXCC filters disabled: %s\n", joinZones(codes))
		case "DEDXCC":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DEDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEDXCC()
					f.BlockAllDEDXCC = true
					f.AllDEDXCC = false
				})
				c.saveFilter()
				return "All DE DXCCs blocked\n"
			}
			codes, invalid := parseDXCCList(value)
			if len(codes) == 0 {
				return "Usage: REJECT DEDXCC <code>[,<code>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalid) > 0 {
				return fmt.Sprintf("Invalid DXCC code: %s\n", strings.Join(invalid, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, code := range codes {
					f.SetDEDXCC(code, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DE DXCC filters disabled: %s\n", joinZones(codes))
		case "DXGRID2":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DXGRID2 <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDXGrid2()
					f.BlockAllDXGrid2 = true
					f.AllDXGrid2 = false
				})
				c.saveFilter()
				return "All DX 2-character grids blocked\n"
			}
			gridList, invalidTokens := parseGrid2List(value)
			if len(gridList) == 0 {
				return "Usage: REJECT DXGRID2 <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalidTokens) > 0 {
				return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, grid := range gridList {
					f.SetDXGrid2Prefix(grid, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DX 2-character grid filters disabled: %s\n", strings.Join(gridList, ", "))
		case "DEGRID2":
			value := strings.TrimSpace(strings.Join(parts[2:], " "))
			if value == "" {
				return "Usage: REJECT DEGRID2 <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if strings.EqualFold(value, "ALL") {
				c.updateFilter(func(f *filter.Filter) {
					f.ResetDEGrid2()
					f.BlockAllDEGrid2 = true
					f.AllDEGrid2 = false
				})
				c.saveFilter()
				return "All DE 2-character grids blocked\n"
			}
			gridList, invalidTokens := parseGrid2List(value)
			if len(gridList) == 0 {
				return "Usage: REJECT DEGRID2 <grid>[,<grid>...] (comma or space separated, or ALL)\nType HELP for usage.\n"
			}
			if len(invalidTokens) > 0 {
				return fmt.Sprintf("Unknown 2-character grid: %s\n", strings.Join(invalidTokens, ", "))
			}
			c.updateFilter(func(f *filter.Filter) {
				for _, grid := range gridList {
					f.SetDEGrid2Prefix(grid, false)
				}
			})
			c.saveFilter()
			return fmt.Sprintf("DE 2-character grid filters disabled: %s\n", strings.Join(gridList, ", "))
		default:
			return "Unknown filter type. Use: ALL, BAND, MODE, SOURCE, DXCALL, DECALL, CONFIDENCE, BEACON, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE, DXDXCC, or DEDXCC\nType HELP for usage.\n"
		}

	default:
		return "Invalid filter command. Type HELP for usage.\n"
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

func parseContinentList(arg string) []string {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	continents := make([]string, 0, len(values))
	for _, value := range values {
		cont := strings.ToUpper(strings.TrimSpace(value))
		if cont == "" || seen[cont] {
			continue
		}
		continents = append(continents, cont)
		seen[cont] = true
	}
	return continents
}

func parseZoneList(arg string) []int {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int]bool)
	zones := make([]int, 0, len(values))
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		zone, err := strconv.Atoi(v)
		if err != nil {
			return append(zones, -1) // leave invalid marker for the caller to handle uniformly
		}
		if seen[zone] {
			continue
		}
		zones = append(zones, zone)
		seen[zone] = true
	}
	return zones
}

func parseDXCCList(arg string) ([]int, []string) {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[int]bool)
	codes := make([]int, 0, len(values))
	invalid := make([]string, 0)
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		code, err := strconv.Atoi(v)
		if err != nil || code <= 0 {
			invalid = append(invalid, value)
			continue
		}
		if seen[code] {
			continue
		}
		codes = append(codes, code)
		seen[code] = true
	}
	return codes, invalid
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

func collectInvalidContinents(continents []string) []string {
	invalid := make([]string, 0)
	for _, cont := range continents {
		if !filter.IsSupportedContinent(cont) {
			invalid = append(invalid, cont)
		}
	}
	return invalid
}

func collectInvalidZones(zones []int) []int {
	invalid := make([]int, 0)
	for _, zone := range zones {
		if !filter.IsSupportedZone(zone) {
			invalid = append(invalid, zone)
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

func parseGrid2List(arg string) ([]string, []string) {
	values := splitListValues(arg)
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool)
	grids := make([]string, 0, len(values))
	invalid := make([]string, 0)
	for _, value := range values {
		raw := strings.ToUpper(strings.TrimSpace(value))
		if raw == "" {
			continue
		}
		if len(raw) > 2 {
			raw = raw[:2]
		}
		if len(raw) != 2 {
			invalid = append(invalid, value)
			continue
		}
		grid := raw
		if seen[grid] {
			continue
		}
		grids = append(grids, grid)
		seen[grid] = true
	}
	return grids, invalid
}

func formatContinentStates(all bool, enabled map[string]bool) string {
	if all {
		return "ALL"
	}
	var b strings.Builder
	for i, cont := range filter.SupportedContinents {
		state := "DISABLED"
		if enabled[cont] {
			state = "ENABLED"
		}
		b.WriteString(fmt.Sprintf("%s=%s", cont, state))
		if i < len(filter.SupportedContinents)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}

func formatZoneStates(all bool, enabled map[int]bool) string {
	if all {
		return "ALL"
	}
	var b strings.Builder
	for zone := filter.MinCQZone(); zone <= filter.MaxCQZone(); zone++ {
		state := "DISABLED"
		if enabled[zone] {
			state = "ENABLED"
		}
		b.WriteString(fmt.Sprintf("%d=%s", zone, state))
		if zone < filter.MaxCQZone() {
			b.WriteString(", ")
		}
	}
	return b.String()
}

func formatDXCCStates(all bool, enabled map[int]bool) string {
	if all {
		return "ALL"
	}
	if len(enabled) == 0 {
		return "NONE"
	}
	codes := make([]int, 0, len(enabled))
	for code := range enabled {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, fmt.Sprintf("%d", code))
	}
	return strings.Join(parts, ", ")
}

func keysString(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func keysInt(m map[int]bool) []string {
	vals := make([]int, 0, len(m))
	for k := range m {
		vals = append(vals, k)
	}
	sort.Ints(vals)
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		out = append(out, fmt.Sprintf("%d", v))
	}
	return out
}

func formatAllowBlockStrings(allowAll, blockAll bool, allow, block map[string]bool) string {
	if blockAll {
		return "BLOCK ALL"
	}
	allowStr := "ALL"
	if len(allow) > 0 {
		allowStr = strings.Join(keysString(allow), ", ")
	} else if !allowAll {
		allowStr = "NONE"
	}
	blockStr := "NONE"
	if len(block) > 0 {
		blockStr = strings.Join(keysString(block), ", ")
	}
	return fmt.Sprintf("allow=%s block=%s", allowStr, blockStr)
}

func formatAllowBlockInts(allowAll, blockAll bool, allow, block map[int]bool) string {
	if blockAll {
		return "BLOCK ALL"
	}
	allowStr := "ALL"
	if len(allow) > 0 {
		allowStr = strings.Join(keysInt(allow), ", ")
	} else if !allowAll {
		allowStr = "NONE"
	}
	blockStr := "NONE"
	if len(block) > 0 {
		blockStr = strings.Join(keysInt(block), ", ")
	}
	return fmt.Sprintf("allow=%s block=%s", allowStr, blockStr)
}

func joinZones(zones []int) string {
	if len(zones) == 0 {
		return ""
	}
	cp := append([]int(nil), zones...)
	sort.Ints(cp)
	parts := make([]string, 0, len(cp))
	for _, z := range cp {
		parts = append(parts, fmt.Sprintf("%d", z))
	}
	return strings.Join(parts, ", ")
}

func formatGrid2State(all bool, enabled map[string]bool) string {
	if all {
		return "ALL"
	}
	parts := make([]string, 0, len(enabled))
	for grid := range enabled {
		parts = append(parts, grid)
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "NONE"
	}
	return strings.Join(parts, ", ")
}

var (
	_ = formatContinentStates
	_ = formatZoneStates
	_ = formatDXCCStates
	_ = formatGrid2State
)

// formatGreeting replaces placeholders with the client's callsign and the cluster call.
func formatGreeting(tmpl, call, cluster string) string {
	if tmpl == "" {
		return ""
	}
	out := strings.ReplaceAll(tmpl, "<CALL>", call)
	out = strings.ReplaceAll(out, "<CLUSTER>", cluster)
	// Allow date/time tokens in the greeting as well.
	return applyWelcomeTokens(out, time.Now().UTC())
}

// applyWelcomeTokens replaces simple time/date placeholders in the welcome message.
// Tokens:
//
//	<DATE>      -> YYYY-MM-DD (UTC)
//	<TIME>      -> HH:MM:SS (UTC)
//	<DATETIME>  -> YYYY-MM-DD HH:MM:SS UTC
func applyWelcomeTokens(msg string, now time.Time) string {
	if msg == "" {
		return msg
	}
	date := now.Format("02-Jan-2006")
	tm := now.Format("15:04:05")
	datetime := now.Format("02-Jan-2006 15:04:05 UTC")

	msg = strings.ReplaceAll(msg, "<DATETIME>", datetime)
	msg = strings.ReplaceAll(msg, "<DATE>", date)
	msg = strings.ReplaceAll(msg, "<TIME>", tm)
	return msg
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
	s.shardsDirty.Store(true)
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
	s.shardsDirty.Store(true)
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
	// Protect broadcast goroutines from stalling on slow or wedged clients by
	// bounding how long a write can block. Each call refreshes the deadline and
	// then clears it, so idle clients are not disconnected by an old timeout.
	if c.conn != nil {
		if err := c.conn.SetWriteDeadline(time.Now().Add(defaultSendDeadline)); err != nil {
			return err
		}
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	// Normalize any existing CRLF to LF, then replace LF with CRLF so callers
	// don't need to worry about line endings (and we avoid doubling CRs).
	message = strings.ReplaceAll(message, "\r\n", "\n")
	message = strings.ReplaceAll(message, "\n", "\r\n")
	_, err := c.writer.WriteString(message)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// ReadLine reads a single logical line from the telnet client while enforcing
// three invariants:
//  1. Telnet IAC negotiations are consumed without leaking into user input.
//  2. User-supplied characters are bounded to maxLen bytes to prevent
//     unbounded growth (e.g., 32 bytes for login, 128 for commands).
//  3. Only the whitelisted character set (letters, digits, space, '/', '#',
//     '@', and '-') is accepted. Command contexts can optionally allow commas,
//     wildcards (*), and the '?' confidence glyph so filter commands retain
//     their legacy syntax. Any other
//     character is immediately rejected, logged, and returned as an error so
//     the caller can tear down the session before state is mutated.
//
// The CRLF terminator is always allowed: '\r' is skipped and '\n' ends the
// input. maxLen is measured in bytes because telnet input is ASCII-oriented.
func (c *Client) ReadLine(maxLen int, context string, allowComma, allowWildcard, allowConfidence bool) (string, error) {
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

		// Handle telnet IAC (Interpret As Command) sequences.
		if b == IAC {
			cmd, err := c.reader.ReadByte()
			if err != nil {
				return "", err
			}
			if cmd == DO || cmd == DONT || cmd == WILL || cmd == WONT {
				if _, err := c.reader.ReadByte(); err != nil {
					return "", err
				}
			}
			continue
		}

		// End of line once LF is observed (CR was already skipped).
		if b == '\n' {
			break
		}
		if b == '\r' {
			continue
		}

		if len(line) >= maxLen {
			c.logRejectedInput(context, fmt.Sprintf("exceeded %d-byte limit", maxLen))
			return "", newInputValidationError(
				fmt.Sprintf("%s input exceeds %d-byte limit", context, maxLen),
				fmt.Sprintf("%s input is too long (maximum %d characters).", friendlyContextLabel(context), maxLen),
			)
		}
		if !isAllowedInputByte(b, allowComma, allowWildcard, allowConfidence) {
			c.logRejectedInput(context, fmt.Sprintf("forbidden byte 0x%02X", b))
			return "", newInputValidationError(
				fmt.Sprintf("%s input contains forbidden byte 0x%02X", context, b),
				fmt.Sprintf("%s input may only contain %s.", friendlyContextLabel(context), allowedCharacterList(allowComma, allowWildcard, allowConfidence)),
			)
		}

		line = append(line, b)
	}

	return string(line), nil
}

// isAllowedInputByte reports whether the byte is part of the strict ingress
// safe list (letters, digits, space, '/', '#', '@'). When allowComma is true,
// comma is also accepted to preserve legacy comma-delimited filter syntax.
// CRLF is handled separately by ReadLine.
func newInputValidationError(reason, userMessage string) error {
	return &InputValidationError{
		reason:      reason,
		userMessage: userMessage,
	}
}

func isAllowedInputByte(b byte, allowComma, allowWildcard, allowConfidence bool) bool {
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

func allowedCharacterList(allowComma, allowWildcard, allowConfidence bool) string {
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
	return base
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
