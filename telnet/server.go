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
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
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
	port           int                   // TCP port to listen on
	welcomeMessage string                // Welcome message for new connections
	maxConnections int                   // Maximum concurrent client connections
	listener       net.Listener          // TCP listener
	clients        map[string]*Client    // Map of callsign → Client
	clientsMutex   sync.RWMutex          // Protects clients map
	shutdown       chan struct{}         // Shutdown coordination channel
	broadcast      chan *spot.Spot       // Broadcast channel for spots (buffered 100)
	processor      *commands.Processor   // Command processor for user commands
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
	spotChan  chan *spot.Spot // Buffered channel for spot delivery (capacity 10)
	filter    *filter.Filter  // Personal spot filter (band, mode, callsign)
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

// NewServer creates a new telnet server
func NewServer(port int, welcomeMessage string, maxConnections int, processor *commands.Processor) *Server {
	return &Server{
		port:           port,
		welcomeMessage: welcomeMessage,
		maxConnections: maxConnections,
		clients:        make(map[string]*Client),
		shutdown:       make(chan struct{}),
		broadcast:      make(chan *spot.Spot, 100),
		processor:      processor,
	}
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
		log.Println("Broadcast channel full, dropping spot")
	}
}

// broadcastSpot sends a spot to all clients (internal)
func (s *Server) broadcastSpot(spot *spot.Spot) {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()

	for _, client := range s.clients {
		// Check if spot passes client's filter
		if !client.filter.Matches(spot) {
			continue
		}

		select {
		case client.spotChan <- spot:
		default:
			// Client's channel is full, skip
			log.Printf("Client %s spot channel full, dropping spot", client.callsign)
		}
	}
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

	// Create client object
	client := &Client{
		conn:      conn,
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		connected: time.Now(),
		address:   address,
		spotChan:  make(chan *spot.Spot, 50),
		filter:    filter.NewFilter(), // Start with no filters (accept all)
	}

	// Send telnet options to suppress advanced features
	client.SendRaw([]byte{IAC, DONT, 1}) // Don't echo
	client.SendRaw([]byte{IAC, DONT, 3}) // Don't suppress go-ahead
	client.SendRaw([]byte{IAC, WONT, 1}) // Won't echo
	client.SendRaw([]byte{IAC, WILL, 3}) // Will suppress go-ahead

	// Send welcome message
	client.Send(s.welcomeMessage)
	client.Send("Enter your callsign: ")

	// Read callsign
	callsign, err := client.ReadLine()
	if err != nil {
		log.Printf("Error reading callsign from %s: %v", address, err)
		return
	}

	client.callsign = strings.ToUpper(strings.TrimSpace(callsign))
	log.Printf("Client %s logged in as %s", address, client.callsign)

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
		response := s.processor.Process(line)

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
		return fmt.Sprintf("Current filters: %s\n", c.filter.String())

	case "set/filter":
		if len(parts) < 3 {
			return "Usage: SET/FILTER BAND <band> | SET/FILTER MODE <mode> | SET/FILTER CALL <pattern>\n"
		}

		filterType := strings.ToUpper(parts[1])
		value := strings.ToUpper(parts[2])

		switch filterType {
		case "BAND":
			c.filter.SetBand(value, true)
			return fmt.Sprintf("Filter set: Band %s\n", value)
		case "MODE":
			c.filter.SetMode(value, true)
			return fmt.Sprintf("Filter set: Mode %s\n", value)
		case "CALL":
			c.filter.AddCallsignPattern(value)
			return fmt.Sprintf("Filter set: Callsign %s\n", value)
		default:
			return "Unknown filter type. Use: BAND, MODE, or CALL\n"
		}

	case "unset/filter":
		if len(parts) < 2 {
			return "Usage: UNSET/FILTER ALL | UNSET/FILTER BAND | UNSET/FILTER MODE | UNSET/FILTER CALL\n"
		}

		filterType := strings.ToUpper(parts[1])

		switch filterType {
		case "ALL":
			c.filter.Reset()
			return "All filters cleared\n"
		case "BAND":
			c.filter.ResetBands()
			return "Band filters cleared\n"
		case "MODE":
			c.filter.ResetModes()
			return "Mode filters cleared\n"
		case "CALL":
			c.filter.ClearCallsignPatterns()
			return "Callsign filters cleared\n"
		default:
			return "Unknown filter type. Use: ALL, BAND, MODE, or CALL\n"
		}

	default:
		return "Unknown filter command\n"
	}
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
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()
	s.clients[client.callsign] = client
	log.Printf("Registered client: %s (total: %d)", client.callsign, len(s.clients))
}

// unregisterClient removes a client from the active clients list
func (s *Server) unregisterClient(client *Client) {
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()
	delete(s.clients, client.callsign)
	close(client.spotChan)
	log.Printf("Unregistered client: %s (total: %d)", client.callsign, len(s.clients))
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
