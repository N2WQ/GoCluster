// Package main implements a high-performance DX Cluster Server for amateur radio operators.
//
// Architecture Overview:
// The server aggregates radio "spots" (station sightings) from multiple sources and broadcasts
// them to connected telnet clients. All spots flow through a unified deduplication engine
// before being stored in a ring buffer and distributed to clients.
//
// Data Flow:
//   RBN Network ──┐
//   PSKReporter ──┼─→ Deduplicator Input → Dedup Engine → Dedup Output
//   Other Sources─┘                                          ↓
//                                                    Ring Buffer (FIFO, 1000 spots)
//                                                             ↓
//                                                    Telnet Broadcast
//                                                             ↓
//                                              Connected Clients (with per-client filters)
//
// Components:
//   - Telnet Server: Handles client connections and broadcasts spots
//   - Ring Buffer: Stores the most recent 1000 spots for historical queries
//   - Deduplicator: Hash-based duplicate detection with configurable time windows
//   - RBN Client: Receives CW/RTTY spots from Reverse Beacon Network
//   - PSKReporter Client: Receives digital mode spots via MQTT
//   - Command Processor: Handles user commands (HELP, SHOW/DX, SHOW/STATION, BYE)
//   - Filter Engine: Per-client filtering by band, mode, and callsign patterns
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/telnet"
)

// Version is the application version string, typically set at build time via linker flags.
// Example: go build -ldflags "-X main.Version=1.0.0"
var Version = "dev"

// main is the application entry point. It orchestrates the startup sequence:
// 1. Loads configuration from config.yaml
// 2. Creates the spot buffer (ring buffer) for storing recent spots
// 3. Initializes the deduplicator (if enabled) - the unified entry point for all spot sources
// 4. Starts the telnet server for client connections
// 5. Connects to RBN and PSKReporter (if enabled)
// 6. Sets up graceful shutdown on SIGINT/SIGTERM
//
// The function blocks until a shutdown signal is received, then cleanly stops all components.
func main() {
	// Print startup message with version information
	fmt.Printf("DX Cluster Server v%s starting...\n", Version)

	// Load configuration from YAML file
	// This includes server settings, telnet config, RBN/PSKReporter settings, and dedup parameters
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Print the loaded configuration to console for verification
	cfg.Print()

	// Create the ring buffer (circular FIFO) to store the most recent 1000 spots
	// This buffer is used for historical queries (SHOW/DX, SHOW/STATION commands)
	// and is shared across all components
	spotBuffer := buffer.NewRingBuffer(1000)
	log.Println("Ring buffer created (capacity: 1000)")

	// Create and start the deduplicator if enabled
	// CRITICAL: This is the UNIFIED DEDUP ENGINE - ALL sources (RBN, PSKReporter, etc.)
	// feed into the deduplicator's input channel. Deduplicated spots flow out to
	// the ring buffer and telnet clients.
	var deduplicator *dedup.Deduplicator
	if cfg.Dedup.Enabled {
		// Convert configured window seconds to time.Duration
		window := time.Duration(cfg.Dedup.ClusterWindowSeconds) * time.Second
		deduplicator = dedup.NewDeduplicator(window)
		deduplicator.Start() // Start the background processing goroutine
		log.Printf("Deduplication enabled with %v window", window)

		// Wire up dedup output to ring buffer and telnet broadcast
		// Data flow: Deduplicator Output Channel → Ring Buffer → Broadcast to clients
		// Note: telnet server is nil initially, will be updated after server creation
		go processOutputSpots(deduplicator, spotBuffer, nil) // We'll pass telnet server later
	}

	// Create command processor that handles user commands (HELP, SHOW/DX, SHOW/STATION, BYE)
	// The processor needs access to the spot buffer for historical queries
	processor := commands.NewProcessor(spotBuffer)

	// Create and start the telnet server
	// This server listens for incoming client connections and manages their sessions
	// Each client gets its own goroutine for handling commands and spot broadcasts
	telnetServer := telnet.NewServer(
		cfg.Telnet.Port,
		cfg.Telnet.WelcomeMessage,
		cfg.Telnet.MaxConnections,
		processor,
	)

	err = telnetServer.Start()
	if err != nil {
		log.Fatalf("Failed to start telnet server: %v", err)
	}

	// Now wire up the telnet server to the output processor
	// We had to create the telnet server first, now we can connect it to the spot flow
	if cfg.Dedup.Enabled {
		// Restart the output processor with telnet server included
		// This goroutine will now both store spots in the buffer AND broadcast to clients
		go processOutputSpots(deduplicator, spotBuffer, telnetServer)
	}

	// Connect to Reverse Beacon Network (RBN) if enabled
	// RBN provides real-time CW and RTTY spots from automated skimmer stations
	// All RBN spots are fed into the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign)
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN: %v", err)
		} else {
			if cfg.Dedup.Enabled {
				// Modern path: RBN → Deduplicator Input Channel
				// All sources funnel through the unified deduplication engine
				go processRBNSpots(rbnClient, deduplicator)
				log.Println("RBN client feeding spots into unified dedup engine")
			} else {
				// Legacy path (when dedup is disabled): RBN → Buffer → Clients directly
				// This bypasses deduplication - spots go straight to storage and broadcast
				go processRBNSpotsNoDedupe(rbnClient, spotBuffer, telnetServer)
			}
		}
	}

	// Connect to PSKReporter if enabled
	// PSKReporter provides digital mode spots (FT8, FT4, etc.) via MQTT
	// All PSKReporter spots are fed into the deduplicator input channel
	var pskrClient *pskreporter.Client
	if cfg.PSKReporter.Enabled {
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, cfg.PSKReporter.Topic)
		err = pskrClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		} else {
			if cfg.Dedup.Enabled {
				// Modern path: PSKReporter → Deduplicator Input Channel
				// Ensures digital mode spots are deduplicated alongside RBN spots
				go processPSKRSpots(pskrClient, deduplicator)
				log.Println("PSKReporter client feeding spots into unified dedup engine")
			} else {
				// Legacy path (when dedup is disabled): PSKReporter → Buffer → Clients directly
				go processPSKRSpotsNoDedupe(pskrClient, spotBuffer, telnetServer)
			}
		}
	}

	// Set up signal handling for graceful shutdown
	// Listen for SIGINT (Ctrl+C) and SIGTERM signals
	// Buffered channel with capacity 1 to avoid missing the signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Display operational status to console
	// This provides users with quick connection information and confirms what features are active
	fmt.Println("\nCluster is running. Press Ctrl+C to stop.")
	fmt.Printf("Connect via: telnet localhost %d\n", cfg.Telnet.Port)
	if cfg.RBN.Enabled {
		fmt.Println("Receiving real-time CW/RTTY spots from RBN...")
	}
	if cfg.PSKReporter.Enabled {
		fmt.Printf("Receiving digital mode spots from PSKReporter (topic: %s)...\n", cfg.PSKReporter.Topic)
	}
	if cfg.Dedup.Enabled {
		fmt.Printf("Unified deduplication active: %d second window\n", cfg.Dedup.ClusterWindowSeconds)
		fmt.Println("Architecture: ALL sources → Dedup Engine → Ring Buffer → Clients")
	}

	// Block waiting for shutdown signal
	// This is the main event loop - the program runs until interrupted
	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v\n", sig)
	fmt.Println("Shutting down gracefully...")

	// Graceful shutdown: Stop all components in reverse startup order
	// This ensures clean disconnection and proper resource cleanup

	// Stop deduplicator first to stop accepting new spots
	if deduplicator != nil {
		deduplicator.Stop()
	}

	// Stop spot sources (RBN and PSKReporter clients)
	// This closes their connections and stops feeding spots into the system
	if rbnClient != nil {
		rbnClient.Stop()
	}

	if pskrClient != nil {
		pskrClient.Stop()
	}

	// Stop the telnet server last, allowing it to broadcast any final spots
	// This disconnects all clients and closes the listening socket
	telnetServer.Stop()

	log.Println("Cluster stopped")
}

// processRBNSpots is a goroutine that bridges RBN spots to the deduplicator.
// This implements the unified architecture where ALL spot sources feed into
// a single deduplication engine.
//
// Data flow: RBN Client → Deduplicator Input Channel
//
// The function runs in an infinite loop until the RBN client's spot channel
// is closed (typically during shutdown). Each spot received from RBN is
// forwarded to the deduplicator's input channel without modification.
func processRBNSpots(client *rbn.Client, deduplicator *dedup.Deduplicator) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()

	for spot := range spotChan {
		// Forward spot to deduplicator input channel
		// All sources (RBN, PSKReporter, etc.) converge here
		dedupInput <- spot
	}
}

// processPSKRSpots is a goroutine that bridges PSKReporter spots to the deduplicator.
// This implements the unified architecture where ALL spot sources feed into
// a single deduplication engine.
//
// Data flow: PSKReporter Client → Deduplicator Input Channel
//
// The function runs in an infinite loop until the PSKReporter client's spot channel
// is closed (typically during shutdown). Each spot received from PSKReporter is
// forwarded to the deduplicator's input channel without modification.
func processPSKRSpots(client *pskreporter.Client, deduplicator *dedup.Deduplicator) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()

	for spot := range spotChan {
		// Forward spot to deduplicator input channel
		// Ensures PSKReporter spots are deduplicated alongside RBN spots
		dedupInput <- spot
	}
}

// processOutputSpots is a goroutine that receives deduplicated spots and distributes them
// to both the ring buffer (for historical queries) and all connected telnet clients
// (for real-time spot broadcasts).
//
// Data flow: Deduplicator Output Channel → Ring Buffer + Telnet Broadcast
//
// The function runs in an infinite loop until the deduplicator's output channel is closed
// (typically during shutdown). This is the final stage of the spot processing pipeline.
//
// Note: The telnet parameter may be nil during initial startup. In that case, spots are
// only stored in the buffer and not broadcast to clients.
func processOutputSpots(deduplicator *dedup.Deduplicator, buf *buffer.RingBuffer, telnet *telnet.Server) {
	outputChan := deduplicator.GetOutputChannel()

	for spot := range outputChan {
		// Store in ring buffer for historical queries (SHOW/DX, SHOW/STATION)
		buf.Add(spot)

		// Broadcast to all connected telnet clients in real-time
		// Only if telnet server is initialized (may be nil during startup)
		if telnet != nil {
			telnet.BroadcastSpot(spot)
		}
	}
}

// processRBNSpotsNoDedupe is a legacy goroutine used when deduplication is disabled.
// It provides a direct path from RBN to the buffer and clients, bypassing the
// deduplication engine entirely.
//
// Data flow: RBN Client → Ring Buffer + Telnet Broadcast (no deduplication)
//
// This path is maintained for backwards compatibility and testing scenarios where
// deduplication is not desired. In production, the unified dedup architecture
// (processRBNSpots) is preferred.
func processRBNSpotsNoDedupe(client *rbn.Client, buf *buffer.RingBuffer, telnet *telnet.Server) {
	spotChan := client.GetSpotChannel()

	for spot := range spotChan {
		// Add directly to buffer without deduplication
		buf.Add(spot)

		// Broadcast to all connected telnet clients
		telnet.BroadcastSpot(spot)
	}
}

// processPSKRSpotsNoDedupe is a legacy goroutine used when deduplication is disabled.
// It provides a direct path from PSKReporter to the buffer and clients, bypassing the
// deduplication engine entirely.
//
// Data flow: PSKReporter Client → Ring Buffer + Telnet Broadcast (no deduplication)
//
// This path is maintained for backwards compatibility and testing scenarios where
// deduplication is not desired. In production, the unified dedup architecture
// (processPSKRSpots) is preferred.
func processPSKRSpotsNoDedupe(client *pskreporter.Client, buf *buffer.RingBuffer, telnet *telnet.Server) {
	spotChan := client.GetSpotChannel()

	for spot := range spotChan {
		// Add directly to buffer without deduplication
		buf.Add(spot)

		// Broadcast to all connected telnet clients
		telnet.BroadcastSpot(spot)
	}
}
