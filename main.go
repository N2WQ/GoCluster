package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/stats"
	"dxcluster/telnet"
)

// Version will be set at build time
var Version = "dev"

func main() {
	fmt.Printf("DX Cluster Server v%s starting...\n", Version)

	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	filter.SetDefaultModeSelection(cfg.Filter.DefaultModes)
	if err := filter.EnsureUserDataDir(); err != nil {
		log.Printf("Warning: unable to initialize filter directory: %v", err)
	}

	// Print the configuration
	cfg.Print()

	// Create stats tracker
	statsTracker := stats.NewTracker()

	// Create spot buffer (ring buffer for storing recent spots)
	// Size tuned for ~15 minutes at ~20k spots/min => 300,000 entries
	spotBuffer := buffer.NewRingBuffer(300000)
	log.Println("Ring buffer created (capacity: 300000)")

	// Create deduplicator if enabled
	// THIS IS THE UNIFIED DEDUP ENGINE - ALL SOURCES FEED INTO IT
	var deduplicator *dedup.Deduplicator
	if cfg.Dedup.Enabled {
		window := time.Duration(cfg.Dedup.ClusterWindowSeconds) * time.Second
		deduplicator = dedup.NewDeduplicator(window)
		deduplicator.Start() // Start the processing loop
		log.Printf("Deduplication enabled with %v window", window)

		// Wire up dedup output to ring buffer and telnet broadcast
		// Deduplicated spots → Ring Buffer → Broadcast to clients
		go processOutputSpots(deduplicator, spotBuffer, nil, statsTracker) // We'll pass telnet server later
	}

	// Create command processor
	processor := commands.NewProcessor(spotBuffer)

	// Create and start telnet server
	telnetServer := telnet.NewServer(
		cfg.Telnet.Port,
		cfg.Telnet.WelcomeMessage,
		cfg.Telnet.MaxConnections,
		cfg.Telnet.BroadcastWorkers,
		processor,
	)

	err = telnetServer.Start()
	if err != nil {
		log.Fatalf("Failed to start telnet server: %v", err)
	}

	// Now wire up the telnet server to the output processor
	if cfg.Dedup.Enabled {
		// Restart the output processor with telnet server
		go processOutputSpots(deduplicator, spotBuffer, telnetServer, statsTracker)
	}

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name)
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN CW/RTTY: %v", err)
		} else {
			if cfg.Dedup.Enabled {
				// RBN → Deduplicator Input Channel
				go processRBNSpots(rbnClient, deduplicator, "RBN-CW")
				log.Println("RBN CW/RTTY client feeding spots into unified dedup engine")
			} else {
				// No dedup - RBN goes directly to buffer (legacy path)
				go processRBNSpotsNoDedupe(rbnClient, spotBuffer, telnetServer, statsTracker)
			}
		}
	}

	// Connect to RBN Digital feed if enabled (port 7001 - FT4/FT8)
	// RBN Digital spots go INTO the deduplicator input channel
	var rbnDigitalClient *rbn.Client
	if cfg.RBNDigital.Enabled {
		rbnDigitalClient = rbn.NewClient(cfg.RBNDigital.Host, cfg.RBNDigital.Port, cfg.RBNDigital.Callsign, cfg.RBNDigital.Name)
		err = rbnDigitalClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN Digital: %v", err)
		} else {
			if cfg.Dedup.Enabled {
				// RBN Digital → Deduplicator Input Channel
				go processRBNSpots(rbnDigitalClient, deduplicator, "RBN-FT")
				log.Println("RBN Digital (FT4/FT8) client feeding spots into unified dedup engine")
			} else {
				// No dedup - RBN Digital goes directly to buffer (legacy path)
				go processRBNSpotsNoDedupe(rbnDigitalClient, spotBuffer, telnetServer, statsTracker)
			}
		}
	}

	// Connect to PSKReporter if enabled
	// PSKReporter spots go INTO the deduplicator input channel
	var pskrClient *pskreporter.Client
	if cfg.PSKReporter.Enabled {
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, cfg.PSKReporter.Topic, cfg.PSKReporter.Name, cfg.PSKReporter.Workers)
		err = pskrClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		} else {
			if cfg.Dedup.Enabled {
				// PSKReporter → Deduplicator Input Channel
				go processPSKRSpots(pskrClient, deduplicator)
				log.Println("PSKReporter client feeding spots into unified dedup engine")
			} else {
				// No dedup - PSKReporter goes directly to buffer (legacy path)
				go processPSKRSpotsNoDedupe(pskrClient, spotBuffer, telnetServer, statsTracker)
			}
		}
	}

	// Start stats display goroutine
	go displayStats(statsTracker, deduplicator, spotBuffer, telnetServer, pskrClient)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\nCluster is running. Press Ctrl+C to stop.")
	fmt.Printf("Connect via: telnet localhost %d\n", cfg.Telnet.Port)
	if cfg.RBN.Enabled {
		fmt.Println("Receiving CW/RTTY spots from RBN (port 7000)...")
	}
	if cfg.RBNDigital.Enabled {
		fmt.Println("Receiving FT4/FT8 spots from RBN Digital (port 7001)...")
	}
	if cfg.PSKReporter.Enabled {
		fmt.Printf("Receiving digital mode spots from PSKReporter (topic: %s)...\n", cfg.PSKReporter.Topic)
	}
	if cfg.Dedup.Enabled {
		fmt.Printf("Unified deduplication active: %d second window\n", cfg.Dedup.ClusterWindowSeconds)
		fmt.Println("Architecture: ALL sources → Dedup Engine → Ring Buffer → Clients")
	}
	fmt.Println("\nStatistics will be displayed every 30 seconds...")
	fmt.Println("---")

	// Wait for shutdown signal
	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v\n", sig)
	fmt.Println("Shutting down gracefully...")

	// Stop deduplicator
	if deduplicator != nil {
		deduplicator.Stop()
	}

	// Stop RBN CW/RTTY client
	if rbnClient != nil {
		rbnClient.Stop()
	}

	// Stop RBN Digital client
	if rbnDigitalClient != nil {
		rbnDigitalClient.Stop()
	}

	// Stop PSKReporter client
	if pskrClient != nil {
		pskrClient.Stop()
	}

	// Stop the telnet server
	telnetServer.Stop()

	log.Println("Cluster stopped")
}

// displayStats prints statistics every 30 seconds
func displayStats(tracker *stats.Tracker, dedup *dedup.Deduplicator, buf *buffer.RingBuffer, telnetServer *telnet.Server, pskr *pskreporter.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Print spot counts by source
		tracker.Print()

		// Print dedup stats
		if dedup != nil {
			processed, duplicates, cacheSize := dedup.GetStats()
			dupRate := 0.0
			if processed > 0 {
				dupRate = float64(duplicates) / float64(processed) * 100
			}
			fmt.Printf("Dedup stats: processed=%d, duplicates=%d (%.1f%%), cache_size=%d\n",
				processed, duplicates, dupRate, cacheSize)
		}

		if telnetServer != nil {
			queueDrops, clientDrops := telnetServer.BroadcastMetricSnapshot()
			fmt.Printf("Telnet broadcast stats: workers=%d, queue_drops=%d, client_drops=%d\n", telnetServer.WorkerCount(), queueDrops, clientDrops)
		}

		if pskr != nil {
			workers, queueLen, drops := pskr.WorkerStats()
			fmt.Printf("PSKReporter stats: workers=%d, queue_len=%d, drops=%d\n", workers, queueLen, drops)
		}

		// Print ring buffer position and approximate memory usage
		position := buf.GetPosition()
		count := buf.GetCount()
		sizeKB := buf.GetSizeKB()
		sizeMB := float64(sizeKB) / 1024.0
		fmt.Printf("Ring buffer: position=%d, total_added=%d, size_mb=%.1fMB\n", position, count, sizeMB)
		fmt.Println("---")
	}
}

// processRBNSpots receives spots from RBN and sends to deduplicator
// This is the UNIFIED ARCHITECTURE path
// RBN → Deduplicator Input Channel
func processRBNSpots(client *rbn.Client, deduplicator *dedup.Deduplicator, source string) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()

	for spot := range spotChan {
		// Send spot to deduplicator input channel
		// All sources send here!
		dedupInput <- spot
	}
	log.Printf("%s: Spot processing stopped", source)
}

// processPSKRSpots receives spots from PSKReporter and sends to deduplicator
// PSKReporter → Deduplicator Input Channel
func processPSKRSpots(client *pskreporter.Client, deduplicator *dedup.Deduplicator) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()

	for spot := range spotChan {
		// Send spot to deduplicator input channel
		dedupInput <- spot
	}
}

// processOutputSpots receives deduplicated spots and distributes them
// Deduplicator Output → Ring Buffer → Broadcast to Clients
func processOutputSpots(deduplicator *dedup.Deduplicator, buf *buffer.RingBuffer, telnet *telnet.Server, tracker *stats.Tracker) {
	outputChan := deduplicator.GetOutputChannel()

	for spot := range outputChan {
		// Track spot by mode (FT8/FT4/CW/MANUAL/etc.) using the spot's Mode
		modeKey := strings.ToUpper(strings.TrimSpace(spot.Mode))
		if modeKey == "" {
			modeKey = string(spot.SourceType)
		}
		tracker.IncrementMode(modeKey)

		// Also track spot by higher-level source node (RBN, RBN-DIGITAL, PSKREPORTER, etc.).
		// Avoid double-counting when the mode key equals the source node (e.g., SourceType=="RBN" and SourceNode=="RBN").
		if spot.SourceNode != "" && spot.SourceNode != modeKey {
			tracker.IncrementSource(spot.SourceNode)
		}

		// Add to ring buffer
		buf.Add(spot)

		// Broadcast to all connected telnet clients
		if telnet != nil {
			telnet.BroadcastSpot(spot)
		}
	}
}

// processRBNSpotsNoDedupe is the legacy path when deduplication is disabled
// RBN → Ring Buffer → Clients (no deduplication)
func processRBNSpotsNoDedupe(client *rbn.Client, buf *buffer.RingBuffer, telnet *telnet.Server, tracker *stats.Tracker) {
	spotChan := client.GetSpotChannel()

	for spot := range spotChan {
		// Track spot by mode
		modeKey := strings.ToUpper(strings.TrimSpace(spot.Mode))
		if modeKey == "" {
			modeKey = string(spot.SourceType)
		}
		tracker.IncrementMode(modeKey)

		// Track spot by source node
		if spot.SourceNode != "" {
			tracker.IncrementSource(spot.SourceNode)
		}

		// Add directly to buffer (no dedup)
		buf.Add(spot)

		// Broadcast to all connected telnet clients
		telnet.BroadcastSpot(spot)
	}
}

// processPSKRSpotsNoDedupe is the legacy path when deduplication is disabled
func processPSKRSpotsNoDedupe(client *pskreporter.Client, buf *buffer.RingBuffer, telnet *telnet.Server, tracker *stats.Tracker) {
	spotChan := client.GetSpotChannel()

	for spot := range spotChan {
		// Track spot by mode
		modeKey := strings.ToUpper(strings.TrimSpace(spot.Mode))
		if modeKey == "" {
			modeKey = string(spot.SourceType)
		}
		tracker.IncrementMode(modeKey)

		// Track spot by source node
		if spot.SourceNode != "" {
			tracker.IncrementSource(spot.SourceNode)
		}

		buf.Add(spot)
		telnet.BroadcastSpot(spot)
	}
}
