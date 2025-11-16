package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/rbn"
	"dxcluster/telnet"
)

// Version will be set at build time
var Version = "dev"

func main() {
	// Print startup message
	fmt.Printf("DX Cluster Server v%s starting...\n", Version)

	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Print the configuration
	cfg.Print()

	// Create spot buffer
	spotBuffer := buffer.NewRingBuffer(1000)

	// Create command processor
	processor := commands.NewProcessor(spotBuffer)

	// Create and start telnet server
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

	// Connect to RBN if enabled
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign)
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN: %v", err)
		} else {
			// Start processing RBN spots
			go processRBNSpots(rbnClient, spotBuffer, telnetServer)
		}
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\nCluster is running. Press Ctrl+C to stop.")
	fmt.Printf("Connect via: telnet localhost %d\n", cfg.Telnet.Port)
	if cfg.RBN.Enabled {
		fmt.Println("Receiving real-time spots from Reverse Beacon Network...")
	}

	// Wait for shutdown signal
	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v\n", sig)
	fmt.Println("Shutting down gracefully...")

	// Stop RBN client
	if rbnClient != nil {
		rbnClient.Stop()
	}

	// Stop the telnet server
	telnetServer.Stop()

	log.Println("Cluster stopped")
}

// processRBNSpots receives spots from RBN and distributes them
func processRBNSpots(client *rbn.Client, buf *buffer.RingBuffer, telnet *telnet.Server) {
	spotChan := client.GetSpotChannel()

	for spot := range spotChan {
		// Add to buffer
		buf.Add(spot)

		// Broadcast to all connected telnet clients
		telnet.BroadcastSpot(spot)
	}
}
