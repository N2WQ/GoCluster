package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/spot"
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

	// Load CTY database for callsign validation
	ctyDB, err := cty.LoadCTYDatabase(cfg.CTY.File)
	if err != nil {
		log.Printf("Warning: failed to load CTY database: %v", err)
	}

	// Create stats tracker
	statsTracker := stats.NewTracker()

	// Create spot buffer (ring buffer for storing recent spots)
	// Size tuned for ~15 minutes at ~20k spots/min => 300,000 entries
	spotBuffer := buffer.NewRingBuffer(300000)
	log.Println("Ring buffer created (capacity: 300000)")

	var correctionIndex *spot.CorrectionIndex
	if cfg.CallCorrection.Enabled {
		correctionIndex = spot.NewCorrectionIndex()
	}

	var knownCalls *spot.KnownCallsigns
	if cfg.Confidence.KnownCallsignsFile != "" {
		knownCalls, err = spot.LoadKnownCallsigns(cfg.Confidence.KnownCallsignsFile)
		if err != nil {
			log.Printf("Warning: failed to load known callsigns: %v", err)
		} else {
			log.Printf("Loaded %d known callsigns from %s", knownCalls.Count(), cfg.Confidence.KnownCallsignsFile)
		}
	}

	freqAverager := spot.NewFrequencyAverager()
	var harmonicDetector *spot.HarmonicDetector
	if cfg.Harmonics.Enabled {
		harmonicDetector = spot.NewHarmonicDetector(spot.HarmonicSettings{
			Enabled:              true,
			RecencyWindow:        time.Duration(cfg.Harmonics.RecencySeconds) * time.Second,
			MaxHarmonicMultiple:  cfg.Harmonics.MaxHarmonicMultiple,
			FrequencyToleranceHz: cfg.Harmonics.FrequencyToleranceHz,
			MinReportDelta:       cfg.Harmonics.MinReportDelta,
		})
	}

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
		go processOutputSpots(deduplicator, spotBuffer, nil, statsTracker, nil, cfg.CallCorrection, ctyDB, harmonicDetector, cfg.Harmonics, knownCalls, freqAverager, cfg.SpotPolicy) // We'll pass telnet server later
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
		go processOutputSpots(deduplicator, spotBuffer, telnetServer, statsTracker, correctionIndex, cfg.CallCorrection, ctyDB, harmonicDetector, cfg.Harmonics, knownCalls, freqAverager, cfg.SpotPolicy)
	}

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name, ctyDB)
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
		rbnDigitalClient = rbn.NewClient(cfg.RBNDigital.Host, cfg.RBNDigital.Port, cfg.RBNDigital.Callsign, cfg.RBNDigital.Name, ctyDB)
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
	var (
		pskrClient *pskreporter.Client
		pskrTopics []string
	)
	if cfg.PSKReporter.Enabled {
		pskrTopics = cfg.PSKReporter.SubscriptionTopics()
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, pskrTopics, cfg.PSKReporter.Name, cfg.PSKReporter.Workers, ctyDB)
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
	statsInterval := time.Duration(cfg.Stats.DisplayIntervalSeconds) * time.Second
	go displayStats(statsInterval, statsTracker, deduplicator, spotBuffer, telnetServer, pskrClient, ctyDB)

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
		topicList := strings.Join(pskrTopics, ", ")
		if topicList == "" {
			topicList = "<none>"
		}
		fmt.Printf("Receiving digital mode spots from PSKReporter (topics: %s)...\n", topicList)
	}
	if cfg.Dedup.Enabled {
		fmt.Printf("Unified deduplication active: %d second window\n", cfg.Dedup.ClusterWindowSeconds)
		fmt.Println("Architecture: ALL sources → Dedup Engine → Ring Buffer → Clients")
	}
	fmt.Printf("\nStatistics will be displayed every %d seconds...\n", cfg.Stats.DisplayIntervalSeconds)
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

// displayStats prints statistics at the configured interval
func displayStats(interval time.Duration, tracker *stats.Tracker, dedup *dedup.Deduplicator, buf *buffer.RingBuffer, telnetServer *telnet.Server, pskr *pskreporter.Client, ctyDB *cty.CTYDatabase) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
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

		if ctyDB != nil {
			metrics := ctyDB.Metrics()
			cacheHitPercent := 0.0
			if metrics.TotalLookups > 0 {
				cacheHitPercent = float64(metrics.CacheHits) / float64(metrics.TotalLookups) * 100
			}
			cacheValidatedPercent := 0.0
			if metrics.Validated > 0 {
				cacheValidatedPercent = float64(metrics.ValidatedFromCache) / float64(metrics.Validated) * 100
			}
			fmt.Printf("CTY lookup stats: total=%d, cache_hits=%d (%.1f%%), cache_entries=%d, validated=%d, cache_validated=%d (%.1f%%)\n",
				metrics.TotalLookups, metrics.CacheHits, cacheHitPercent, metrics.CacheEntries, metrics.Validated, metrics.ValidatedFromCache, cacheValidatedPercent)
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
// Deduplicator Output  Ring Buffer  Broadcast to Clients
func processOutputSpots(
	deduplicator *dedup.Deduplicator,
	buf *buffer.RingBuffer,
	telnet *telnet.Server,
	tracker *stats.Tracker,
	correctionIdx *spot.CorrectionIndex,
	correctionCfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	harmonicDetector *spot.HarmonicDetector,
	harmonicCfg config.HarmonicConfig,
	knownCalls *spot.KnownCallsigns,
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
) {
	outputChan := deduplicator.GetOutputChannel()

	for spot := range outputChan {
		modeKey := strings.ToUpper(strings.TrimSpace(spot.Mode))
		if modeKey == "" {
			modeKey = string(spot.SourceType)
		}
		tracker.IncrementMode(modeKey)

		if spot.SourceNode != "" && spot.SourceNode != modeKey {
			tracker.IncrementSource(spot.SourceNode)
		}

		if spotPolicy.MaxAgeSeconds > 0 {
			if time.Since(spot.Time) > time.Duration(spotPolicy.MaxAgeSeconds)*time.Second {
				log.Printf("Spot dropped (stale): %s at %.1fkHz (age=%ds)", spot.DXCall, spot.Frequency, int(time.Since(spot.Time).Seconds()))
				continue
			}
		}

		var suppress bool
		if telnet != nil {
			suppress = maybeApplyCallCorrection(spot, correctionIdx, correctionCfg, ctyDB, knownCalls)
			if suppress {
				continue
			}
		}

		if harmonicDetector != nil && harmonicCfg.Enabled {
			if drop, fundamental := harmonicDetector.ShouldDrop(spot, time.Now().UTC()); drop {
				log.Printf("Harmonic suppressed: %s fundamental=%.1fkHz harmonic=%.1fkHz", spot.DXCall, fundamental, spot.Frequency)
				continue
			}
		}

		if freqAvg != nil && shouldAverageFrequency(spot) {
			window := frequencyAverageWindow(spotPolicy)
			tolerance := frequencyAverageTolerance(spotPolicy)
			avg, reports := freqAvg.Average(spot.DXCall, spot.Frequency, time.Now().UTC(), window, tolerance)
			rounded := math.Round(avg*10) / 10
			if reports >= spotPolicy.FrequencyAveragingMinReports && math.Abs(rounded-spot.Frequency) >= tolerance {
				log.Printf("Frequency averaged: %s %.3f -> %.3f kHz (%d reports)", spot.DXCall, spot.Frequency, rounded, reports)
				spot.Frequency = rounded
			}
		}

		buf.Add(spot)

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

func maybeApplyCallCorrection(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, known *spot.KnownCallsigns) bool {
	if spotEntry == nil {
		return false
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		spotEntry.Confidence = ""
		return false
	}
	if idx == nil || !cfg.Enabled {
		spotEntry.Confidence = "?"
		return false
	}

	window := callCorrectionWindow(cfg)
	now := time.Now().UTC()
	defer idx.Add(spotEntry, now, window)

	settings := spot.CorrectionSettings{
		MinConsensusReports:  cfg.MinConsensusReports,
		MinAdvantage:         cfg.MinAdvantage,
		MinConfidencePercent: cfg.MinConfidencePercent,
		MaxEditDistance:      cfg.MaxEditDistance,
		RecencyWindow:        window,
	}
	others := idx.Candidates(spotEntry, now, window)
	corrected, supporters, correctedConfidence, subjectConfidence, totalReporters, ok := spot.SuggestCallCorrection(spotEntry, others, settings, now)

	knownCall := known != nil && known.Contains(spotEntry.DXCall)
	spotEntry.Confidence = formatConfidence(subjectConfidence, totalReporters, knownCall)

	if ok && ctyDB != nil {
		if _, valid := ctyDB.LookupCallsign(corrected); valid {
			log.Printf("Call correction applied: %s -> %s at %.1f kHz (%d corroborators, %d%% confidence)",
				spotEntry.DXCall, corrected, spotEntry.Frequency, supporters, correctedConfidence)
			spotEntry.DXCall = corrected
			spotEntry.Confidence = "C"
		} else {
			log.Printf("Call correction rejected (CTY miss): suggested %s at %.1f kHz", corrected, spotEntry.Frequency)
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				log.Printf("Call correction suppression engaged: dropping spot from %s at %.1f kHz", spotEntry.DXCall, spotEntry.Frequency)
				return true
			}
			spotEntry.Confidence = "B"
		}
	} else if ok && ctyDB == nil {
		log.Printf("Call correction suggestion ignored (no CTY database): %s -> %s (%d corroborators, %d%% confidence)",
			spotEntry.DXCall, corrected, supporters, correctedConfidence)
		spotEntry.Confidence = "C"
	}

	return false
}

func callCorrectionWindow(cfg config.CallCorrectionConfig) time.Duration {
	if cfg.RecencySeconds <= 0 {
		return 45 * time.Second
	}
	return time.Duration(cfg.RecencySeconds) * time.Second
}

func frequencyAverageWindow(policy config.SpotPolicy) time.Duration {
	seconds := policy.FrequencyAveragingSeconds
	if seconds <= 0 {
		seconds = 45
	}
	return time.Duration(seconds) * time.Second
}

func frequencyAverageTolerance(policy config.SpotPolicy) float64 {
	toleranceHz := policy.FrequencyAveragingToleranceHz
	if toleranceHz <= 0 {
		toleranceHz = 300
	}
	return toleranceHz / 1000.0
}

func formatConfidence(percent int, totalReporters int, known bool) string {
	if totalReporters <= 1 {
		if known {
			return "S"
		}
		return "?"
	}

	value := percent
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}

	switch {
	case value <= 25:
		if known {
			return "S"
		}
		return "?"
	case value <= 75:
		return "P"
	default:
		return "V"
	}
}

func shouldAverageFrequency(s *spot.Spot) bool {
	mode := strings.ToUpper(strings.TrimSpace(s.Mode))
	return mode == "CW" || mode == "RTTY"
}
