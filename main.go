package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
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
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

const (
	dedupeEntryBytes    = 32
	ctyCacheEntryBytes  = 96
	knownCallEntryBytes = 24
	sourceModeDelimiter = "|"
)

// Version will be set at build time
var Version = "dev"

func main() {
	disableTUI := os.Getenv("DXC_NO_TUI") == "1"
	ui := newDashboard(!disableTUI)
	if ui != nil {
		ui.WaitReady()
		defer ui.Stop()
		log.SetOutput(ui.SystemWriter())
		ui.SetStats([]string{"Initializing..."})
	} else {
		log.SetOutput(os.Stdout)
	}

	log.Printf("DX Cluster Server v%s starting...", Version)

	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	filter.SetDefaultModeSelection(cfg.Filter.DefaultModes)
	if err := filter.EnsureUserDataDir(); err != nil {
		log.Printf("Warning: unable to initialize filter directory: %v", err)
	}

	// Print the configuration (stdout only when not using the dashboard)
	if ui == nil {
		cfg.Print()
	} else {
		log.Printf("Configuration loaded for %s (%s)", cfg.Server.Name, cfg.Server.NodeID)
	}

	// Load CTY database for callsign validation
	ctyDB, err := cty.LoadCTYDatabase(cfg.CTY.File)
	if err != nil {
		log.Printf("Warning: failed to load CTY database: %v", err)
	}

	// Create stats tracker
	statsTracker := stats.NewTracker()

	capacity := cfg.Buffer.Capacity
	if capacity <= 0 {
		capacity = 300000
	}
	// Create spot buffer (ring buffer for storing recent spots)
	spotBuffer := buffer.NewRingBuffer(capacity)
	log.Printf("Ring buffer created (capacity: %d)", capacity)

	var correctionIndex *spot.CorrectionIndex
	if cfg.CallCorrection.Enabled {
		correctionIndex = spot.NewCorrectionIndex()
	}
	spot.SetFrequencyToleranceHz(cfg.CallCorrection.FrequencyToleranceHz)

	var knownCalls *spot.KnownCallsigns
	if cfg.Confidence.KnownCallsignsFile != "" {
		knownCalls, err = spot.LoadKnownCallsigns(cfg.Confidence.KnownCallsignsFile)
		if err != nil {
			log.Printf("Warning: failed to load known callsigns: %v", err)
		} else {
			log.Printf("Loaded %d known callsigns from %s", knownCalls.Count(), cfg.Confidence.KnownCallsignsFile)
		}
	}

	var skewStore *skew.Store
	if cfg.Skew.Enabled {
		skewStore = skew.NewStore()
		if table, loadErr := skew.LoadFile(cfg.Skew.File); loadErr == nil {
			skewStore.Set(table)
			log.Printf("Loaded %d RBN skew corrections from %s", table.Count(), cfg.Skew.File)
		} else {
			log.Printf("Warning: failed to load RBN skew table (%s): %v", cfg.Skew.File, loadErr)
		}
		if skewStore.Count() == 0 {
			if count, err := refreshSkewTable(cfg.Skew, skewStore); err != nil {
				log.Printf("Warning: initial RBN skew download failed: %v", err)
			} else {
				log.Printf("Downloaded %d RBN skew corrections from %s", count, cfg.Skew.URL)
			}
		}
		if skewStore.Count() > 0 {
			startSkewScheduler(cfg.Skew, skewStore)
		} else {
			log.Printf("Warning: RBN skew scheduler disabled (no initial data); ensure %s is reachable", cfg.Skew.URL)
			skewStore = nil
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
			MinReportDeltaStep:   cfg.Harmonics.MinReportDeltaStep,
		})
	}

	// Create the deduplicator (always active; a zero-second window behaves like "disabled").
	// THIS IS THE UNIFIED DEDUP ENGINE - ALL SOURCES FEED INTO IT
	dedupWindow := time.Duration(cfg.Dedup.ClusterWindowSeconds) * time.Second
	deduplicator := dedup.NewDeduplicator(dedupWindow, cfg.Dedup.PreferStrongerSNR)
	deduplicator.Start()
	if dedupWindow > 0 {
		log.Printf("Deduplication active with %v window", dedupWindow)
	} else {
		log.Println("Deduplication disabled (cluster window=0); spots pass through unfiltered")
	}

	// Create command processor
	processor := commands.NewProcessor(spotBuffer)

	// Create and start telnet server
	telnetServer := telnet.NewServer(
		cfg.Telnet.Port,
		cfg.Telnet.WelcomeMessage,
		cfg.Telnet.MaxConnections,
		cfg.Telnet.BroadcastWorkers,
		cfg.Telnet.SkipHandshake,
		processor,
	)

	err = telnetServer.Start()
	if err != nil {
		log.Fatalf("Failed to start telnet server: %v", err)
	}

	// Start the unified output processor once the telnet server is ready
	go processOutputSpots(deduplicator, spotBuffer, telnetServer, statsTracker, correctionIndex, cfg.CallCorrection, ctyDB, harmonicDetector, cfg.Harmonics, knownCalls, freqAverager, cfg.SpotPolicy, ui)

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name, ctyDB, skewStore, cfg.RBN.KeepSSIDSuffix)
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN CW/RTTY: %v", err)
		} else {
			go processRBNSpots(rbnClient, deduplicator, "RBN-CW")
			log.Println("RBN CW/RTTY client feeding spots into unified dedup engine")
		}
	}

	// Connect to RBN Digital feed if enabled (port 7001 - FT4/FT8)
	// RBN Digital spots go INTO the deduplicator input channel
	var rbnDigitalClient *rbn.Client
	if cfg.RBNDigital.Enabled {
		rbnDigitalClient = rbn.NewClient(cfg.RBNDigital.Host, cfg.RBNDigital.Port, cfg.RBNDigital.Callsign, cfg.RBNDigital.Name, ctyDB, skewStore, cfg.RBNDigital.KeepSSIDSuffix)
		err = rbnDigitalClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN Digital: %v", err)
		} else {
			go processRBNSpots(rbnDigitalClient, deduplicator, "RBN-FT")
			log.Println("RBN Digital (FT4/FT8) client feeding spots into unified dedup engine")
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
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, pskrTopics, cfg.PSKReporter.Name, cfg.PSKReporter.Workers, ctyDB, skewStore)
		err = pskrClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		} else {
			go processPSKRSpots(pskrClient, deduplicator)
			log.Println("PSKReporter client feeding spots into unified dedup engine")
		}
	}

	// Start stats display goroutine
	statsInterval := time.Duration(cfg.Stats.DisplayIntervalSeconds) * time.Second
	go displayStats(statsInterval, statsTracker, deduplicator, spotBuffer, ctyDB, knownCalls, telnetServer, ui)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("Cluster is running. Press Ctrl+C to stop.")
	log.Printf("Connect via: telnet localhost %d", cfg.Telnet.Port)
	if cfg.RBN.Enabled {
		log.Println("Receiving CW/RTTY spots from RBN (port 7000)...")
	}
	if cfg.RBNDigital.Enabled {
		log.Println("Receiving FT4/FT8 spots from RBN Digital (port 7001)...")
	}
	if cfg.PSKReporter.Enabled {
		topicList := strings.Join(pskrTopics, ", ")
		if topicList == "" {
			topicList = "<none>"
		}
		log.Printf("Receiving digital mode spots from PSKReporter (topics: %s)...", topicList)
	}
	if cfg.Dedup.ClusterWindowSeconds > 0 {
		log.Printf("Unified deduplication active: %d second window", cfg.Dedup.ClusterWindowSeconds)
	} else {
		log.Println("Unified deduplication bypassed (window=0); duplicates are not filtered")
	}
	log.Println("Architecture: ALL sources → Dedup Engine → Ring Buffer → Clients")
	log.Printf("Statistics will be displayed every %d seconds...", cfg.Stats.DisplayIntervalSeconds)
	log.Println("---")

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal: %v", sig)
	log.Println("Shutting down gracefully...")

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
func displayStats(interval time.Duration, tracker *stats.Tracker, dedup *dedup.Deduplicator, buf *buffer.RingBuffer, ctyDB *cty.CTYDatabase, known *spot.KnownCallsigns, telnetSrv *telnet.Server, dash *dashboard) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prevSourceCounts := make(map[string]uint64)
	prevSourceModeCounts := make(map[string]uint64)

	for range ticker.C {
		lines := make([]string, 0, 6)
		lines = append(lines, formatUptimeLine(tracker.GetUptime()))
		lines = append(lines, formatMemoryLine(buf, dedup, ctyDB, known))

		sourceTotals := tracker.GetSourceCounts()
		sourceModeTotals := tracker.GetSourceModeCounts()

		rbnTotal := diffCounter(sourceTotals, prevSourceCounts, "RBN")
		rbnCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "CW")
		rbnRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "RTTY")
		lines = append(lines, fmt.Sprintf("RBN: %d (TOTAL) / %d (CW) / %d (RTTY)", rbnTotal, rbnCW, rbnRTTY))

		rbnFTTotal := diffCounter(sourceTotals, prevSourceCounts, "RBN-DIGITAL")
		rbnFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-DIGITAL", "FT8")
		rbnFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-DIGITAL", "FT4")
		lines = append(lines, fmt.Sprintf("RBN FT: %d (TOTAL) / %d (FT8) / %d (FT4)", rbnFTTotal, rbnFT8, rbnFT4))

		pskTotal := diffCounter(sourceTotals, prevSourceCounts, "PSKREPORTER")
		pskCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "CW")
		pskRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "RTTY")
		pskFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT8")
		pskFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT4")
		lines = append(lines, fmt.Sprintf("PSKReporter: %d (TOTAL) / %d (CW) / %d (RTTY) / %d (FT8) / %d (FT4)", pskTotal, pskCW, pskRTTY, pskFT8, pskFT4))

		totalCorrections := tracker.CallCorrections()
		totalFreqCorrections := tracker.FrequencyCorrections()
		totalHarmonics := tracker.HarmonicSuppressions()
		lines = append(lines, fmt.Sprintf("Corrected calls: %d (C) / %d (Q) / %d (H)", totalCorrections, totalFreqCorrections, totalHarmonics))
		var queueDrops, clientDrops uint64
		if telnetSrv != nil {
			queueDrops, clientDrops = telnetSrv.BroadcastMetricSnapshot()
		}
		lines = append(lines, fmt.Sprintf("Telnet drops: %d (Q) / %d (C)", queueDrops, clientDrops))

		prevSourceCounts = sourceTotals
		prevSourceModeCounts = sourceModeTotals

		if dash != nil {
			dash.SetStats(lines)
		} else {
			for _, line := range lines {
				log.Print(line)
			}
		}
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
	dash *dashboard,
) {
	outputChan := deduplicator.GetOutputChannel()

	for spot := range outputChan {
		modeKey := strings.ToUpper(strings.TrimSpace(spot.Mode))
		if modeKey == "" {
			modeKey = string(spot.SourceType)
		}
		tracker.IncrementMode(modeKey)

		sourceName := strings.ToUpper(strings.TrimSpace(spot.SourceNode))
		if sourceName != "" {
			tracker.IncrementSource(sourceName)
			tracker.IncrementSourceMode(sourceName, modeKey)
		}

		if spotPolicy.MaxAgeSeconds > 0 {
			if time.Since(spot.Time) > time.Duration(spotPolicy.MaxAgeSeconds)*time.Second {
				// log.Printf("Spot dropped (stale): %s at %.1fkHz (age=%ds)", spot.DXCall, spot.Frequency, int(time.Since(spot.Time).Seconds()))
				continue
			}
		}

		var suppress bool
		if telnet != nil {
			suppress = maybeApplyCallCorrection(spot, correctionIdx, correctionCfg, ctyDB, knownCalls, tracker, dash)
			if suppress {
				continue
			}
		}

		if harmonicDetector != nil && harmonicCfg.Enabled {
			if drop, fundamental, corroborators, deltaDB := harmonicDetector.ShouldDrop(spot, time.Now().UTC()); drop {
				harmonicMsg := fmt.Sprintf("Harmonic suppressed: %s %.1f -> %.1f kHz (%d corroborators, %d dB)", spot.DXCall, spot.Frequency, fundamental, corroborators, deltaDB)
				if tracker != nil {
					tracker.IncrementHarmonicSuppressions()
				}
				if dash != nil {
					dash.AppendHarmonic(harmonicMsg)
				} else {
					log.Println(harmonicMsg)
				}
				continue
			}
		}

		if freqAvg != nil && shouldAverageFrequency(spot) {
			window := frequencyAverageWindow(spotPolicy)
			tolerance := frequencyAverageTolerance(spotPolicy)
			avg, corroborators, totalReports := freqAvg.Average(spot.DXCall, spot.Frequency, time.Now().UTC(), window, tolerance)
			rounded := math.Round(avg*10) / 10
			confidence := 0
			if totalReports > 0 {
				confidence = corroborators * 100 / totalReports
			}
			if corroborators >= spotPolicy.FrequencyAveragingMinReports && math.Abs(rounded-spot.Frequency) >= tolerance {
				message := fmt.Sprintf("Frequency corrected: %s %.1f -> %.1f kHz (%d corroborators, %d%% confidence)", spot.DXCall, spot.Frequency, rounded, corroborators, confidence)
				spot.Frequency = rounded
				if tracker != nil {
					tracker.IncrementFrequencyCorrections()
				}
				if dash != nil {
					dash.AppendFrequency(message)
				} else {
					log.Println(message)
				}
			}
		}

		buf.Add(spot)

		if telnet != nil {
			telnet.BroadcastSpot(spot)
		}
	}
}

func maybeApplyCallCorrection(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, known *spot.KnownCallsigns, tracker *stats.Tracker, dash *dashboard) bool {
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
	ctyMatch := false
	if ctyDB != nil {
		if _, ok := ctyDB.LookupCallsign(spotEntry.DXCall); ok {
			ctyMatch = true
		}
	}
	spotEntry.Confidence = formatConfidence(subjectConfidence, totalReporters, knownCall, ctyMatch)

	if !ok {
		return false
	}

	message := fmt.Sprintf("Call corrected: %s -> %s at %.1f kHz (%d corroborators, %d%% confidence)",
		spotEntry.DXCall, corrected, spotEntry.Frequency, supporters, correctedConfidence)

	if ctyDB != nil {
		if _, valid := ctyDB.LookupCallsign(corrected); valid {
			if dash != nil {
				dash.AppendCall(message)
			} else {
				log.Println(message)
			}
			spotEntry.DXCall = corrected
			spotEntry.Confidence = "C"
			if tracker != nil {
				tracker.IncrementCallCorrections()
			}
		} else {
			log.Printf("Call correction rejected (CTY miss): suggested %s at %.1f kHz", corrected, spotEntry.Frequency)
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				log.Printf("Call correction suppression engaged: dropping spot from %s at %.1f kHz", spotEntry.DXCall, spotEntry.Frequency)
				return true
			}
			if !knownCall {
				spotEntry.Confidence = "B"
			}
		}
		return false
	}

	if dash != nil {
		dash.AppendCall(message)
	} else {
		log.Println(message)
	}
	spotEntry.DXCall = corrected
	spotEntry.Confidence = "C"
	if tracker != nil {
		tracker.IncrementCallCorrections()
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

func formatConfidence(percent int, totalReporters int, known bool, ctyMatch bool) string {
	if totalReporters <= 1 {
		if ctyMatch || known {
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

func refreshSkewTable(cfg config.SkewConfig, store *skew.Store) (int, error) {
	if store == nil {
		return 0, errors.New("skew: store is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	entries, err := skew.Fetch(ctx, cfg.URL)
	if err != nil {
		return 0, fmt.Errorf("skew: fetch failed: %w", err)
	}
	filtered := skew.FilterEntries(entries, cfg.MinSpots)
	if len(filtered) == 0 {
		return 0, fmt.Errorf("skew: no entries after filtering (min_spots=%d)", cfg.MinSpots)
	}
	table, err := skew.NewTable(filtered)
	if err != nil {
		return 0, fmt.Errorf("skew: build table: %w", err)
	}
	store.Set(table)
	if err := skew.WriteJSON(filtered, cfg.File); err != nil {
		return 0, fmt.Errorf("skew: write json: %w", err)
	}
	return table.Count(), nil
}

func startSkewScheduler(cfg config.SkewConfig, store *skew.Store) {
	if store == nil {
		return
	}
	go func() {
		for {
			delay := nextSkewRefreshDelay(time.Now().UTC())
			timer := time.NewTimer(delay)
			<-timer.C
			if count, err := refreshSkewTable(cfg, store); err != nil {
				log.Printf("Warning: scheduled RBN skew download failed: %v", err)
			} else {
				log.Printf("Scheduled RBN skew download complete (%d entries)", count)
			}
		}
	}()
}

func nextSkewRefreshDelay(now time.Time) time.Duration {
	target := time.Date(now.Year(), now.Month(), now.Day(), 0, 30, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func formatMemoryLine(buf *buffer.RingBuffer, dedup *dedup.Deduplicator, ctyDB *cty.CTYDatabase, known *spot.KnownCallsigns) string {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	execMB := bytesToMB(mem.Alloc)

	ringMB := 0.0
	if buf != nil {
		ringMB = float64(buf.GetSizeKB()) / 1024.0
	}

	dedupeMB := 0.0
	dedupeRatio := 0.0
	if dedup != nil {
		processed, duplicates, cacheSize := dedup.GetStats()
		dedupeMB = bytesToMB(uint64(cacheSize * dedupeEntryBytes))
		if processed > 0 {
			dedupeRatio = float64(duplicates) / float64(processed) * 100
		}
	}

	ctyMB := 0.0
	ctyRatio := 0.0
	if ctyDB != nil {
		metrics := ctyDB.Metrics()
		ctyMB = bytesToMB(uint64(metrics.CacheEntries * ctyCacheEntryBytes))
		if metrics.TotalLookups > 0 {
			ctyRatio = float64(metrics.CacheHits) / float64(metrics.TotalLookups) * 100
		}
	}

	knownMB := 0.0
	knownRatio := 0.0
	if known != nil {
		knownMB = bytesToMB(uint64(known.Count() * knownCallEntryBytes))
		lookups, hits := known.Stats()
		if lookups > 0 {
			knownRatio = float64(hits) / float64(lookups) * 100
		}
	}

	return fmt.Sprintf("Memory MB: %.1f / %.1f / %.1f (%.1f%%) / %.1f (%.1f%%) / %.1f (%.1f%%)",
		execMB, ringMB, dedupeMB, dedupeRatio, ctyMB, ctyRatio, knownMB, knownRatio)
}

func formatUptimeLine(uptime time.Duration) string {
	hours := int(uptime.Hours())
	minutes := int(uptime.Minutes()) % 60
	return fmt.Sprintf("Uptime: %02d:%02d", hours, minutes)
}

func diffCounter(current, previous map[string]uint64, key string) uint64 {
	if current == nil {
		current = map[string]uint64{}
	}
	if previous == nil {
		previous = map[string]uint64{}
	}
	key = strings.ToUpper(strings.TrimSpace(key))
	cur := current[key]
	prev := previous[key]
	if cur >= prev {
		return cur - prev
	}
	return cur
}

func diffSourceMode(current, previous map[string]uint64, source, mode string) uint64 {
	key := sourceModeKey(source, mode)
	return diffCounter(current, previous, key)
}

func sourceModeKey(source, mode string) string {
	source = strings.ToUpper(strings.TrimSpace(source))
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if source == "" || mode == "" {
		return ""
	}
	return source + sourceModeDelimiter + mode
}

func bytesToMB(b uint64) float64 {
	return float64(b) / (1024.0 * 1024.0)
}
