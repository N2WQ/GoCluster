// Program gocluster wires together all ingest clients (RBN, PSKReporter),
// protections (deduplication, call correction, harmonics), persistence layers
// (ring buffer, grid store), and the telnet server UI.
package main

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dxcluster/bandmap"
	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/gridstore"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
	"dxcluster/uls"

	"github.com/dustin/go-humanize"
)

const (
	dedupeEntryBytes    = 32
	ctyCacheEntryBytes  = 96
	knownCallEntryBytes = 24
	sourceModeDelimiter = "|"
)

// Version will be set at build time
var Version = "dev"

type gridMetrics struct {
	learnedTotal atomic.Uint64
	cacheLookups atomic.Uint64
	cacheHits    atomic.Uint64
}

type gridCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	lru      *list.List
	entries  map[string]*list.Element
}

type gridEntry struct {
	call      string
	grid      string
	updatedAt time.Time
}

// newGridCache builds an in-memory LRU for grids keyed by callsign, honoring an
// optional TTL so old observations can expire without hitting SQLite.
func newGridCache(capacity int, ttl time.Duration) *gridCache {
	if capacity <= 0 {
		capacity = 100000
	}
	return &gridCache{
		capacity: capacity,
		ttl:      ttl,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
	}
}

// shouldUpdate returns true when the provided grid differs from what is stored
// in the cache (or DB), and updates the cache with the new value. On cache miss
// it consults SQLite to avoid duplicate writes when the database already holds
// the same grid.
func (c *gridCache) shouldUpdate(call, grid string, store *gridstore.Store) bool {
	if call == "" || grid == "" {
		return false
	}
	now := time.Now()

	// Fast path: cache present
	c.mu.Lock()
	if elem, ok := c.entries[call]; ok {
		entry := elem.Value.(*gridEntry)
		if c.ttl > 0 && now.Sub(entry.updatedAt) > c.ttl {
			// stale entry; evict and treat as miss
			c.lru.Remove(elem)
			delete(c.entries, call)
		} else if entry.grid == grid {
			c.mu.Unlock()
			return false
		}
		entry.grid = grid
		entry.updatedAt = now
		c.lru.MoveToFront(elem)
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()

	// Miss: check DB to avoid redundant writes
	if store != nil {
		if rec, err := store.Get(call); err == nil && rec != nil && rec.Grid.Valid {
			existing := strings.ToUpper(strings.TrimSpace(rec.Grid.String))
			if existing == grid {
				c.add(call, grid)
				return false
			}
		}
	}

	c.add(call, grid)
	return true
}

func (c *gridCache) add(call, grid string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[call]; ok {
		entry := elem.Value.(*gridEntry)
		entry.grid = grid
		entry.updatedAt = time.Now()
		c.lru.MoveToFront(elem)
		return
	}

	elem := c.lru.PushFront(&gridEntry{call: call, grid: grid, updatedAt: time.Now()})
	c.entries[call] = elem
	if c.capacity > 0 && len(c.entries) > c.capacity {
		// Evict least-recently-used
		if tail := c.lru.Back(); tail != nil {
			c.lru.Remove(tail)
			if e, ok := tail.Value.(*gridEntry); ok {
				delete(c.entries, e.call)
			}
		}
	}
}

func (c *gridCache) get(call string) (string, bool) {
	if call == "" {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[call]; ok {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*gridEntry)
		if c.ttl > 0 && time.Since(entry.updatedAt) > c.ttl {
			c.lru.Remove(elem)
			delete(c.entries, call)
			return "", false
		}
		return entry.grid, entry.grid != ""
	}
	return "", false
}

func (c *gridCache) lookupWithMetrics(call string, metrics *gridMetrics) (string, bool) {
	if metrics != nil {
		metrics.cacheLookups.Add(1)
	}
	grid, ok := c.get(call)
	if ok && metrics != nil {
		metrics.cacheHits.Add(1)
	}
	return grid, ok
}

func main() {
	disableTUI := os.Getenv("DXC_NO_TUI") == "1"
	ui := newDashboard(!disableTUI)
	if ui != nil {
		ui.WaitReady()
		defer ui.Stop()
		// Dashboard handles its own timestamp formatting; disable the default log prefixes.
		log.SetFlags(0)
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
	callCacheTTL := time.Duration(cfg.CallCache.TTLSeconds) * time.Second
	spot.ConfigureNormalizeCallCache(cfg.CallCache.Size, callCacheTTL)
	rbn.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
	pskreporter.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
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

	// Optional dedicated call-correction debug logger
	var corrDebugFile *os.File
	var corrDebugLogger *log.Logger
	if cfg.CallCorrection.DebugLog {
		path := strings.TrimSpace(cfg.CallCorrection.DebugLogFile)
		if path != "" {
			if f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
				log.Printf("Warning: unable to open call correction debug log file %s: %v", path, err)
			} else {
				corrDebugFile = f
				corrDebugLogger = log.New(f, "", log.LstdFlags)
				log.Printf("Call correction debug logging to %s", path)
			}
		}
	}

	// Start the FCC ULS downloader in the background (does not block spot processing)
	uls.StartBackground(cfg.FCCULS)

	// Load CTY database for callsign validation
	ctyDB, err := cty.LoadCTYDatabase(cfg.CTY.File)
	if err != nil {
		log.Printf("Warning: failed to load CTY database: %v", err)
	}
	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	if strings.TrimSpace(cfg.FCCULS.DBPath) != "" {
		uls.SetLicenseDBPath(cfg.FCCULS.DBPath)
	}

	// Create stats tracker
	statsTracker := stats.NewTracker()
	unlicensedReporter := makeUnlicensedReporter(ui, statsTracker)

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

	var knownCalls atomic.Pointer[spot.KnownCallsigns]
	knownCallsPath := strings.TrimSpace(cfg.KnownCalls.File)
	knownCallsURL := strings.TrimSpace(cfg.KnownCalls.URL)
	if knownCallsPath != "" {
		if _, err := os.Stat(knownCallsPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if knownCallsURL != "" {
					if fresh, refreshErr := refreshKnownCallsigns(cfg.KnownCalls); refreshErr != nil {
						log.Printf("Warning: known calls download failed: %v", refreshErr)
					} else {
						knownCalls.Store(fresh)
						log.Printf("Downloaded %d known callsigns from %s", fresh.Count(), knownCallsURL)
					}
				} else {
					log.Printf("Warning: known calls file %s missing and no download URL configured", knownCallsPath)
				}
			} else {
				log.Printf("Warning: unable to access known calls file %s: %v", knownCallsPath, err)
			}
		}
		if knownCalls.Load() == nil {
			if loaded, loadErr := spot.LoadKnownCallsigns(knownCallsPath); loadErr != nil {
				log.Printf("Warning: failed to load known callsigns: %v", loadErr)
			} else {
				knownCalls.Store(loaded)
				log.Printf("Loaded %d known callsigns from %s", loaded.Count(), knownCallsPath)
			}
		}
	}
	gridStore, err := gridstore.Open(cfg.GridDBPath)
	if err != nil {
		log.Fatalf("Failed to open grid database: %v", err)
	}
	defer gridStore.Close()
	if known := knownCalls.Load(); known != nil {
		if err := seedKnownCalls(gridStore, known); err != nil {
			log.Printf("Warning: failed to seed known calls into grid database: %v", err)
		}
	}
	cache := newGridCache(cfg.GridCacheSize, time.Duration(cfg.GridCacheTTLSec)*time.Second)
	gridTTL := time.Duration(cfg.GridTTLDays) * 24 * time.Hour
	gridUpdater, gridUpdateState, stopGridWriter, gridLookup := startGridWriter(gridStore, time.Duration(cfg.GridFlushSec)*time.Second, cache, gridTTL)
	defer func() {
		if stopGridWriter != nil {
			stopGridWriter()
		}
	}()

	if cfg.KnownCalls.Enabled && knownCallsURL != "" && knownCallsPath != "" {
		if knownCalls.Load() != nil {
			startKnownCallScheduler(cfg.KnownCalls, &knownCalls, gridStore)
		} else {
			log.Printf("Warning: known calls scheduler disabled (no initial data); ensure %s is reachable", cfg.KnownCalls.URL)
		}
	} else if cfg.KnownCalls.Enabled {
		log.Printf("Warning: known calls download enabled but url or file missing")
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
	deduplicator := dedup.NewDeduplicator(dedupWindow, cfg.Dedup.PreferStrongerSNR, cfg.Dedup.OutputBufferSize)
	deduplicator.Start()
	if dedupWindow > 0 {
		log.Printf("Deduplication active with %v window", dedupWindow)
	} else {
		log.Println("Deduplication disabled (cluster window=0); spots pass through unfiltered")
	}

	// Create command processor
	processor := commands.NewProcessor(spotBuffer)

	// Create and start telnet server
	telnetServer := telnet.NewServer(telnet.ServerOptions{
		Port:              cfg.Telnet.Port,
		WelcomeMessage:    cfg.Telnet.WelcomeMessage,
		DuplicateLoginMsg: cfg.Telnet.DuplicateLoginMsg,
		LoginGreeting:     cfg.Telnet.LoginGreeting,
		ClusterCall:       cfg.Server.NodeID,
		MaxConnections:    cfg.Telnet.MaxConnections,
		BroadcastWorkers:  cfg.Telnet.BroadcastWorkers,
		BroadcastQueue:    cfg.Telnet.BroadcastQueue,
		WorkerQueue:       cfg.Telnet.WorkerQueue,
		ClientBuffer:      cfg.Telnet.ClientBuffer,
		SkipHandshake:     cfg.Telnet.SkipHandshake,
	}, processor)

	err = telnetServer.Start()
	if err != nil {
		log.Fatalf("Failed to start telnet server: %v", err)
	}

	// Start the unified output processor once the telnet server is ready
	go processOutputSpots(deduplicator, spotBuffer, telnetServer, statsTracker, correctionIndex, cfg.CallCorrection, ctyDB, harmonicDetector, cfg.Harmonics, &knownCalls, freqAverager, cfg.SpotPolicy, ui, gridUpdater, gridLookup, unlicensedReporter, corrDebugLogger)

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name, ctyDB, skewStore, cfg.RBN.KeepSSIDSuffix)
		rbnClient.SetUnlicensedReporter(unlicensedReporter)
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
		rbnDigitalClient.SetUnlicensedReporter(unlicensedReporter)
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
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, pskrTopics, cfg.PSKReporter.Name, cfg.PSKReporter.Workers, ctyDB, skewStore, cfg.PSKReporter.AppendSpotterSSID)
		pskrClient.SetUnlicensedReporter(unlicensedReporter)
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
	go displayStatsWithFCC(statsInterval, statsTracker, deduplicator, spotBuffer, ctyDB, &knownCalls, telnetServer, ui, gridUpdateState, gridStore, cfg.FCCULS.DBPath)

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

	if corrDebugFile != nil {
		corrDebugFile.Close()
	}

	log.Println("Cluster stopped")
}

// makeUnlicensedReporter formats drop messages for the dashboard/logger and bumps stats.
func makeUnlicensedReporter(dash *dashboard, tracker *stats.Tracker) func(source, role, call, mode string, freq float64) {
	return func(source, role, call, mode string, freq float64) {
		if tracker != nil {
			tracker.IncrementUnlicensedDrops()
		}
		source = strings.ToUpper(strings.TrimSpace(source))
		role = strings.ToUpper(strings.TrimSpace(role))
		mode = strings.ToUpper(strings.TrimSpace(mode))
		call = strings.TrimSpace(strings.ToUpper(call))

		message := fmt.Sprintf("Unlicensed US %s %s dropped from %s %s @ %.1f kHz", role, call, source, mode, freq)
		if dash != nil {
			dash.AppendUnlicensed(message)
		} else {
			log.Println(message)
		}
	}
}

// displayStats prints statistics at the configured interval
// displayStatsWithFCC prints statistics at the configured interval. FCC metadata is refreshed
// from disk each tick so the dashboard reflects the latest download/build state.
func displayStatsWithFCC(interval time.Duration, tracker *stats.Tracker, dedup *dedup.Deduplicator, buf *buffer.RingBuffer, ctyDB *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns], telnetSrv *telnet.Server, dash *dashboard, gridStats *gridMetrics, gridDB *gridstore.Store, fccDBPath string) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prevSourceCounts := make(map[string]uint64)
	prevSourceModeCounts := make(map[string]uint64)
	fccSnap := loadFCCSnapshot(fccDBPath)

	for range ticker.C {
		// Refresh FCC snapshot each interval to reflect completed downloads/builds.
		fccSnap = loadFCCSnapshot(fccDBPath)

		sourceTotals := tracker.GetSourceCounts()
		sourceModeTotals := tracker.GetSourceModeCounts()

		rbnTotal := diffCounter(sourceTotals, prevSourceCounts, "RBN")
		rbnCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "CW")
		rbnRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "RTTY")

		rbnFTTotal := diffCounter(sourceTotals, prevSourceCounts, "RBN-DIGITAL")
		rbnFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-DIGITAL", "FT8")
		rbnFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-DIGITAL", "FT4")

		pskTotal := diffCounter(sourceTotals, prevSourceCounts, "PSKREPORTER")
		pskCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "CW")
		pskRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "RTTY")
		pskFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT8")
		pskFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT4")

		totalCorrections := tracker.CallCorrections()
		totalUnlicensed := tracker.UnlicensedDrops()
		totalFreqCorrections := tracker.FrequencyCorrections()
		totalHarmonics := tracker.HarmonicSuppressions()

		var queueDrops, clientDrops uint64
		var clientCount int
		if telnetSrv != nil {
			queueDrops, clientDrops = telnetSrv.BroadcastMetricSnapshot()
			clientCount = telnetSrv.GetClientCount()
		}

		combinedRBN := rbnTotal + rbnFTTotal
		lines := []string{
			fmt.Sprintf("%s   %s", formatUptimeLine(tracker.GetUptime()), formatMemoryLine(buf, dedup, ctyDB, knownPtr)),  // 1
			formatGridLineOrPlaceholder(gridStats, gridDB),                                                                // 2
			formatFCCLineOrPlaceholder(fccSnap),                                                                           // 3
			fmt.Sprintf("RBN: %d TOTAL / %d CW / %d RTTY / %d FT8 / %d FT4", combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4), // 4
			fmt.Sprintf("PSKReporter: %s TOTAL / %s CW / %s RTTY / %s FT8 / %s FT4",
				humanize.Comma(int64(pskTotal)),
				humanize.Comma(int64(pskCW)),
				humanize.Comma(int64(pskRTTY)),
				humanize.Comma(int64(pskFT8)),
				humanize.Comma(int64(pskFT4)),
			), // 5
			fmt.Sprintf("Corrected calls: %d (C) / %d (U) / %d (F) / %d (H)", totalCorrections, totalUnlicensed, totalFreqCorrections, totalHarmonics), // 6
			fmt.Sprintf("Telnet: %d clients. Drops: %d (Q) / %d (C)", clientCount, queueDrops, clientDrops),                                            // 7
		}

		prevSourceCounts = sourceTotals
		prevSourceModeCounts = sourceModeTotals

		if dash != nil {
			dash.SetStats(lines)
		} else {
			for _, line := range lines {
				log.Print(line)
			}
			log.Print("") // spacer between stats and status/messages
		}
	}
}

// processRBNSpots receives spots from RBN and sends to deduplicator
// This is the UNIFIED ARCHITECTURE path
// RBN → Deduplicator Input Channel
func processRBNSpots(client *rbn.Client, deduplicator *dedup.Deduplicator, source string) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		// Non-blocking send to avoid wedging ingest if dedup blocks.
		select {
		case dedupInput <- spot:
		default:
			count := drops.Add(1)
			if count == 1 || count%100 == 0 {
				log.Printf("%s: Dedup input full, dropping spot (total drops=%d)", source, count)
			}
		}
	}
	log.Printf("%s: Spot processing stopped", source)
}

// processPSKRSpots receives spots from PSKReporter and sends to deduplicator
// PSKReporter → Deduplicator Input Channel
func processPSKRSpots(client *pskreporter.Client, deduplicator *dedup.Deduplicator) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		// Non-blocking send to avoid backing up the PSK worker pool when dedup is slow.
		select {
		case dedupInput <- spot:
		default:
			count := drops.Add(1)
			if count == 1 || count%100 == 0 {
				log.Printf("PSKReporter: Dedup input full, dropping spot (total drops=%d)", count)
			}
		}
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
	knownCalls *atomic.Pointer[spot.KnownCallsigns],
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
	dash *dashboard,
	gridUpdate func(call, grid string),
	gridLookup func(call string) (string, bool),
	unlicensedReporter func(source, role, call, mode string, freq float64),
	corrDebugLogger *log.Logger,
) {
	outputChan := deduplicator.GetOutputChannel()

	for s := range outputChan {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("processOutputSpots panic: %v\n%s", r, debug.Stack())
				}
			}()

			if s == nil {
				return
			}

			s.RefreshBeaconFlag()

			if gridLookup != nil {
				// Backfill missing grids from the persisted store so downstream consumers
				// see metadata even when the upstream spot omitted it.
				if strings.TrimSpace(s.DXMetadata.Grid) == "" {
					if grid, ok := gridLookup(s.DXCall); ok {
						s.DXMetadata.Grid = grid
					}
				}
				if strings.TrimSpace(s.DEMetadata.Grid) == "" {
					if grid, ok := gridLookup(s.DECall); ok {
						s.DEMetadata.Grid = grid
					}
				}
			}
			if s.IsBeacon {
				// Beacons are tagged with a strong confidence so they still display a glyph.
				s.Confidence = "V"
			}

			if !s.IsBeacon && spotPolicy.MaxAgeSeconds > 0 {
				if time.Since(s.Time) > time.Duration(spotPolicy.MaxAgeSeconds)*time.Second {
					// log.Printf("Spot dropped (stale): %s at %.1fkHz (age=%ds)", s.DXCall, s.Frequency, int(time.Since(s.Time).Seconds()))
					return
				}
			}

			var suppress bool
			if telnet != nil && !s.IsBeacon {
				suppress = maybeApplyCallCorrectionWithLogger(s, correctionIdx, correctionCfg, ctyDB, knownCalls, tracker, dash, corrDebugLogger)
				if suppress {
					return
				}
				// Call correction can change the DX call; recompute beacon flag accordingly.
				s.RefreshBeaconFlag()
			}

			if !s.IsBeacon && harmonicDetector != nil && harmonicCfg.Enabled {
				if drop, fundamental, corroborators, deltaDB := harmonicDetector.ShouldDrop(s, time.Now().UTC()); drop {
					harmonicMsg := fmt.Sprintf("Harmonic suppressed: %s %.1f -> %.1f kHz (%d / %d dB)", s.DXCall, s.Frequency, fundamental, corroborators, deltaDB)
					harmonicMsgDash := harmonicMsg
					if dash != nil {
						harmonicMsgDash = fmt.Sprintf("Harmonic suppressed: %s [red]%.1f[-] -> [green]%.1f[-] kHz (%d / %d dB)", s.DXCall, s.Frequency, fundamental, corroborators, deltaDB)
					}
					if tracker != nil {
						tracker.IncrementHarmonicSuppressions()
					}
					if dash != nil {
						dash.AppendHarmonic(harmonicMsgDash)
					} else {
						log.Println(harmonicMsg)
					}
					return
				}
			}

			if !s.IsBeacon && freqAvg != nil && shouldAverageFrequency(s) {
				window := frequencyAverageWindow(spotPolicy)
				tolerance := frequencyAverageTolerance(spotPolicy)
				avg, corroborators, totalReports := freqAvg.Average(s.DXCall, s.Frequency, time.Now().UTC(), window, tolerance)
				rounded := math.Round(avg*10) / 10
				confidence := 0
				if totalReports > 0 {
					confidence = corroborators * 100 / totalReports
				}
				// Apply the averaged frequency when we have enough corroborators and the rounded
				// value actually differs from the reported frequency. We deliberately decouple
				// this apply threshold from the inclusion tolerance so sub-500 Hz shifts are
				// preserved instead of being discarded by the same 0.5 kHz gate.
				delta := math.Abs(rounded - s.Frequency)
				if corroborators >= spotPolicy.FrequencyAveragingMinReports && delta >= 0.05 {
					message := fmt.Sprintf("Frequency corrected: %s %.1f -> %.1f kHz (%d / %d%%)", s.DXCall, s.Frequency, rounded, corroborators, confidence)
					messageDash := message
					if dash != nil {
						messageDash = fmt.Sprintf("Frequency corrected: %s [red]%.1f[-] -> [green]%.1f[-] kHz (%d / %d%%)", s.DXCall, s.Frequency, rounded, corroborators, confidence)
					}
					s.Frequency = rounded
					if tracker != nil {
						tracker.IncrementFrequencyCorrections()
					}
					if dash != nil {
						dash.AppendFrequency(messageDash)
					} else {
						log.Println(message)
					}
				}
			}

			// Ensure CW/RTTY/SSB carry at least a placeholder confidence glyph when no correction applied.
			if !s.IsBeacon {
				modeUpper := strings.ToUpper(strings.TrimSpace(s.Mode))
				if (modeUpper == "CW" || modeUpper == "RTTY" || modeUpper == "SSB") && strings.TrimSpace(s.Confidence) == "" {
					s.Confidence = "?"
				}
			}

			// Final CTY/licensing gate runs after corrections so busted calls can be fixed first.
			if applyLicenseGate(s, ctyDB, unlicensedReporter) {
				return
			}

			if tracker != nil {
				modeKey := strings.ToUpper(strings.TrimSpace(s.Mode))
				if modeKey == "" {
					modeKey = string(s.SourceType)
				}
				tracker.IncrementMode(modeKey)

				sourceName := strings.ToUpper(strings.TrimSpace(s.SourceNode))
				if sourceName != "" {
					tracker.IncrementSource(sourceName)
					tracker.IncrementSourceMode(sourceName, modeKey)
				}
			}

			if gridUpdate != nil {
				if dxGrid := strings.TrimSpace(s.DXMetadata.Grid); dxGrid != "" {
					gridUpdate(s.DXCall, dxGrid)
				}
				if deGrid := strings.TrimSpace(s.DEMetadata.Grid); deGrid != "" {
					gridUpdate(s.DECall, deGrid)
				}
			}

			buf.Add(s)

			if telnet != nil {
				telnet.BroadcastSpot(s)
			}
		}()
	}
}

// applyLicenseGate runs the FCC/CTY check after all corrections and returns true when the spot should be dropped.
func applyLicenseGate(s *spot.Spot, ctyDB *cty.CTYDatabase, reporter func(source, role, call, mode string, freq float64)) bool {
	if s == nil {
		return false
	}
	if s.IsBeacon {
		return false
	}
	if ctyDB == nil {
		return false
	}

	dxInfo := effectivePrefixInfo(ctyDB, s.DXCall)
	deInfo := effectivePrefixInfo(ctyDB, s.DECall)

	// Refresh metadata from the final CTY match but preserve any grid data we already attached.
	dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
	deGrid := strings.TrimSpace(s.DEMetadata.Grid)
	s.DXMetadata = metadataFromPrefix(dxInfo)
	s.DEMetadata = metadataFromPrefix(deInfo)
	if dxGrid != "" {
		s.DXMetadata.Grid = dxGrid
	}
	if deGrid != "" {
		s.DEMetadata.Grid = deGrid
	}

	if dxInfo != nil && dxInfo.ADIF == 291 && !uls.IsLicensedUS(s.DXCall) {
		if reporter != nil {
			reporter(s.SourceNode, "DX", s.DXCall, s.Mode, s.Frequency)
		}
		return true
	}
	if deInfo != nil && deInfo.ADIF == 291 && !uls.IsLicensedUS(s.DECall) {
		if reporter != nil {
			reporter(s.SourceNode, "DE", s.DECall, s.Mode, s.Frequency)
		}
		return true
	}
	return false
}

func effectivePrefixInfo(ctyDB *cty.CTYDatabase, call string) *cty.PrefixInfo {
	if ctyDB == nil {
		return nil
	}
	if call == "" {
		return nil
	}
	info, ok := ctyDB.LookupCallsign(call)
	if !ok {
		info = nil
	}
	base := uls.NormalizeForLicense(call)
	if base != "" && base != call {
		if baseInfo, ok := ctyDB.LookupCallsign(base); ok && baseInfo != nil {
			info = baseInfo
		}
	}
	return info
}

func metadataFromPrefix(info *cty.PrefixInfo) spot.CallMetadata {
	if info == nil {
		return spot.CallMetadata{}
	}
	return spot.CallMetadata{
		Continent: info.Continent,
		Country:   info.Country,
		CQZone:    info.CQZone,
		ITUZone:   info.ITUZone,
		ADIF:      info.ADIF,
	}
}

func maybeApplyCallCorrection(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns], tracker *stats.Tracker, dash *dashboard) bool {
	return maybeApplyCallCorrectionWithLogger(spotEntry, idx, cfg, ctyDB, knownPtr, tracker, dash, nil)
}

func maybeApplyCallCorrectionWithLogger(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns], tracker *stats.Tracker, dash *dashboard, dbg *log.Logger) bool {
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
		MinConsensusReports:      cfg.MinConsensusReports,
		MinAdvantage:             cfg.MinAdvantage,
		MinConfidencePercent:     cfg.MinConfidencePercent,
		MaxEditDistance:          cfg.MaxEditDistance,
		RecencyWindow:            window,
		Strategy:                 cfg.Strategy,
		MinSNRCW:                 cfg.MinSNRCW,
		MinSNRRTTY:               cfg.MinSNRRTTY,
		DistanceModelCW:          cfg.DistanceModelCW,
		DistanceModelRTTY:        cfg.DistanceModelRTTY,
		Distance3ExtraReports:    cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:  cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence: cfg.Distance3ExtraConfidence,
		DistanceCacheSize:        cfg.DistanceCacheSize,
		DistanceCacheTTL:         time.Duration(cfg.DistanceCacheTTLSeconds) * time.Second,
		DebugLog:                 cfg.DebugLog,
		DebugLogger:              dbg,
		QualityBinHz:             cfg.QualityBinHz,
		QualityGoodThreshold:     cfg.QualityGoodThreshold,
		QualityNewCallIncrement:  cfg.QualityNewCallIncrement,
		QualityBustedDecrement:   cfg.QualityBustedDecrement,
	}
	others := idx.Candidates(spotEntry, now, window)
	entries := spotsToEntries(others)
	corrected, supporters, correctedConfidence, subjectConfidence, totalReporters, ok := spot.SuggestCallCorrection(spotEntry, entries, settings, now)

	var knownCall bool
	if knownPtr != nil {
		if known := knownPtr.Load(); known != nil {
			knownCall = known.Contains(spotEntry.DXCall)
		}
	}
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

	message := fmt.Sprintf("Call corrected: %s -> %s at %.1f kHz (%d / %d%%)",
		spotEntry.DXCall, corrected, spotEntry.Frequency, supporters, correctedConfidence)
	messageDash := message
	if dash != nil {
		messageDash = fmt.Sprintf("Call corrected: [red]%s[-] -> [green]%s[-] at %.1f kHz (%d / %d%%)",
			spotEntry.DXCall, corrected, spotEntry.Frequency, supporters, correctedConfidence)
	}

	if ctyDB != nil {
		if _, valid := ctyDB.LookupCallsign(corrected); valid {
			if dash != nil {
				dash.AppendCall(messageDash)
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
			spotEntry.Confidence = "B"
		}
		return false
	}

	if dash != nil {
		dash.AppendCall(messageDash)
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

// spotsToEntries converts []*spot.Spot to bandmap.SpotEntry using Hz units for frequency.
func spotsToEntries(spots []*spot.Spot) []bandmap.SpotEntry {
	if len(spots) == 0 {
		return nil
	}
	entries := make([]bandmap.SpotEntry, 0, len(spots))
	for _, s := range spots {
		if s == nil {
			continue
		}
		entries = append(entries, bandmap.SpotEntry{
			Call:    s.DXCall,
			Spotter: s.DECall,
			Mode:    s.Mode,
			FreqHz:  uint32(s.Frequency*1000 + 0.5),
			Time:    s.Time.Unix(),
			SNR:     s.Report,
		})
	}
	return entries
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
	case value >= 51:
		return "V"
	case value >= 25:
		return "P"
	default:
		return "?"
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
			delay := nextSkewRefreshDelay(cfg, time.Now().UTC())
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

func nextSkewRefreshDelay(cfg config.SkewConfig, now time.Time) time.Duration {
	hour, minute := skewRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func skewRefreshHourMinute(cfg config.SkewConfig) (int, int) {
	refresh := strings.TrimSpace(cfg.RefreshUTC)
	if refresh == "" {
		refresh = "00:30"
	}
	if parsed, err := time.Parse("15:04", refresh); err == nil {
		return parsed.Hour(), parsed.Minute()
	}
	return 0, 30
}

// startKnownCallScheduler downloads the known-calls file at the configured UTC
// time every day and updates the in-memory cache pointer after each refresh.
func startKnownCallScheduler(cfg config.KnownCallsConfig, knownPtr *atomic.Pointer[spot.KnownCallsigns], store *gridstore.Store) {
	if knownPtr == nil {
		return
	}
	go func() {
		for {
			delay := nextKnownCallRefreshDelay(cfg, time.Now().UTC())
			timer := time.NewTimer(delay)
			<-timer.C
			if fresh, err := refreshKnownCallsigns(cfg); err != nil {
				log.Printf("Warning: scheduled known calls download failed: %v", err)
			} else {
				knownPtr.Store(fresh)
				log.Printf("Scheduled known calls download complete (%d entries)", fresh.Count())
				if store != nil {
					if err := seedKnownCalls(store, fresh); err != nil {
						log.Printf("Warning: failed to reseed known calls into grid database: %v", err)
					}
				}
			}
		}
	}()
}

// refreshKnownCallsigns downloads the known calls file, writes it to disk, and
// returns the parsed cache.
func refreshKnownCallsigns(cfg config.KnownCallsConfig) (*spot.KnownCallsigns, error) {
	url := strings.TrimSpace(cfg.URL)
	path := strings.TrimSpace(cfg.File)
	if url == "" {
		return nil, errors.New("known calls: URL is empty")
	}
	if path == "" {
		return nil, errors.New("known calls: file path is empty")
	}
	if err := downloadKnownCallFile(url, path); err != nil {
		return nil, err
	}
	return spot.LoadKnownCallsigns(path)
}

// downloadKnownCallFile streams the remote SCP file to a temp file and swaps it
// into place atomically so readers never see a partial write.
func downloadKnownCallFile(url, destination string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("known calls: build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("known calls: fetch failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("known calls: fetch failed: status %s", resp.Status)
	}

	dir := filepath.Dir(destination)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("known calls: create directory: %w", err)
		}
	}
	tmpDir := dir
	if tmpDir == "" {
		tmpDir = "."
	}
	tmpFile, err := os.CreateTemp(tmpDir, "knowncalls-*.tmp")
	if err != nil {
		return fmt.Errorf("known calls: create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return fmt.Errorf("known calls: copy body: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("known calls: finalize temp file: %w", err)
	}
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("known calls: remove old file: %w", err)
	}
	if err := os.Rename(tmpName, destination); err != nil {
		return fmt.Errorf("known calls: replace file: %w", err)
	}
	return nil
}

func nextKnownCallRefreshDelay(cfg config.KnownCallsConfig, now time.Time) time.Duration {
	hour, minute := knownCallRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func knownCallRefreshHourMinute(cfg config.KnownCallsConfig) (int, int) {
	refresh := strings.TrimSpace(cfg.RefreshUTC)
	if refresh == "" {
		refresh = "01:00"
	}
	if parsed, err := time.Parse("15:04", refresh); err == nil {
		return parsed.Hour(), parsed.Minute()
	}
	return 1, 0
}

func formatGridLine(metrics *gridMetrics, store *gridstore.Store) string {
	updatesSinceStart := metrics.learnedTotal.Load()
	cacheLookups := metrics.cacheLookups.Load()
	cacheHits := metrics.cacheHits.Load()

	dbTotal := int64(-1)
	if store != nil {
		if count, err := store.Count(); err == nil {
			dbTotal = count
		} else {
			log.Printf("Warning: gridstore count failed: %v", err)
		}
	}
	hitRate := 0.0
	if cacheLookups > 0 {
		hitRate = float64(cacheHits) * 100 / float64(cacheLookups)
	}

	if dbTotal >= 0 {
		return fmt.Sprintf("Grid database: %s (UPDATED) / %s (TOTAL) / %.1f%%",
			humanize.Comma(int64(updatesSinceStart)),
			humanize.Comma(dbTotal),
			hitRate)
	}
	return fmt.Sprintf("Grid database: %s (UPDATED) / %.1f%%",
		humanize.Comma(int64(updatesSinceStart)),
		hitRate)
}

type fccSnapshot struct {
	HDCount   int64
	AMCount   int64
	DBSize    int64
	UpdatedAt time.Time
	Path      string
}

func formatFCCLine(fcc *fccSnapshot) string {
	if fcc == nil {
		return ""
	}
	ts := ""
	if !fcc.UpdatedAt.IsZero() {
		ts = fcc.UpdatedAt.UTC().Format("01-02-2006 15:04:05")
	}
	return fmt.Sprintf("FCC ULS: %s records. Last updated %s", humanize.Comma(fcc.HDCount), ts)
}

func formatGridLineOrPlaceholder(metrics *gridMetrics, store *gridstore.Store) string {
	if metrics == nil {
		return "Grid database: (not available)"
	}
	return formatGridLine(metrics, store)
}

func formatFCCLineOrPlaceholder(fcc *fccSnapshot) string {
	if fcc == nil {
		return "FCC ULS: (not available)"
	}
	return formatFCCLine(fcc)
}

func seedKnownCalls(store *gridstore.Store, known *spot.KnownCallsigns) error {
	if store == nil || known == nil {
		return nil
	}
	if err := store.ClearKnownFlags(); err != nil {
		return err
	}
	calls := known.List()
	if len(calls) == 0 {
		return nil
	}
	records := make([]gridstore.Record, 0, len(calls))
	now := time.Now().UTC()
	for _, call := range calls {
		records = append(records, gridstore.Record{
			Call:         call,
			IsKnown:      true,
			Observations: 0,
			FirstSeen:    now,
			UpdatedAt:    now,
		})
	}
	return store.UpsertBatch(records)
}

func startGridWriter(store *gridstore.Store, flushInterval time.Duration, cache *gridCache, ttl time.Duration) (func(call, grid string), *gridMetrics, func(), func(call string) (string, bool)) {
	if store == nil {
		return nil, nil, nil, nil
	}
	if flushInterval <= 0 {
		flushInterval = 60 * time.Second
	}
	metrics := &gridMetrics{}
	type update struct {
		call string
		grid string
	}
	updates := make(chan update, 8192)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		var ttlTicker *time.Ticker
		var ttlCh <-chan time.Time
		if ttl > 0 {
			ttlTicker = time.NewTicker(ttl)
			defer ttlTicker.Stop()
			ttlCh = ttlTicker.C
		}

		pending := make(map[string]update)
		flush := func() {
			if len(pending) == 0 {
				return
			}
			batch := make([]gridstore.Record, 0, len(pending))
			now := time.Now().UTC()
			for _, u := range pending {
				rec := gridstore.Record{
					Call:         u.call,
					Grid:         sqlNullString(u.grid),
					Observations: 1,
					FirstSeen:    now,
					UpdatedAt:    now,
				}
				batch = append(batch, rec)
			}
			if err := store.UpsertBatch(batch); err != nil {
				log.Printf("Warning: gridstore batch upsert failed: %v", err)
			}
			metrics.learnedTotal.Add(uint64(len(batch)))
			clear(pending)
		}

		for {
			select {
			case u, ok := <-updates:
				if !ok {
					flush()
					return
				}
				pending[u.call] = u
				if len(pending) >= 500 {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ttlCh:
				cutoff := time.Now().UTC().Add(-ttl)
				if removed, err := store.PurgeOlderThan(cutoff); err != nil {
					log.Printf("Warning: gridstore TTL purge failed: %v", err)
				} else if removed > 0 {
					log.Printf("Gridstore: purged %d entries older than %v", removed, ttl)
				}
			}
		}
	}()

	updateFn := func(call, grid string) {
		call = strings.TrimSpace(strings.ToUpper(call))
		grid = strings.TrimSpace(strings.ToUpper(grid))
		if call == "" || len(grid) < 4 {
			return
		}
		if cache != nil && !cache.shouldUpdate(call, grid, store) {
			return
		}
		select {
		case updates <- update{call: call, grid: grid}:
		default:
			// Drop silently to avoid backpressure on the spot pipeline.
		}
	}

	stopFn := func() {
		close(updates)
		<-done
	}

	lookupFn := func(call string) (string, bool) {
		call = strings.TrimSpace(strings.ToUpper(call))
		if call == "" {
			return "", false
		}
		if cache != nil {
			if grid, ok := cache.lookupWithMetrics(call, metrics); ok {
				return grid, true
			}
		}
		if store == nil {
			return "", false
		}
		rec, err := store.Get(call)
		if err != nil || rec == nil || !rec.Grid.Valid {
			return "", false
		}
		grid := strings.ToUpper(strings.TrimSpace(rec.Grid.String))
		if grid == "" {
			return "", false
		}
		if cache != nil {
			cache.add(call, grid)
		}
		return grid, true
	}

	return updateFn, metrics, stopFn, lookupFn
}

// loadFCCSnapshot opens the FCC ULS database to report simple stats for the dashboard.
func loadFCCSnapshot(path string) *fccSnapshot {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}
	db, err := sql.Open("sqlite", path+"?_busy_timeout=5000")
	if err != nil {
		log.Printf("Warning: FCC ULS open failed: %v", err)
		return nil
	}
	defer db.Close()

	count := func(table string) int64 {
		var c int64
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&c); err != nil {
			log.Printf("Warning: FCC ULS count %s failed: %v", table, err)
			return 0
		}
		return c
	}

	snap := &fccSnapshot{
		HDCount:   count("HD"),
		AMCount:   count("AM"),
		DBSize:    info.Size(),
		UpdatedAt: info.ModTime(),
		Path:      path,
	}
	return snap
}

func sqlNullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func formatMemoryLine(buf *buffer.RingBuffer, dedup *dedup.Deduplicator, ctyDB *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns]) string {
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
	var known *spot.KnownCallsigns
	if knownPtr != nil {
		known = knownPtr.Load()
	}
	if known != nil {
		knownMB = bytesToMB(uint64(known.Count() * knownCallEntryBytes))
		lookups, hits := known.StatsDX()
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
