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
	httppprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	pprof "runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dxcluster/archive"
	"dxcluster/bandmap"
	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/filter"
	"dxcluster/gridstore"
	"dxcluster/peer"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
	"dxcluster/uls"

	"github.com/dustin/go-humanize"
	"golang.org/x/term"
)

const (
	dedupeEntryBytes    = 32
	ctyCacheEntryBytes  = 96
	knownCallEntryBytes = 24
	sourceModeDelimiter = "|"
	defaultConfigPath   = "data/config"
	legacyConfigPath    = "config.yaml"
	envConfigPath       = "DXC_CONFIG_PATH"

	// envGridDBCheckOnMiss overrides the config-driven grid_db_check_on_miss at runtime.
	// When true, grid updates may synchronously consult SQLite on cache miss to avoid
	// redundant writes. When false, the hot path never blocks on that read and may
	// perform extra batched writes instead.
	envGridDBCheckOnMiss = "DXC_GRID_DB_CHECK_ON_MISS"
)

var licCache = newLicenseCache(5 * time.Minute)

// licenseCache caches FCC license checks to avoid repeated lookups on hot paths.
type licenseCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]licenseEntry
}

type licenseEntry struct {
	licensed bool
	at       time.Time
}

func newLicenseCache(ttl time.Duration) *licenseCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &licenseCache{
		ttl:     ttl,
		entries: make(map[string]licenseEntry),
	}
}

func (lc *licenseCache) get(call string, now time.Time) (bool, bool) {
	if lc == nil || call == "" {
		return false, false
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	entry, ok := lc.entries[call]
	if !ok {
		return false, false
	}
	if lc.ttl > 0 && now.Sub(entry.at) > lc.ttl {
		delete(lc.entries, call)
		return false, false
	}
	return entry.licensed, true
}

func (lc *licenseCache) set(call string, licensed bool, now time.Time) {
	if lc == nil || call == "" {
		return
	}
	lc.mu.Lock()
	lc.entries[call] = licenseEntry{licensed: licensed, at: now}
	lc.mu.Unlock()
}

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
// it may consult SQLite (when store is non-nil) to avoid duplicate writes when
// the database already holds the same grid. Passing store=nil disables the DB
// read and treats cache misses as updates (useful for A/B testing CPU/latency).
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

func isStdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

func loadClusterConfig() (*config.Config, string, error) {
	candidates := make([]string, 0, 3)
	if envPath := strings.TrimSpace(os.Getenv(envConfigPath)); envPath != "" {
		candidates = append(candidates, envPath)
	}
	candidates = append(candidates, defaultConfigPath, legacyConfigPath)

	var lastErr error
	for _, path := range candidates {
		if path == "" {
			continue
		}
		cfg, err := config.Load(path)
		if err != nil {
			if os.IsNotExist(err) {
				lastErr = err
				continue
			}
			return nil, path, err
		}
		return cfg, cfg.LoadedFrom, nil
	}
	return nil, "", fmt.Errorf("unable to load config; tried %s (last error: %v)", strings.Join(candidates, ", "), lastErr)
}

func gridDBCheckOnMissEnabled(cfg *config.Config) (bool, string) {
	enabled := true
	source := "default"
	if cfg != nil && cfg.GridDBCheckOnMiss != nil {
		enabled = *cfg.GridDBCheckOnMiss
		source = strings.TrimSpace(cfg.LoadedFrom)
		if source == "" {
			source = "config"
		}
	}

	raw := strings.TrimSpace(os.Getenv(envGridDBCheckOnMiss))
	if raw == "" {
		return enabled, source
	}

	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("Gridstore: ignoring invalid %s=%q; using %s value=%v", envGridDBCheckOnMiss, raw, source, enabled)
		return enabled, source
	}

	return parsed, envGridDBCheckOnMiss
}

func main() {
	// Load configuration
	cfg, configSource, err := loadClusterConfig()
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	log.Printf("Loaded configuration from %s", configSource)

	uiMode := strings.ToLower(strings.TrimSpace(cfg.UI.Mode))
	renderAllowed := isStdoutTTY()

	var ui uiSurface
	switch uiMode {
	case "headless":
		log.Printf("UI disabled (mode=headless)")
	case "tview":
		if !renderAllowed {
			log.Printf("UI disabled (tview requires an interactive console)")
		} else {
			ui = newDashboard(true)
		}
	case "ansi":
		if !renderAllowed {
			log.Printf("UI disabled (ansi renderer requires an interactive console)")
		} else {
			ui = newANSIConsole(cfg.UI, renderAllowed)
		}
	default:
		log.Printf("UI mode %q not recognized; defaulting to headless", uiMode)
	}

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callCacheTTL := time.Duration(cfg.CallCache.TTLSeconds) * time.Second
	spot.ConfigureNormalizeCallCache(cfg.CallCache.Size, callCacheTTL)
	rbn.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
	pskreporter.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
	filter.SetDefaultModeSelection(cfg.Filter.DefaultModes)
	filter.SetDefaultSourceSelection(cfg.Filter.DefaultSources)
	if err := filter.EnsureUserDataDir(); err != nil {
		log.Printf("Warning: unable to initialize filter directory: %v", err)
	}

	// Print the configuration (stdout only when not using the dashboard)
	if ui == nil {
		cfg.Print()
	} else {
		log.Printf("Configuration loaded for %s (%s)", cfg.Server.Name, cfg.Server.NodeID)
	}

	// Optional call-correction decision logger (asynchronous SQLite writer).
	var corrLogger spot.CorrectionTraceLogger
	if cfg.CallCorrection.DebugLog {
		logger, err := spot.NewDecisionLogger(cfg.CallCorrection.DebugLogFile, 0)
		if err != nil {
			log.Printf("Warning: unable to start call-correction decision logger: %v", err)
		} else {
			corrLogger = logger
			path := spot.DecisionLogPath(cfg.CallCorrection.DebugLogFile, time.Now().UTC())
			log.Printf("Call correction decision logging to %s (SQLite, non-blocking)", path)
		}
	}

	// Toggle FCC ULS lookups independently of the downloader so disabled configs
	// can keep the DB on disk without performing license checks.
	uls.SetLicenseChecksEnabled(cfg.FCCULS.Enabled)

	// Start the FCC ULS downloader in the background (does not block spot processing)
	uls.StartBackground(ctx, cfg.FCCULS)

	// Load CTY database for callsign validation and schedule refreshes.
	var ctyDB atomic.Pointer[cty.CTYDatabase]
	ctyPath := strings.TrimSpace(cfg.CTY.File)
	ctyURL := strings.TrimSpace(cfg.CTY.URL)
	if cfg.CTY.Enabled && ctyPath != "" {
		if _, err := os.Stat(ctyPath); err != nil && errors.Is(err, os.ErrNotExist) && ctyURL != "" {
			if fresh, refreshErr := refreshCTYDatabase(cfg.CTY); refreshErr != nil {
				log.Printf("Warning: CTY download failed: %v", refreshErr)
			} else {
				ctyDB.Store(fresh)
				log.Printf("Downloaded CTY database from %s", ctyURL)
			}
		}
	}
	if cfg.CTY.Enabled && ctyDB.Load() == nil && ctyPath != "" {
		if loaded, loadErr := cty.LoadCTYDatabase(ctyPath); loadErr != nil {
			log.Printf("Warning: failed to load CTY database: %v", loadErr)
		} else {
			ctyDB.Store(loaded)
			log.Printf("Loaded CTY database from %s", ctyPath)
		}
	}
	ctyLookup := func() *cty.CTYDatabase {
		return ctyDB.Load()
	}
	if cfg.CTY.Enabled && ctyURL != "" && ctyPath != "" {
		startCTYScheduler(ctx, cfg.CTY, &ctyDB)
	} else if cfg.CTY.Enabled {
		log.Printf("Warning: CTY download enabled but url or file missing")
	}
	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	spot.ConfigureBaudotWeights(cfg.CallCorrection.BaudotWeights.Insert, cfg.CallCorrection.BaudotWeights.Delete, cfg.CallCorrection.BaudotWeights.Sub, cfg.CallCorrection.BaudotWeights.Scale)
	if priors := strings.TrimSpace(cfg.CallCorrection.QualityPriorsFile); priors != "" {
		if n, err := spot.LoadCallQualityPriors(priors, cfg.CallCorrection.QualityBinHz); err != nil {
			log.Printf("Warning: failed to load quality priors from %s: %v", priors, err)
		} else {
			log.Printf("Loaded %d call quality priors from %s", n, priors)
		}
	}
	var spotterReliability spot.SpotterReliability
	if relPath := strings.TrimSpace(cfg.CallCorrection.SpotterReliabilityFile); relPath != "" {
		if rel, n, err := spot.LoadSpotterReliability(relPath); err != nil {
			log.Printf("Warning: failed to load spotter reliability from %s: %v", relPath, err)
		} else {
			spotterReliability = rel
			log.Printf("Loaded %d spotter reliability weights from %s", n, relPath)
		}
	}
	adaptiveMinReports := spot.NewAdaptiveMinReports(cfg.CallCorrection)
	refresher := newAdaptiveRefresher(adaptiveMinReports, cfg.CallCorrection.AdaptiveRefreshByBand, noopRefresh)
	if refresher != nil {
		refresher.Start()
		defer refresher.Stop()
	}
	if cfg.FCCULS.Enabled && strings.TrimSpace(cfg.FCCULS.DBPath) != "" {
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
		correctionIndex.StartCleanup(time.Minute, callCorrectionWindow(cfg.CallCorrection))
	}

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

	gridDBCheckOnMiss, gridDBCheckSource := gridDBCheckOnMissEnabled(cfg)
	log.Printf("Gridstore: db_check_on_miss=%v (source=%s)", gridDBCheckOnMiss, gridDBCheckSource)

	cache := newGridCache(cfg.GridCacheSize, time.Duration(cfg.GridCacheTTLSec)*time.Second)
	gridTTL := time.Duration(cfg.GridTTLDays) * 24 * time.Hour
	gridUpdater, gridUpdateState, stopGridWriter, gridLookup := startGridWriter(gridStore, time.Duration(cfg.GridFlushSec)*time.Second, cache, gridTTL, gridDBCheckOnMiss)
	defer func() {
		if stopGridWriter != nil {
			stopGridWriter()
		}
	}()

	if cfg.KnownCalls.Enabled && knownCallsURL != "" && knownCallsPath != "" {
		if knownCalls.Load() != nil {
			startKnownCallScheduler(ctx, cfg.KnownCalls, &knownCalls, gridStore)
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
			startSkewScheduler(ctx, cfg.Skew, skewStore)
		} else {
			log.Printf("Warning: RBN skew scheduler disabled (no initial data); ensure %s is reachable", cfg.Skew.URL)
			skewStore = nil
		}
	}

	freqAverager := spot.NewFrequencyAverager()
	freqAverager.StartCleanup(time.Minute, frequencyAverageWindow(cfg.SpotPolicy))
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
		harmonicDetector.StartCleanup(time.Minute)
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

	secondaryWindow := time.Duration(cfg.Dedup.SecondaryWindowSeconds) * time.Second
	var secondaryDeduper *dedup.SecondaryDeduper
	if secondaryWindow > 0 {
		secondaryDeduper = dedup.NewSecondaryDeduper(secondaryWindow, cfg.Dedup.SecondaryPreferStrong)
		secondaryDeduper.Start()
		log.Printf("Secondary deduplication active with %v window (broadcast-only)", secondaryWindow)
	} else {
		log.Println("Secondary deduplication disabled; all spots broadcast")
	}

	modeSeeds := make([]spot.ModeSeed, 0, len(cfg.ModeInference.DigitalSeeds))
	for _, seed := range cfg.ModeInference.DigitalSeeds {
		modeSeeds = append(modeSeeds, spot.ModeSeed{
			FrequencyKHz: seed.FrequencyKHz,
			Mode:         seed.Mode,
		})
	}
	modeAssigner := spot.NewModeAssigner(spot.ModeInferenceSettings{
		DXFreqCacheTTL:        time.Duration(cfg.ModeInference.DXFreqCacheTTLSeconds) * time.Second,
		DXFreqCacheSize:       cfg.ModeInference.DXFreqCacheSize,
		DigitalWindow:         time.Duration(cfg.ModeInference.DigitalWindowSeconds) * time.Second,
		DigitalMinCorroborate: cfg.ModeInference.DigitalMinCorroborators,
		DigitalSeedTTL:        time.Duration(cfg.ModeInference.DigitalSeedTTLSeconds) * time.Second,
		DigitalCacheSize:      cfg.ModeInference.DigitalCacheSize,
		DigitalSeeds:          modeSeeds,
	})
	log.Printf("Mode inference: dx_cache=%d ttl=%ds digital_window=%ds min_corrob=%d seeds=%d seed_ttl=%ds",
		cfg.ModeInference.DXFreqCacheSize,
		cfg.ModeInference.DXFreqCacheTTLSeconds,
		cfg.ModeInference.DigitalWindowSeconds,
		cfg.ModeInference.DigitalMinCorroborators,
		len(modeSeeds),
		cfg.ModeInference.DigitalSeedTTLSeconds)

	// Start peering manager (DXSpider PC protocol) if enabled.
	var peerManager *peer.Manager
	if cfg.Peering.Enabled {
		pm, err := peer.NewManager(cfg.Peering, cfg.Peering.LocalCallsign, deduplicator.GetInputChannel(), cfg.SpotPolicy.MaxAgeSeconds)
		if err != nil {
			log.Fatalf("Failed to init peering manager: %v", err)
		}
		if err := pm.Start(ctx); err != nil {
			log.Fatalf("Failed to start peering manager: %v", err)
		}
		peerManager = pm
		log.Printf("Peering: listen_port=%d peers=%d hop=%d keepalive=%ds", cfg.Peering.ListenPort, len(cfg.Peering.Peers), cfg.Peering.HopCount, cfg.Peering.KeepaliveSeconds)
	}

	// Initialize archive writer (optional) before wiring consumers that need read access.
	var archiveWriter *archive.Writer
	if cfg.Archive.Enabled {
		if w, err := archive.NewWriter(cfg.Archive); err != nil {
			log.Printf("Warning: archive disabled due to init error: %v", err)
		} else {
			archiveWriter = w
			archiveWriter.Start()
			log.Printf("Archive: writing to %s (batch=%d/%dms queue=%d cleanup=%ds ft_retention=%ds other_retention=%ds)", cfg.Archive.DBPath, cfg.Archive.BatchSize, cfg.Archive.BatchIntervalMS, cfg.Archive.QueueSize, cfg.Archive.CleanupIntervalSeconds, cfg.Archive.RetentionFTSeconds, cfg.Archive.RetentionDefaultSeconds)
			defer archiveWriter.Stop()
		}
	}

	// Create command processor (SHOW/DX reads from archive when available, otherwise ring buffer)
	processor := commands.NewProcessor(spotBuffer, archiveWriter, deduplicator.GetInputChannel())

	// Create and start telnet server
	telnetServer := telnet.NewServer(telnet.ServerOptions{
		Port:                   cfg.Telnet.Port,
		WelcomeMessage:         cfg.Telnet.WelcomeMessage,
		DuplicateLoginMsg:      cfg.Telnet.DuplicateLoginMsg,
		LoginGreeting:          cfg.Telnet.LoginGreeting,
		ClusterCall:            cfg.Server.NodeID,
		MaxConnections:         cfg.Telnet.MaxConnections,
		BroadcastWorkers:       cfg.Telnet.BroadcastWorkers,
		BroadcastQueue:         cfg.Telnet.BroadcastQueue,
		WorkerQueue:            cfg.Telnet.WorkerQueue,
		ClientBuffer:           cfg.Telnet.ClientBuffer,
		BroadcastBatchInterval: time.Duration(cfg.Telnet.BroadcastBatchIntervalMS) * time.Millisecond,
		Transport:              cfg.Telnet.Transport,
		EchoMode:               cfg.Telnet.EchoMode,
		SkipHandshake:          cfg.Telnet.SkipHandshake,
		LoginLineLimit:         cfg.Telnet.LoginLineLimit,
		CommandLineLimit:       cfg.Telnet.CommandLineLimit,
	}, processor)

	err = telnetServer.Start()
	if err != nil {
		log.Fatalf("Failed to start telnet server: %v", err)
	}
	// Hook peering raw passthrough (e.g., PC26) into telnet broadcast once available.
	if peerManager != nil {
		peerManager.SetRawBroadcast(telnetServer.BroadcastRaw)
		peerManager.SetWWVBroadcast(telnetServer.BroadcastWWV)
		peerManager.SetAnnouncementBroadcast(telnetServer.BroadcastAnnouncement)
		peerManager.SetDirectMessage(telnetServer.SendDirectMessage)
		peerManager.SetUserCountProvider(telnetServer.GetClientCount)
	}

	// Start the unified output processor once the telnet server is ready
	var lastOutput atomic.Int64
	go processOutputSpots(deduplicator, secondaryDeduper, modeAssigner, spotBuffer, telnetServer, peerManager, statsTracker, correctionIndex, cfg.CallCorrection, ctyLookup, harmonicDetector, cfg.Harmonics, &knownCalls, freqAverager, cfg.SpotPolicy, ui, gridUpdater, gridLookup, unlicensedReporter, corrLogger, adaptiveMinReports, refresher, spotterReliability, cfg.RBN.KeepSSIDSuffix, archiveWriter, &lastOutput)
	startPipelineHealthMonitor(ctx, deduplicator, &lastOutput, peerManager)

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name, ctyLookup, skewStore, cfg.RBN.KeepSSIDSuffix, cfg.RBN.SlotBuffer)
		rbnClient.SetTelnetTransport(cfg.RBN.TelnetTransport)
		rbnClient.SetUnlicensedReporter(unlicensedReporter)
		if cfg.RBN.KeepaliveSec > 0 {
			rbnClient.EnableKeepalive(time.Duration(cfg.RBN.KeepaliveSec) * time.Second)
		}
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN CW/RTTY: %v", err)
		} else {
			go processRBNSpots(rbnClient, deduplicator, "RBN-CW", cfg.SpotPolicy)
			log.Println("RBN CW/RTTY client feeding spots into unified dedup engine")
		}
	}

	// Connect to RBN Digital feed if enabled (port 7001 - FT4/FT8)
	// RBN Digital spots go INTO the deduplicator input channel
	var rbnDigitalClient *rbn.Client
	if cfg.RBNDigital.Enabled {
		rbnDigitalClient = rbn.NewClient(cfg.RBNDigital.Host, cfg.RBNDigital.Port, cfg.RBNDigital.Callsign, cfg.RBNDigital.Name, ctyLookup, skewStore, cfg.RBNDigital.KeepSSIDSuffix, cfg.RBNDigital.SlotBuffer)
		rbnDigitalClient.SetTelnetTransport(cfg.RBNDigital.TelnetTransport)
		rbnDigitalClient.SetUnlicensedReporter(unlicensedReporter)
		if cfg.RBNDigital.KeepaliveSec > 0 {
			rbnDigitalClient.EnableKeepalive(time.Duration(cfg.RBNDigital.KeepaliveSec) * time.Second)
		}
		err = rbnDigitalClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN Digital: %v", err)
		} else {
			go processRBNSpots(rbnDigitalClient, deduplicator, "RBN-FT", cfg.SpotPolicy)
			log.Println("RBN Digital (FT4/FT8) client feeding spots into unified dedup engine")
		}
	}

	// Connect to human/relay telnet feed if enabled (upstream cluster or operator-submitted spots)
	var humanTelnetClient *rbn.Client
	if cfg.HumanTelnet.Enabled {
		rawPassthrough := make(chan string, 256)
		go func() {
			for line := range rawPassthrough {
				if telnetServer == nil {
					continue
				}
				if kind := wwvKindFromLine(line); kind != "" {
					telnetServer.BroadcastWWV(kind, line)
					continue
				}
				if announcement := announcementFromLine(line); announcement != "" {
					telnetServer.BroadcastAnnouncement(announcement)
				}
			}
		}()

		humanTelnetClient = rbn.NewClient(cfg.HumanTelnet.Host, cfg.HumanTelnet.Port, cfg.HumanTelnet.Callsign, cfg.HumanTelnet.Name, ctyLookup, skewStore, cfg.HumanTelnet.KeepSSIDSuffix, cfg.HumanTelnet.SlotBuffer)
		humanTelnetClient.SetTelnetTransport(cfg.HumanTelnet.TelnetTransport)
		humanTelnetClient.UseMinimalParser()
		humanTelnetClient.SetRawPassthrough(rawPassthrough)
		if cfg.HumanTelnet.KeepaliveSec > 0 {
			// Prevent idle disconnects on upstream telnet feeds by sending periodic CRLF.
			humanTelnetClient.EnableKeepalive(time.Duration(cfg.HumanTelnet.KeepaliveSec) * time.Second)
		}
		humanTelnetClient.SetUnlicensedReporter(unlicensedReporter)
		err = humanTelnetClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to human/relay telnet feed: %v", err)
		} else {
			go processHumanTelnetSpots(humanTelnetClient, deduplicator, "HUMAN-TELNET", cfg.SpotPolicy)
			log.Println("Human/relay telnet client feeding spots into unified dedup engine")
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
		pskrClient = pskreporter.NewClient(cfg.PSKReporter.Broker, cfg.PSKReporter.Port, pskrTopics, cfg.PSKReporter.Name, cfg.PSKReporter.Workers, ctyLookup, skewStore, cfg.PSKReporter.AppendSpotterSSID, cfg.PSKReporter.SpotChannelSize)
		pskrClient.SetUnlicensedReporter(unlicensedReporter)
		err = pskrClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		} else {
			go processPSKRSpots(pskrClient, deduplicator, cfg.SpotPolicy)
			log.Println("PSKReporter client feeding spots into unified dedup engine")
		}
	}

	// Start stats display goroutine
	statsInterval := time.Duration(cfg.Stats.DisplayIntervalSeconds) * time.Second
	go displayStatsWithFCC(statsInterval, statsTracker, deduplicator, secondaryDeduper, spotBuffer, ctyLookup, &knownCalls, telnetServer, ui, gridUpdateState, gridStore, cfg.FCCULS.DBPath)

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
	if cfg.HumanTelnet.Enabled {
		log.Printf("Receiving human/relay spots from %s:%d...", cfg.HumanTelnet.Host, cfg.HumanTelnet.Port)
	}
	if cfg.Dedup.ClusterWindowSeconds > 0 {
		log.Printf("Unified deduplication active: %d second window", cfg.Dedup.ClusterWindowSeconds)
	} else {
		log.Println("Unified deduplication bypassed (window=0); duplicates are not filtered")
	}
	log.Println("Architecture: ALL sources -> Dedup Engine -> Ring Buffer -> Clients")
	log.Printf("Statistics will be displayed every %d seconds...", cfg.Stats.DisplayIntervalSeconds)
	log.Println("---")
	maybeStartHeapLogger()
	maybeStartDiagServer()

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal: %v", sig)
	log.Println("Shutting down gracefully...")

	// Stop periodic cleanup loops
	if freqAverager != nil {
		freqAverager.StopCleanup()
	}
	if harmonicDetector != nil {
		harmonicDetector.StopCleanup()
	}
	if correctionIndex != nil {
		correctionIndex.StopCleanup()
	}

	// Stop deduplicator
	if deduplicator != nil {
		deduplicator.Stop()
	}
	if peerManager != nil {
		peerManager.Stop()
	}
	if secondaryDeduper != nil {
		secondaryDeduper.Stop()
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

	if corrLogger != nil {
		if err := corrLogger.Close(); err != nil {
			log.Printf("Warning: call-correction decision logger close: %v", err)
		}
		dropped := corrLogger.Dropped()
		if dropped > 0 {
			log.Printf("Call-correction decision logger dropped %d entries under load", dropped)
		}
	}

	log.Println("Cluster stopped")
}

// makeUnlicensedReporter formats drop messages for the dashboard/logger and bumps stats.
func makeUnlicensedReporter(dash uiSurface, tracker *stats.Tracker) func(source, role, call, mode string, freq float64) {
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
			colored := fmt.Sprintf("Unlicensed US %s [red]%s[-] dropped from %s %s @ %.1f kHz", role, call, source, mode, freq)
			dash.AppendUnlicensed(colored)
			return
		}
		log.Println(message)
	}
}

// displayStats prints statistics at the configured interval
// displayStatsWithFCC prints statistics at the configured interval. FCC metadata is refreshed
// from disk each tick so the dashboard reflects the latest download/build state.
func displayStatsWithFCC(interval time.Duration, tracker *stats.Tracker, dedup *dedup.Deduplicator, secondary *dedup.SecondaryDeduper, buf *buffer.RingBuffer, ctyLookup func() *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns], telnetSrv *telnet.Server, dash uiSurface, gridStats *gridMetrics, gridDB *gridstore.Store, fccDBPath string) {
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

		// PSKReporter includes a per-mode breakdown in the stats ticker.
		pskTotal := diffCounter(sourceTotals, prevSourceCounts, "PSKREPORTER")
		pskCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "CW")
		pskRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "RTTY")
		pskFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT8")
		pskFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT4")
		pskMSK144 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "MSK144")

		totalCorrections := tracker.CallCorrections()
		totalUnlicensed := tracker.UnlicensedDrops()
		totalFreqCorrections := tracker.FrequencyCorrections()
		totalHarmonics := tracker.HarmonicSuppressions()

		var secondaryLine string
		if secondary != nil {
			secProcessed, secDupes, secCache := secondary.GetStats()
			_ = secCache
			forwarded := secProcessed - secDupes
			secondaryLine = fmt.Sprintf("Secondary dedup: %s in / %s dup / %s fwd", humanize.Comma(int64(secProcessed)), humanize.Comma(int64(secDupes)), humanize.Comma(int64(forwarded)))
		} else {
			secondaryLine = "Secondary dedup: disabled"
		}

		var queueDrops, clientDrops uint64
		var clientCount int
		if telnetSrv != nil {
			queueDrops, clientDrops = telnetSrv.BroadcastMetricSnapshot()
			clientCount = telnetSrv.GetClientCount()
		}

		combinedRBN := rbnTotal + rbnFTTotal
		lines := []string{
			fmt.Sprintf("%s   %s", formatUptimeLine(tracker.GetUptime()), formatMemoryLine(buf, dedup, secondary, ctyLookup, knownPtr)), // 1
			formatGridLineOrPlaceholder(gridStats, gridDB), // 2
			formatFCCLineOrPlaceholder(fccSnap),            // 3
			fmt.Sprintf("RBN: %d TOTAL / %d CW / %d RTTY / %d FT8 / %d FT4", combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4), // 4
			fmt.Sprintf("PSKReporter: %s TOTAL / %s CW / %s RTTY / %s FT8 / %s FT4 / %s MSK144",
				humanize.Comma(int64(pskTotal)),
				humanize.Comma(int64(pskCW)),
				humanize.Comma(int64(pskRTTY)),
				humanize.Comma(int64(pskFT8)),
				humanize.Comma(int64(pskFT4)),
				humanize.Comma(int64(pskMSK144)),
			), // 5
			fmt.Sprintf("Corrected calls: %d (C) / %d (U) / %d (F) / %d (H)", totalCorrections, totalUnlicensed, totalFreqCorrections, totalHarmonics), // 6
			secondaryLine, // 7
			fmt.Sprintf("Telnet: %d clients. Drops: %d (Q) / %d (C)", clientCount, queueDrops, clientDrops), // 8
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
func processRBNSpots(client *rbn.Client, deduplicator *dedup.Deduplicator, source string, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		if isStale(spot, spotPolicy) {
			continue
		}
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

// processHumanTelnetSpots marks incoming telnet spots as human-sourced and sends them into dedup.
func processHumanTelnetSpots(client *rbn.Client, deduplicator *dedup.Deduplicator, source string, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()
	var drops atomic.Uint64

	for sp := range spotChan {
		if sp != nil {
			sp.IsHuman = true
			sp.SourceType = spot.SourceUpstream
			if strings.TrimSpace(sp.SourceNode) == "" {
				sp.SourceNode = source
			}
			if strings.TrimSpace(sp.Mode) == "" {
				sp.Mode = "RTTY" // temporary default until mode parser is added
				sp.EnsureNormalized()
			}
			if isStale(sp, spotPolicy) {
				continue
			}
		}
		select {
		case dedupInput <- sp:
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
func processPSKRSpots(client *pskreporter.Client, deduplicator *dedup.Deduplicator, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
	dedupInput := deduplicator.GetInputChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		if isStale(spot, spotPolicy) {
			continue
		}
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

// isStale enforces the global max_age_seconds guard before deduplication so old
// spots are dropped early and do not consume dedupe/window resources.
func isStale(s *spot.Spot, policy config.SpotPolicy) bool {
	if s == nil || policy.MaxAgeSeconds <= 0 {
		return false
	}
	if s.Time.IsZero() {
		return false
	}
	return time.Since(s.Time) > time.Duration(policy.MaxAgeSeconds)*time.Second
}

// processOutputSpots receives deduplicated spots and distributes them
// Deduplicator Output  Ring Buffer  Broadcast to Clients
func processOutputSpots(
	deduplicator *dedup.Deduplicator,
	secondary *dedup.SecondaryDeduper,
	modeAssigner *spot.ModeAssigner,
	buf *buffer.RingBuffer,
	telnet *telnet.Server,
	peerManager *peer.Manager,
	tracker *stats.Tracker,
	correctionIdx *spot.CorrectionIndex,
	correctionCfg config.CallCorrectionConfig,
	ctyLookup func() *cty.CTYDatabase,
	harmonicDetector *spot.HarmonicDetector,
	harmonicCfg config.HarmonicConfig,
	knownCalls *atomic.Pointer[spot.KnownCallsigns],
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
	dash uiSurface,
	gridUpdate func(call, grid string),
	gridLookup func(call string) (string, bool),
	unlicensedReporter func(source, role, call, mode string, freq float64),
	corrLogger spot.CorrectionTraceLogger,
	adaptiveMinReports *spot.AdaptiveMinReports,
	refresher *adaptiveRefresher,
	spotterReliability spot.SpotterReliability,
	broadcastKeepSSID bool,
	archiveWriter *archive.Writer,
	lastOutput *atomic.Int64,
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
			s.EnsureNormalized()
			spot.ApplySourceHumanFlag(s)
			explicitMode := strings.TrimSpace(s.Mode) != ""
			if modeAssigner != nil {
				modeAssigner.Assign(s, explicitMode)
			}
			s.EnsureNormalized()
			ctyDB := ctyLookup()
			dirty := false
			modeUpper := s.ModeNorm
			if refresher != nil {
				refresher.IncrementSpots()
			}

			s.RefreshBeaconFlag()

			if gridLookup != nil {
				// Backfill missing grids from the persisted store so downstream consumers
				// see metadata even when the upstream spot omitted it.
				if strings.TrimSpace(s.DXMetadata.Grid) == "" {
					if grid, ok := gridLookup(s.DXCall); ok {
						s.DXMetadata.Grid = grid
						dirty = true
					}
				}
				if strings.TrimSpace(s.DEMetadata.Grid) == "" {
					if grid, ok := gridLookup(s.DECall); ok {
						s.DEMetadata.Grid = grid
						dirty = true
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
				suppress = maybeApplyCallCorrectionWithLogger(s, correctionIdx, correctionCfg, ctyDB, knownCalls, tracker, dash, corrLogger, adaptiveMinReports, spotterReliability)
				if suppress {
					return
				}
				// Call correction can change the DX call; recompute beacon flag accordingly.
				s.RefreshBeaconFlag()
				dirty = true
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
				avg, corroborators, _ := freqAvg.Average(s.DXCall, s.Frequency, time.Now().UTC(), window, tolerance)
				// Half-up rounding to 0.1 kHz to avoid banker's rounding at .x5 boundaries.
				rounded := math.Floor(avg*10+0.5) / 10
				// Apply the averaged frequency when we have enough corroborators and the rounded
				// value actually differs from the reported frequency. We deliberately decouple
				// this apply threshold from the inclusion tolerance so sub-500 Hz shifts are
				// preserved instead of being discarded by the same 0.5 kHz gate.
				delta := math.Abs(rounded - s.Frequency)
				if corroborators >= spotPolicy.FrequencyAveragingMinReports && delta >= 0.05 {
					s.Frequency = rounded
					if tracker != nil {
						tracker.IncrementFrequencyCorrections()
					}
					// Frequency corrections are applied silently (no dedicated dashboard pane).
					dirty = true
				}
			}

			// Ensure CW/RTTY/SSB carry at least a placeholder confidence glyph when no correction applied.
			if !s.IsBeacon {
				if (modeUpper == "CW" || modeUpper == "RTTY" || modeUpper == "SSB") && strings.TrimSpace(s.Confidence) == "" {
					s.Confidence = "?"
					dirty = true
				}
			}

			if dirty {
				s.EnsureNormalized()
				dirty = false
			}
			// Final CTY/licensing gate runs after corrections so busted calls can be fixed first.
			if applyLicenseGate(s, ctyDB, unlicensedReporter) {
				return
			}
			// License gate refreshes metadata; normalize once more before stats/broadcast.
			if !dirty {
				// applyLicenseGate mutates metadata; mark dirty for final normalization.
				dirty = true
			}

			if dirty {
				s.EnsureNormalized()
				dirty = false
			}
			if tracker != nil {
				modeKey := modeUpper
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

			// Ensure DE metadata is populated before secondary dedupe. Upstream CTY lookups
			// can be bypassed when spotters carry SSID tokens or CTY is missing; refresh
			// here so secondary dedupe has DXCC/zone available.
			if secondary != nil && (s.DEMetadata.ADIF <= 0 || s.DEMetadata.CQZone <= 0) && ctyDB != nil {
				if info := effectivePrefixInfo(ctyDB, s.DECall); info != nil {
					deGrid := strings.TrimSpace(s.DEMetadata.Grid)
					s.DEMetadata = metadataFromPrefix(info)
					if deGrid != "" {
						s.DEMetadata.Grid = deGrid
					}
				}
			}

			// Broadcast-only dedupe: ring/history already updated above.
			if secondary != nil && !secondary.ShouldForward(s) {
				return
			}

			// Final fan-out guards (symmetry with peer belt-and-suspenders): do not
			// deliver stale spots to any downstream sink, even if an upstream stage
			// failed to drop them.
			if isStale(s, spotPolicy) {
				return
			}

			if lastOutput != nil {
				lastOutput.Store(time.Now().UTC().UnixNano())
			}

			if archiveWriter != nil {
				archiveWriter.Enqueue(s)
			}

			if telnet != nil {
				toSend := s
				if !broadcastKeepSSID && s != nil {
					toSend = cloneSpotForBroadcast(s)
					toSend.DECall = collapseSSIDForBroadcast(s.DECall)
				}
				telnet.BroadcastSpot(toSend)
			}
			if peerManager != nil && s.SourceType != spot.SourceUpstream && s.SourceType != spot.SourcePeer {
				peerManager.PublishDX(s)
			}
		}()
	}
}

// startPipelineHealthMonitor logs warnings when the output pipeline or dedup
// goroutine appear stalled. It is intentionally lightweight and non-blocking.
func startPipelineHealthMonitor(ctx context.Context, dedup *dedup.Deduplicator, lastOutput *atomic.Int64, peerManager *peer.Manager) {
	const (
		checkInterval      = 30 * time.Second
		outputStallWarning = 2 * time.Minute
	)
	ticker := time.NewTicker(checkInterval)
	go func() {
		defer ticker.Stop()
		var lastReconnects uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().UTC()
				if lastOutput != nil {
					ns := lastOutput.Load()
					if ns > 0 {
						age := now.Sub(time.Unix(0, ns))
						if age > outputStallWarning {
							reconnects := uint64(0)
							if peerManager != nil {
								reconnects = peerManager.ReconnectCount()
							}
							dedupStamp := "unknown"
							if dedup != nil {
								if last := dedup.LastProcessedAt(); !last.IsZero() {
									dedupStamp = last.UTC().Format(time.RFC3339)
								}
							}
							log.Printf("Warning: output pipeline idle for %s (dedup_last=%s, peer_reconnects=%d)", age, dedupStamp, reconnects)
						}
					}
				}
				if peerManager != nil {
					if reconnects := peerManager.ReconnectCount(); reconnects != lastReconnects {
						log.Printf("Peering: outbound reconnects=%d", reconnects)
						lastReconnects = reconnects
					}
				}
				if dedup != nil {
					if last := dedup.LastProcessedAt(); !last.IsZero() {
						if age := now.Sub(last); age > outputStallWarning {
							log.Printf("Warning: deduplicator idle for %s", age)
						}
					}
				}
			}
		}
	}()
}

// collapseSSIDForBroadcast trims SSID fragments so clients see a single
// skimmer identity (e.g., N2WQ-1-# -> N2WQ-#, N2WQ-1 -> N2WQ).
// It preserves non-numeric suffixes.
func collapseSSIDForBroadcast(call string) string {
	call = strings.TrimSpace(call)
	if call == "" {
		return call
	}
	if strings.HasSuffix(call, "-#") {
		trimmed := strings.TrimSuffix(call, "-#")
		return stripNumericSSID(trimmed) + "-#"
	}
	return stripNumericSSID(call)
}

func stripNumericSSID(call string) string {
	idx := strings.LastIndexByte(call, '-')
	if idx <= 0 || idx == len(call)-1 {
		return call
	}
	suffix := call[idx+1:]
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < '0' || suffix[i] > '9' {
			return call
		}
	}
	return call[:idx]
}

func cloneSpotForBroadcast(src *spot.Spot) *spot.Spot {
	if src == nil {
		return nil
	}
	return &spot.Spot{
		ID:         src.ID,
		DXCall:     src.DXCall,
		DECall:     src.DECall,
		Frequency:  src.Frequency,
		Band:       src.Band,
		Mode:       src.Mode,
		Report:     src.Report,
		HasReport:  src.HasReport,
		Time:       src.Time,
		Comment:    src.Comment,
		SourceType: src.SourceType,
		SourceNode: src.SourceNode,
		SpotterIP:  src.SpotterIP,
		TTL:        src.TTL,
		IsHuman:    src.IsHuman,
		IsBeacon:   src.IsBeacon,
		DXMetadata: src.DXMetadata,
		DEMetadata: src.DEMetadata,
		Confidence: src.Confidence,
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

	now := time.Now()
	if dxInfo != nil && dxInfo.ADIF == 291 {
		if licensed, ok := licCache.get(s.DXCallNorm, now); ok {
			if !licensed {
				if reporter != nil {
					reporter(s.SourceNode, "DX", s.DXCallNorm, s.ModeNorm, s.Frequency)
				}
				return true
			}
		} else if !uls.IsLicensedUS(s.DXCallNorm) {
			licCache.set(s.DXCallNorm, false, now)
			if reporter != nil {
				reporter(s.SourceNode, "DX", s.DXCallNorm, s.ModeNorm, s.Frequency)
			}
			return true
		} else {
			licCache.set(s.DXCallNorm, true, now)
		}
	}
	if deInfo != nil && deInfo.ADIF == 291 {
		if licensed, ok := licCache.get(s.DECallNorm, now); ok {
			if !licensed {
				if reporter != nil {
					reporter(s.SourceNode, "DE", s.DECallNorm, s.ModeNorm, s.Frequency)
				}
				return true
			}
		} else if !uls.IsLicensedUS(s.DECallNorm) {
			licCache.set(s.DECallNorm, false, now)
			if reporter != nil {
				reporter(s.SourceNode, "DE", s.DECallNorm, s.ModeNorm, s.Frequency)
			}
			return true
		} else {
			licCache.set(s.DECallNorm, true, now)
		}
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

func maybeApplyCallCorrectionWithLogger(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns], tracker *stats.Tracker, dash uiSurface, traceLogger spot.CorrectionTraceLogger, adaptive *spot.AdaptiveMinReports, spotterReliability spot.SpotterReliability) bool {
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

	now := time.Now().UTC()
	window := callCorrectionWindow(cfg)
	defer idx.Add(spotEntry, now, window)

	modeUpper := strings.ToUpper(strings.TrimSpace(spotEntry.Mode))

	if adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") {
		adaptive.Observe(spotEntry.Band, spotEntry.DECall, now)
	}

	minReports := cfg.MinConsensusReports
	if adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") {
		if dyn := adaptive.MinReportsForBand(spotEntry.Band, now); dyn > 0 {
			minReports = dyn
		}
	}
	state := "normal"
	if adaptive != nil {
		state = adaptive.StateForBand(spotEntry.Band, now)
	}
	qualityBinHz := cfg.QualityBinHz
	freqToleranceHz := cfg.FrequencyToleranceHz
	if params, ok := resolveBandStateParams(cfg.BandStateOverrides, spotEntry.Band, state); ok {
		if params.QualityBinHz > 0 {
			qualityBinHz = params.QualityBinHz
		}
		if params.FrequencyToleranceHz > 0 {
			freqToleranceHz = params.FrequencyToleranceHz
		}
	}

	settings := spot.CorrectionSettings{
		MinConsensusReports:      minReports,
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
		TraceLogger:              traceLogger,
		FrequencyToleranceHz:     freqToleranceHz,
		QualityBinHz:             qualityBinHz,
		QualityGoodThreshold:     cfg.QualityGoodThreshold,
		QualityNewCallIncrement:  cfg.QualityNewCallIncrement,
		QualityBustedDecrement:   cfg.QualityBustedDecrement,
		SpotterReliability:       spotterReliability,
		MinSpotterReliability:    cfg.MinSpotterReliability,
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

type bandStateParams struct {
	QualityBinHz         int
	FrequencyToleranceHz float64
}

// resolveBandStateParams returns per-band, per-state overrides when defined; otherwise false.
func resolveBandStateParams(overrides []config.BandStateOverride, band, state string) (bandStateParams, bool) {
	b := strings.ToLower(strings.TrimSpace(band))
	if b == "" || len(overrides) == 0 {
		return bandStateParams{}, false
	}
	stateKey := strings.ToLower(strings.TrimSpace(state))
	for _, o := range overrides {
		for _, candidate := range o.Bands {
			if strings.ToLower(strings.TrimSpace(candidate)) != b {
				continue
			}
			switch stateKey {
			case "quiet":
				return bandStateParams{
					QualityBinHz:         o.Quiet.QualityBinHz,
					FrequencyToleranceHz: o.Quiet.FrequencyToleranceHz,
				}, true
			case "busy":
				return bandStateParams{
					QualityBinHz:         o.Busy.QualityBinHz,
					FrequencyToleranceHz: o.Busy.FrequencyToleranceHz,
				}, true
			default:
				return bandStateParams{
					QualityBinHz:         o.Normal.QualityBinHz,
					FrequencyToleranceHz: o.Normal.FrequencyToleranceHz,
				}, true
			}
		}
	}
	return bandStateParams{}, false
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

func startSkewScheduler(ctx context.Context, cfg config.SkewConfig, store *skew.Store) {
	if store == nil {
		return
	}
	go func() {
		for {
			delay := nextSkewRefreshDelay(cfg, time.Now().UTC())
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
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

// downloadFileAtomic streams the remote file to a temp file and swaps it into place
// atomically so readers never see a partial write.
func downloadFileAtomic(url, destination string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("fetch failed: status %s", resp.Status)
	}

	dir := filepath.Dir(destination)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
	}
	tmpDir := dir
	if tmpDir == "" {
		tmpDir = "."
	}
	tmpFile, err := os.CreateTemp(tmpDir, "download-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return fmt.Errorf("copy body: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("finalize temp file: %w", err)
	}
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove old file: %w", err)
	}
	if err := os.Rename(tmpName, destination); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	return nil
}

// startKnownCallScheduler downloads the known-calls file at the configured UTC
// time every day and updates the in-memory cache pointer after each refresh.
func startKnownCallScheduler(ctx context.Context, cfg config.KnownCallsConfig, knownPtr *atomic.Pointer[spot.KnownCallsigns], store *gridstore.Store) {
	if knownPtr == nil {
		return
	}
	go func() {
		for {
			delay := nextKnownCallRefreshDelay(cfg, time.Now().UTC())
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
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
	if err := downloadFileAtomic(url, path, 1*time.Minute); err != nil {
		return nil, fmt.Errorf("known calls: %w", err)
	}
	return spot.LoadKnownCallsigns(path)
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

// startCTYScheduler downloads cty.plist at the configured UTC time every day and
// updates the in-memory CTY database pointer after each refresh.
func startCTYScheduler(ctx context.Context, cfg config.CTYConfig, ctyPtr *atomic.Pointer[cty.CTYDatabase]) {
	if ctyPtr == nil {
		return
	}
	go func() {
		for {
			delay := nextCTYRefreshDelay(cfg, time.Now().UTC())
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if fresh, err := refreshCTYDatabase(cfg); err != nil {
				log.Printf("Warning: scheduled CTY download failed: %v", err)
			} else {
				ctyPtr.Store(fresh)
				log.Printf("Scheduled CTY download complete (%d prefixes)", len(fresh.Keys))
			}
		}
	}()
}

// refreshCTYDatabase downloads cty.plist, writes it atomically, and returns the parsed DB.
func refreshCTYDatabase(cfg config.CTYConfig) (*cty.CTYDatabase, error) {
	url := strings.TrimSpace(cfg.URL)
	path := strings.TrimSpace(cfg.File)
	if url == "" {
		return nil, errors.New("cty: URL is empty")
	}
	if path == "" {
		return nil, errors.New("cty: file path is empty")
	}
	if err := downloadFileAtomic(url, path, 1*time.Minute); err != nil {
		return nil, fmt.Errorf("cty: %w", err)
	}
	db, err := cty.LoadCTYDatabase(path)
	if err != nil {
		return nil, fmt.Errorf("cty: load: %w", err)
	}
	return db, nil
}

func nextCTYRefreshDelay(cfg config.CTYConfig, now time.Time) time.Duration {
	hour, minute := ctyRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func ctyRefreshHourMinute(cfg config.CTYConfig) (int, int) {
	refresh := strings.TrimSpace(cfg.RefreshUTC)
	if refresh == "" {
		refresh = "00:45"
	}
	if parsed, err := time.Parse("15:04", refresh); err == nil {
		return parsed.Hour(), parsed.Minute()
	}
	return 0, 45
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
	hitPercent := int(math.Ceil(hitRate))

	if dbTotal >= 0 {
		return fmt.Sprintf("Grid database: %s TOTAL / %d%%",
			humanize.Comma(dbTotal),
			hitPercent)
	}
	return fmt.Sprintf("Grid database: %s UPDATED / %d%%",
		humanize.Comma(int64(updatesSinceStart)),
		hitPercent)
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

func startGridWriter(store *gridstore.Store, flushInterval time.Duration, cache *gridCache, ttl time.Duration, dbCheckOnMiss bool) (func(call, grid string), *gridMetrics, func(), func(call string) (string, bool)) {
	if store == nil {
		return nil, nil, nil, nil
	}
	if flushInterval <= 0 {
		flushInterval = 60 * time.Second
	}
	metrics := &gridMetrics{}
	// Synchronous DB reads are disabled on the hot path to keep output non-blocking.
	// dbCheckOnMiss now gates async cache backfill instead of in-band reads.
	asyncLookupEnabled := dbCheckOnMiss
	type update struct {
		call string
		grid string
	}
	updates := make(chan update, 8192)
	done := make(chan struct{})
	lookupQueue := make(chan string, 4096)
	lookupDone := make(chan struct{})
	var lookupPendingMu sync.Mutex
	lookupPending := make(map[string]struct{})

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
			start := time.Now()
			if err := store.UpsertBatch(batch); err != nil {
				if gridstore.IsBusyError(err) {
					// Keep the batch in-memory so a later flush can retry after the lock clears.
					log.Printf("Warning: gridstore batch upsert busy (retaining %d pending): %v", len(batch), err)
					return
				}
				log.Printf("Warning: gridstore batch upsert failed (dropping %d pending): %v", len(batch), err)
				clear(pending)
				return
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				log.Printf("Gridstore: batch upsert %d records in %s", len(batch), elapsed)
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
		// Skip synchronous SQLite reads on cache miss; treat it as an update to keep
		// the output path non-blocking (extra writes are acceptable).
		if cache != nil && !cache.shouldUpdate(call, grid, nil) {
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
		if asyncLookupEnabled {
			close(lookupQueue)
			<-lookupDone
		}
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
		// Cache miss: enqueue async lookup to avoid blocking output.
		if asyncLookupEnabled {
			lookupPendingMu.Lock()
			if _, exists := lookupPending[call]; !exists {
				lookupPending[call] = struct{}{}
				select {
				case lookupQueue <- call:
				default:
					delete(lookupPending, call)
				}
			}
			lookupPendingMu.Unlock()
		}
		return "", false
	}

	if asyncLookupEnabled {
		go func() {
			defer close(lookupDone)
			for call := range lookupQueue {
				rec, err := store.Get(call)
				if err == nil && rec != nil && rec.Grid.Valid {
					grid := strings.ToUpper(strings.TrimSpace(rec.Grid.String))
					if grid != "" && cache != nil {
						cache.add(call, grid)
					}
				} else if err != nil && !gridstore.IsBusyError(err) {
					log.Printf("Warning: gridstore async lookup failed for %s: %v", call, err)
				}
				lookupPendingMu.Lock()
				delete(lookupPending, call)
				lookupPendingMu.Unlock()
			}
		}()
	} else {
		close(lookupDone)
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

// formatMemoryLine reports memory-ish metrics in order:
// exec alloc / ring buffer / primary dedup (dup%) / secondary dedup / CTY cache (hit%) / known calls (hit%).
func formatMemoryLine(buf *buffer.RingBuffer, dedup *dedup.Deduplicator, secondary *dedup.SecondaryDeduper, ctyLookup func() *cty.CTYDatabase, knownPtr *atomic.Pointer[spot.KnownCallsigns]) string {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	execMB := bytesToMB(mem.Alloc)

	ringMB := 0.0
	if buf != nil {
		ringMB = float64(buf.GetSizeKB()) / 1024.0
	}

	dedupeMB := 0.0
	dedupeRatio := 0.0
	secondaryMB := 0.0
	if dedup != nil {
		processed, duplicates, cacheSize := dedup.GetStats()
		dedupeMB = bytesToMB(uint64(cacheSize * dedupeEntryBytes))
		if processed > 0 {
			dedupeRatio = float64(duplicates) / float64(processed) * 100
		}
	}
	if secondary != nil {
		_, _, cacheSize := secondary.GetStats()
		secondaryMB = bytesToMB(uint64(cacheSize * dedupeEntryBytes))
	}

	ctyMB := 0.0
	ctyRatio := 0.0
	if ctyLookup != nil {
		db := ctyLookup()
		if db != nil {
			metrics := db.Metrics()
			ctyMB = bytesToMB(uint64(metrics.CacheEntries * ctyCacheEntryBytes))
			if metrics.TotalLookups > 0 {
				ctyRatio = float64(metrics.CacheHits) / float64(metrics.TotalLookups) * 100
			}
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

	return fmt.Sprintf("Memory MB: %.1f / %.1f / %.1f (%.1f%%) / %.1f / %.1f (%.1f%%) / %.1f (%.1f%%)",
		execMB, ringMB, dedupeMB, dedupeRatio, secondaryMB, ctyMB, ctyRatio, knownMB, knownRatio)
}

func formatUptimeLine(uptime time.Duration) string {
	hours := int(uptime.Hours())
	minutes := int(uptime.Minutes()) % 60
	return fmt.Sprintf("Uptime: %02d:%02d", hours, minutes)
}

// wwvKindFromLine tags non-DX lines coming from human/relay telnet ingest.
// We only forward WWV/WCY bulletins to telnet clients; upstream keepalives,
// prompts, or other control chatter (e.g., "de N2WQ-22" banners) are dropped.
func wwvKindFromLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return ""
	}
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "WWV") {
		return "WWV"
	}
	if strings.HasPrefix(upper, "WCY") {
		return "WCY"
	}
	return ""
}

// announcementFromLine returns the raw announcement text for "To ALL" broadcasts.
func announcementFromLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return ""
	}
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "TO ALL") {
		return trimmed
	}
	return ""
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

// maybeStartHeapLogger starts periodic heap logging when DXC_HEAP_LOG_INTERVAL is set
// (e.g., "60s"). Defaults to disabled when the variable is empty or invalid.
func maybeStartHeapLogger() {
	intervalStr := strings.TrimSpace(os.Getenv("DXC_HEAP_LOG_INTERVAL"))
	if intervalStr == "" {
		return
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil || interval <= 0 {
		log.Printf("Heap logger disabled (invalid DXC_HEAP_LOG_INTERVAL=%q)", intervalStr)
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		log.Printf("Heap logger enabled (every %s)", interval)
		for range ticker.C {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Printf("Heap: alloc=%.1f MB sys=%.1f MB objects=%d gc=%d next_gc=%.1f MB",
				bytesToMB(m.HeapAlloc),
				bytesToMB(m.Sys),
				m.HeapObjects,
				m.NumGC,
				bytesToMB(m.NextGC))
		}
	}()
}

// maybeStartDiagServer exposes /debug/pprof/* and /debug/heapdump when DXC_PPROF_ADDR is set
// (example: DXC_PPROF_ADDR=localhost:6061). Default is off.
func maybeStartDiagServer() {
	addr := strings.TrimSpace(os.Getenv("DXC_PPROF_ADDR"))
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/heapdump", func(w http.ResponseWriter, r *http.Request) {
		ts := time.Now().UTC().Format("2006-01-02T15-04-05Z")
		dir := filepath.Join("data", "diagnostics")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			http.Error(w, fmt.Sprintf("mkdir diagnostics: %v", err), http.StatusInternalServerError)
			return
		}
		path := filepath.Join(dir, fmt.Sprintf("heap-%s.pprof", ts))
		f, err := os.Create(path)
		if err != nil {
			http.Error(w, fmt.Sprintf("create heap dump: %v", err), http.StatusInternalServerError)
			return
		}
		defer f.Close()
		runtime.GC() // collect latest data
		if err := pprof.WriteHeapProfile(f); err != nil {
			http.Error(w, fmt.Sprintf("write heap profile: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "heap profile written to %s\n", path)
	})
	mux.Handle("/debug/pprof/", http.HandlerFunc(httppprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(httppprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(httppprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(httppprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(httppprof.Trace))

	go func() {
		log.Printf("Diagnostics server listening on %s (pprof + /debug/heapdump)", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("Diagnostics server error: %v", err)
		}
	}()
}
