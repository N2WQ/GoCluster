// Program gocluster wires together all ingest clients (RBN, PSKReporter),
// protections (deduplication, call correction, harmonics), persistence layers
// (ring buffer, grid store), and the telnet server UI.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"sort"
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
	"dxcluster/download"
	"dxcluster/filter"
	"dxcluster/gridstore"
	"dxcluster/pathreliability"
	"dxcluster/peer"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/reputation"
	"dxcluster/skew"
	"dxcluster/solarweather"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
	"dxcluster/ui"
	"dxcluster/uls"

	"github.com/cockroachdb/pebble"
	"github.com/dustin/go-humanize"
	"golang.org/x/term"
)

const (
	dedupeEntryBytes          = 32
	callMetaEntryBytes        = 96
	knownCallEntryBytes       = 24
	sourceModeDelimiter       = "|"
	defaultConfigPath         = "data/config"
	pathReliabilityConfigFile = "path_reliability.yaml"
	solarWeatherConfigFile    = "solarweather.yaml"
	envConfigPath             = "DXC_CONFIG_PATH"

	// envGridDBCheckOnMiss overrides the config-driven grid_db_check_on_miss at runtime.
	// When true, grid updates may synchronously consult SQLite on cache miss to avoid
	// redundant writes. When false, the hot path never blocks on that read and may
	// perform extra batched writes instead.
	envGridDBCheckOnMiss = "DXC_GRID_DB_CHECK_ON_MISS"
	// envBlockProfileRate enables block profiling when set to a Go duration or integer nanoseconds.
	envBlockProfileRate = "DXC_BLOCK_PROFILE_RATE"
	// envMutexProfileFraction enables mutex profiling when set to an integer fraction (1/N).
	envMutexProfileFraction = "DXC_MUTEX_PROFILE_FRACTION"
	// envMapLogInterval enables periodic map size logging when set to a Go duration.
	envMapLogInterval = "DXC_MAP_LOG_INTERVAL"
)

// Version will be set at build time
var Version = "dev"
var Commit = "unknown"
var BuildTime = "unknown"

type binaryVersion struct {
	version     string
	commit      string
	buildTime   string
	vcsModified string
	goVersion   string
}

// Purpose: Resolve executable identity from linker flags or Go build metadata.
// Key aspects: Prefers explicit ldflags, then falls back to embedded VCS settings.
// Upstream: main startup/version output.
// Downstream: runtime/debug.ReadBuildInfo and startup logging.
func resolveBinaryVersion() binaryVersion {
	info := binaryVersion{
		version:   strings.TrimSpace(Version),
		commit:    strings.TrimSpace(Commit),
		buildTime: strings.TrimSpace(BuildTime),
	}
	if info.version == "" {
		info.version = "dev"
	}
	if info.commit == "" {
		info.commit = "unknown"
	}
	if info.buildTime == "" {
		info.buildTime = "unknown"
	}

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	info.goVersion = strings.TrimSpace(buildInfo.GoVersion)
	if info.goVersion == "" {
		info.goVersion = runtime.Version()
	}
	if mainVer := strings.TrimSpace(buildInfo.Main.Version); mainVer != "" && mainVer != "(devel)" && info.version == "dev" {
		info.version = mainVer
	}

	vcsRevision := ""
	vcsTime := ""
	vcsModified := ""
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			vcsRevision = strings.TrimSpace(setting.Value)
		case "vcs.time":
			vcsTime = strings.TrimSpace(setting.Value)
		case "vcs.modified":
			vcsModified = strings.TrimSpace(setting.Value)
		}
	}
	if info.commit == "unknown" && vcsRevision != "" {
		info.commit = shortRevision(vcsRevision)
	}
	if info.buildTime == "unknown" && vcsTime != "" {
		info.buildTime = vcsTime
	}
	if info.version == "dev" && vcsRevision != "" {
		info.version = "dev-" + shortRevision(vcsRevision)
	}
	if vcsModified != "" {
		info.vcsModified = vcsModified
	}
	return info
}

func shortRevision(revision string) string {
	const maxLen = 12
	if len(revision) <= maxLen {
		return revision
	}
	return revision[:maxLen]
}

func shouldPrintVersion(args []string) bool {
	for _, arg := range args {
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "--version", "-version", "version":
			return true
		}
	}
	return false
}

func printVersion(info binaryVersion) {
	fmt.Printf("gocluster %s\n", info.version)
	fmt.Printf("commit: %s\n", info.commit)
	fmt.Printf("built:  %s\n", info.buildTime)
	if info.vcsModified != "" {
		fmt.Printf("dirty:  %s\n", info.vcsModified)
	}
	if info.goVersion != "" {
		fmt.Printf("go:     %s\n", info.goVersion)
	}
}

type gridMetrics struct {
	learnedTotal atomic.Uint64
	cacheLookups atomic.Uint64
	cacheHits    atomic.Uint64
	asyncDrops   atomic.Uint64
	syncDrops    atomic.Uint64

	rateMu          sync.Mutex
	lastLookupCount uint64
	lastSample      time.Time
}

const (
	gridSyncLookupWorkers    = 4
	gridSyncLookupQueueDepth = 512
	gridSyncLookupTimeout    = 8 * time.Millisecond
)

const (
	gridCheckpointDirName       = "checkpoint"
	gridCheckpointInterval      = time.Hour
	gridCheckpointRetention     = 24 * time.Hour
	gridCheckpointVerifyTimeout = 30 * time.Second
	gridIntegrityScanTimeout    = 5 * time.Minute
	gridIntegrityScanUTC        = "05:00"
	gridRestoreWarnAfter        = 60 * time.Second
	gridCheckpointNameLayoutUTC = "2006-01-02T15-04-05Z"
)

type gridStoreHandle struct {
	store atomic.Pointer[gridstore.Store]
}

func newGridStoreHandle(store *gridstore.Store) *gridStoreHandle {
	handle := &gridStoreHandle{}
	if store != nil {
		handle.store.Store(store)
	}
	return handle
}

func (h *gridStoreHandle) Store() *gridstore.Store {
	if h == nil {
		return nil
	}
	return h.store.Load()
}

func (h *gridStoreHandle) Available() bool {
	return h.Store() != nil
}

func (h *gridStoreHandle) Set(store *gridstore.Store) {
	if h == nil {
		return
	}
	h.store.Store(store)
}

func (h *gridStoreHandle) Close() error {
	if h == nil {
		return nil
	}
	store := h.Store()
	if store == nil {
		return nil
	}
	return store.Close()
}

type gridLookupRequest struct {
	baseCall string
	rawCall  string
	resp     chan gridLookupResult
}

type gridLookupResult struct {
	grid    string
	derived bool
	ok      bool
}

// Purpose: Report whether stdout is a TTY for UI gating.
// Key aspects: Uses term.IsTerminal on stdout fd.
// Upstream: main UI selection.
// Downstream: term.IsTerminal.
func isStdoutTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// Purpose: Load configuration from env/default directories.
// Key aspects: Tries env override first, then the default config dir.
// Upstream: main startup.
// Downstream: config.Load and os.IsNotExist.
func loadClusterConfig() (*config.Config, string, error) {
	candidates := make([]string, 0, 2)
	if envPath := strings.TrimSpace(os.Getenv(envConfigPath)); envPath != "" {
		candidates = append(candidates, envPath)
	}
	candidates = append(candidates, defaultConfigPath)

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
	return nil, "", fmt.Errorf("unable to load config; tried %s (last error: %w)", strings.Join(candidates, ", "), lastErr)
}

// Purpose: Resolve grid_db_check_on_miss behavior and its source.
// Key aspects: Env DXC_GRID_DB_CHECK_ON_MISS overrides config defaults.
// Upstream: main grid cache setup.
// Downstream: strconv.ParseBool and logging on invalid input.
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

// Purpose: Program entrypoint; wires configuration, ingest, and output pipeline.
// Key aspects: Initializes caches/clients/UI and manages graceful shutdown.
// Upstream: OS process start.
// Downstream: Startup helpers, goroutines, and network services.
func main() {
	versionInfo := resolveBinaryVersion()
	Version = versionInfo.version
	if shouldPrintVersion(os.Args[1:]) {
		printVersion(versionInfo)
		return
	}

	// Load configuration
	cfg, configSource, err := loadClusterConfig()
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	logMux, logErr := setupLogging(cfg.Logging, os.Stdout)
	// logFanout handles timestamp formatting for each sink.
	log.SetFlags(0)
	log.SetOutput(logMux)
	if logMux != nil {
		defer logMux.Close()
	}
	if logErr != nil {
		log.Printf("Logging: %v", logErr)
	}
	log.Printf("Loaded configuration from %s", configSource)
	if err := spot.SetDXClusterLineLength(cfg.Telnet.OutputLineLength); err != nil {
		log.Printf("Invalid telnet output line length: %v", err)
		return
	}

	// Load path reliability config from dedicated file in the config directory.
	pathCfgPath := filepath.Join(configSource, pathReliabilityConfigFile)
	pathCfg, pathCfgErr := pathreliability.LoadFile(pathCfgPath)
	if pathCfgErr != nil {
		if os.IsNotExist(pathCfgErr) {
			pathCfg = pathreliability.DefaultConfig()
			pathCfg.Enabled = false
			log.Printf("Path reliability config not found at %s; feature disabled", pathCfgPath)
		} else {
			log.Printf("Warning: failed to load path reliability config (%s): %v", pathCfgPath, pathCfgErr)
			pathCfg = pathreliability.DefaultConfig()
		}
	}
	allowedBands, allowedBandSet := normalizeAllowedBands(pathCfg.AllowedBands)
	pathPredictor := pathreliability.NewPredictor(pathCfg, allowedBands)
	if pathCfg.Enabled {
		if err := pathreliability.InitH3MappingsFromDir(cfg.H3TablePath); err != nil {
			log.Printf("Path reliability H3 mapping init failed: %v; feature disabled", err)
			pathCfg.Enabled = false
			pathPredictor = pathreliability.NewPredictor(pathCfg, allowedBands)
		}
	}

	solarCfgPath := filepath.Join(configSource, solarWeatherConfigFile)
	solarCfg, solarCfgErr := solarweather.LoadFile(solarCfgPath)
	if solarCfgErr != nil {
		if os.IsNotExist(solarCfgErr) {
			solarCfg = solarweather.DefaultConfig()
			solarCfg.Enabled = false
			log.Printf("Solar weather config not found at %s; overrides disabled", solarCfgPath)
		} else {
			log.Printf("Warning: failed to load solar weather config (%s): %v", solarCfgPath, solarCfgErr)
			solarCfg = solarweather.DefaultConfig()
		}
	}
	if err := solarCfg.Validate(); err != nil {
		log.Printf("Warning: invalid solar weather config: %v; overrides disabled", err)
		solarCfg.Enabled = false
	}

	uiMode := strings.ToLower(strings.TrimSpace(cfg.UI.Mode))
	renderAllowed := isStdoutTTY()

	var surface ui.Surface
	switch uiMode {
	case "headless":
		log.Printf("UI disabled (mode=headless)")
	case "tview":
		if !renderAllowed {
			log.Printf("UI disabled (tview requires an interactive console)")
		} else {
			surface = newDashboard(cfg.UI, true)
		}
	case "tview-v2":
		if !renderAllowed {
			log.Printf("UI disabled (tview-v2 requires an interactive console)")
		} else {
			surface = ui.NewDashboardV2(cfg.UI, true)
		}
	case "ansi":
		if !renderAllowed {
			log.Printf("UI disabled (ansi renderer requires an interactive console)")
		} else {
			surface = newANSIConsole(cfg.UI, renderAllowed)
		}
	default:
		log.Printf("UI mode %q not recognized; defaulting to headless", uiMode)
	}

	if surface != nil {
		surface.WaitReady()
		defer surface.Stop()
		if logMux != nil {
			// UI surfaces render their own timestamps; keep log lines raw.
			logMux.SetConsoleSink(surface.SystemWriter(), false)
		}
		surface.SetStats([]string{"Initializing..."})
	} else {
		if logMux != nil {
			logMux.SetConsoleSink(os.Stdout, true)
		}
	}

	log.Printf("DX Cluster Server v%s starting... (commit=%s built=%s)", Version, versionInfo.commit, versionInfo.buildTime)
	ctx, cancel := context.WithCancel(context.Background())
	var solarMgr *solarweather.Manager
	if solarCfg.Enabled {
		solarMgr = solarweather.NewManager(solarCfg, log.Default())
		solarMgr.Start(ctx)
		log.Printf("Solar weather overrides enabled")
	}
	var propScheduler *propReportScheduler
	if cfg.PropReport.Enabled {
		if !cfg.Logging.Enabled {
			log.Printf("Prop report enabled, but logging is disabled; no log rotation events will trigger reports")
		}
		propRunner := newPropReportGenerator(configSource, log.Default())
		propScheduler = newPropReportScheduler(true, propRunner, log.Default(), propReportTimeout)
		propScheduler.Start(ctx)
		if logMux != nil {
			logMux.SetRotateHook(func(prevDate time.Time, prevPath, newPath string) {
				if prevDate.IsZero() {
					return
				}
				propScheduler.Enqueue(propReportJob{
					Date:    prevDate,
					LogPath: prevPath,
				})
			})
		}
		startPropReportDailyScheduler(ctx, cfg.PropReport, cfg.Logging, propScheduler)
		log.Printf("Prop report scheduler enabled")
	} else {
		log.Printf("Prop report scheduler disabled")
	}
	defer func() {
		cancel()
		if propScheduler != nil {
			propScheduler.Wait()
		}
	}()
	if pathCfg.Enabled && pathPredictor != nil {
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			compactInterval := time.Hour
			compactMinPeak := 1000
			compactRatio := 0.5
			var lastCompact time.Time
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					now := time.Now().UTC()
					pathPredictor.PurgeStale(now)
					if lastCompact.IsZero() || now.Sub(lastCompact) >= compactInterval {
						if compacted := pathPredictor.Compact(compactMinPeak, compactRatio); compacted > 0 {
							log.Printf("Path predictor compacted %d shards", compacted)
						}
						lastCompact = now
					}
				}
			}
		}()
	}

	callCacheTTL := time.Duration(cfg.CallCache.TTLSeconds) * time.Second
	spot.ConfigureNormalizeCallCache(cfg.CallCache.Size, callCacheTTL)
	rbn.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
	pskreporter.ConfigureCallCache(cfg.CallCache.Size, callCacheTTL)
	filter.SetDefaultModeSelection(cfg.Filter.DefaultModes)
	filter.SetDefaultSourceSelection(cfg.Filter.DefaultSources)
	if err := filter.EnsureUserDataDir(); err != nil {
		log.Printf("Warning: unable to initialize filter directory: %v", err)
	}

	metaCache := newCallMetaCache(cfg.GridCacheSize, time.Duration(cfg.GridCacheTTLSec)*time.Second)

	// Print the configuration (stdout only when not using the dashboard)
	if surface == nil {
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
	uls.SetLicenseCacheTTL(time.Duration(cfg.FCCULS.CacheTTLSeconds) * time.Second)
	uls.SetAllowlistPath(cfg.FCCULS.AllowlistPath)

	// Start the FCC ULS downloader in the background (does not block spot processing)
	uls.StartBackground(ctx, cfg.FCCULS)

	// Load CTY database for callsign validation, track refresh age, and schedule retries.
	var ctyDB atomic.Pointer[cty.CTYDatabase]
	ctyState := newCTYRefreshState()
	ctyPath := strings.TrimSpace(cfg.CTY.File)
	ctyURL := strings.TrimSpace(cfg.CTY.URL)
	if cfg.CTY.Enabled && ctyPath != "" {
		if _, err := os.Stat(ctyPath); err != nil && errors.Is(err, os.ErrNotExist) && ctyURL != "" {
			if fresh, updated, refreshErr := refreshCTYDatabase(ctx, cfg.CTY); refreshErr != nil {
				log.Printf("Warning: CTY download failed: %v", refreshErr)
				ctyState.recordFailure(time.Now().UTC(), refreshErr)
			} else if updated && fresh != nil {
				ctyDB.Store(fresh)
				ctyState.recordSuccess(time.Now().UTC())
				log.Printf("Downloaded CTY database from %s", ctyURL)
			} else {
				ctyState.recordSuccess(time.Now().UTC())
				log.Printf("CTY database already up to date (%s)", ctyPath)
			}
		}
	}
	if cfg.CTY.Enabled && ctyDB.Load() == nil && ctyPath != "" {
		if loaded, loadErr := cty.LoadCTYDatabase(ctyPath); loadErr != nil {
			log.Printf("Warning: failed to load CTY database: %v", loadErr)
			ctyState.recordFailure(time.Now().UTC(), loadErr)
		} else {
			ctyDB.Store(loaded)
			ctyState.recordSuccess(time.Now().UTC())
			log.Printf("Loaded CTY database from %s", ctyPath)
		}
	}
	ctyLookup := func() *cty.CTYDatabase {
		return ctyDB.Load()
	}
	if cfg.CTY.Enabled && ctyURL != "" && ctyPath != "" {
		startCTYScheduler(ctx, cfg.CTY, &ctyDB, metaCache, ctyState)
	} else if cfg.CTY.Enabled {
		log.Printf("Warning: CTY download enabled but url or file missing")
	}
	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	spot.ConfigureBaudotWeights(cfg.CallCorrection.BaudotWeights.Insert, cfg.CallCorrection.BaudotWeights.Delete, cfg.CallCorrection.BaudotWeights.Sub, cfg.CallCorrection.BaudotWeights.Scale)
	pinPriors := true
	if cfg.CallCorrection.CallQualityPinPriors != nil {
		pinPriors = *cfg.CallCorrection.CallQualityPinPriors
	}
	spot.ConfigureCallQualityStore(
		time.Duration(cfg.CallCorrection.CallQualityTTLSeconds)*time.Second,
		cfg.CallCorrection.CallQualityMaxEntries,
		time.Duration(cfg.CallCorrection.CallQualityCleanupIntervalSeconds)*time.Second,
		pinPriors,
	)
	if priors := strings.TrimSpace(cfg.CallCorrection.QualityPriorsFile); priors != "" {
		if n, err := spot.LoadCallQualityPriors(priors, cfg.CallCorrection.QualityBinHz); err != nil {
			log.Printf("Warning: failed to load quality priors from %s: %v", priors, err)
		} else {
			log.Printf("Loaded %d call quality priors from %s", n, priors)
		}
	}
	var spotterReliability spot.SpotterReliability
	var spotterReliabilityCW spot.SpotterReliability
	var spotterReliabilityRTTY spot.SpotterReliability
	var confusionModel *spot.ConfusionModel
	if relPath := strings.TrimSpace(cfg.CallCorrection.SpotterReliabilityFile); relPath != "" {
		if rel, n, err := spot.LoadSpotterReliability(relPath); err != nil {
			log.Printf("Warning: failed to load spotter reliability from %s: %v", relPath, err)
		} else {
			spotterReliability = rel
			log.Printf("Loaded %d spotter reliability weights from %s", n, relPath)
		}
	}
	if relPath := strings.TrimSpace(cfg.CallCorrection.SpotterReliabilityFileCW); relPath != "" {
		if rel, n, err := spot.LoadSpotterReliability(relPath); err != nil {
			log.Printf("Warning: failed to load CW spotter reliability from %s: %v", relPath, err)
		} else {
			spotterReliabilityCW = rel
			log.Printf("Loaded %d CW spotter reliability weights from %s", n, relPath)
		}
	}
	if relPath := strings.TrimSpace(cfg.CallCorrection.SpotterReliabilityFileRTTY); relPath != "" {
		if rel, n, err := spot.LoadSpotterReliability(relPath); err != nil {
			log.Printf("Warning: failed to load RTTY spotter reliability from %s: %v", relPath, err)
		} else {
			spotterReliabilityRTTY = rel
			log.Printf("Loaded %d RTTY spotter reliability weights from %s", n, relPath)
		}
	}
	if cfg.CallCorrection.ConfusionModelEnabled {
		modelPath := strings.TrimSpace(cfg.CallCorrection.ConfusionModelFile)
		if modelPath == "" {
			log.Printf("Warning: call correction confusion model enabled but confusion_model_file is empty")
		} else if loaded, err := spot.LoadConfusionModel(modelPath); err != nil {
			log.Printf("Warning: failed to load confusion model from %s: %v", modelPath, err)
		} else {
			confusionModel = loaded
			log.Printf("Loaded call correction confusion model from %s", modelPath)
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
	dropDedupeWindow := time.Duration(cfg.Logging.DropDedupeWindowSeconds) * time.Second
	dropLogDeduper := newDropLogDeduper(dropDedupeWindow, defaultDropLogDedupeMaxKeys)
	dropReporter := makeDroppedReporter(surface, dropLogDeduper)
	unlicensedReporter := makeUnlicensedReporter(surface, statsTracker, dropLogDeduper)

	var repGate *reputation.Gate
	var repDropReporter func(reputation.DropEvent)
	if cfg.Reputation.Enabled {
		gate, err := reputation.NewGate(cfg.Reputation, ctyLookup)
		if err != nil {
			log.Printf("Warning: reputation gate disabled: %v", err)
		} else {
			repGate = gate
			repGate.Start(ctx)
			repDropReporter = makeReputationDropReporter(surface, dropReporter, statsTracker, cfg.Reputation)
		}
	}

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
		correctionIndex.StartCleanup(time.Minute, callCorrectionCleanupWindow(cfg.CallCorrection))
	}
	var callCooldown *spot.CallCooldown
	if cfg.CallCorrection.CooldownEnabled {
		callCooldown = spot.NewCallCooldown(spot.CallCooldownConfig{
			Enabled:          cfg.CallCorrection.CooldownEnabled,
			MinReporters:     cfg.CallCorrection.CooldownMinReporters,
			Duration:         time.Duration(cfg.CallCorrection.CooldownDurationSeconds) * time.Second,
			TTL:              time.Duration(cfg.CallCorrection.CooldownTTLSeconds) * time.Second,
			BinHz:            cfg.CallCorrection.CooldownBinHz,
			MaxReporters:     cfg.CallCorrection.CooldownMaxReporters,
			BypassAdvantage:  cfg.CallCorrection.CooldownBypassAdvantage,
			BypassConfidence: cfg.CallCorrection.CooldownBypassConfidence,
		})
		callCooldown.StartCleanup(time.Duration(cfg.CallCorrection.CooldownTTLSeconds) * time.Second)
	}
	var recentBandStore *spot.RecentBandStore
	if cfg.CallCorrection.Enabled && (cfg.CallCorrection.RecentBandBonusEnabled || cfg.CallCorrection.StabilizerEnabled) {
		recentBandStore = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
		recentBandStore.StartCleanup()
	}

	var knownCalls atomic.Pointer[spot.KnownCallsigns]
	knownCallsPath := strings.TrimSpace(cfg.KnownCalls.File)
	knownCallsURL := strings.TrimSpace(cfg.KnownCalls.URL)
	if knownCallsPath != "" {
		if _, err := os.Stat(knownCallsPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if knownCallsURL != "" {
					if fresh, updated, refreshErr := refreshKnownCallsigns(ctx, cfg.KnownCalls); refreshErr != nil {
						log.Printf("Warning: known calls download failed: %v", refreshErr)
					} else if updated && fresh != nil {
						knownCalls.Store(fresh)
						log.Printf("Downloaded %d known callsigns from %s", fresh.Count(), knownCallsURL)
					} else {
						log.Printf("Known calls file already up to date (%s)", knownCallsPath)
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
	gridOpts := gridstore.Options{
		CacheSizeBytes:        int64(cfg.GridBlockCacheMB) << 20,
		BloomFilterBitsPerKey: cfg.GridBloomFilterBits,
		MemTableSizeBytes:     uint64(cfg.GridMemTableSizeMB) << 20,
		L0CompactionThreshold: cfg.GridL0Compaction,
		L0StopWritesThreshold: cfg.GridL0StopWrites,
		WriteQueueDepth:       cfg.GridWriteQueueDepth,
	}
	gridStoreHandle := newGridStoreHandle(nil)
	gridStore, err := gridstore.Open(cfg.GridDBPath, gridOpts)
	if err != nil {
		if pebble.IsCorruptionError(err) {
			log.Printf("Gridstore: corruption detected on open (%v); starting checkpoint restore", err)
			startGridStoreRecovery(ctx, gridStoreHandle, cfg.GridDBPath, gridOpts, &knownCalls, metaCache)
		} else {
			log.Printf("Failed to open grid database: %v", err)
			return
		}
	} else {
		gridStoreHandle.Set(gridStore)
		if known := knownCalls.Load(); known != nil {
			if err := seedKnownCalls(gridStore, known); err != nil {
				log.Printf("Warning: failed to seed known calls into grid database: %v", err)
			}
		}
	}
	defer gridStoreHandle.Close()

	gridDBCheckOnMiss, gridDBCheckSource := gridDBCheckOnMissEnabled(cfg)
	log.Printf("Gridstore: db_check_on_miss=%v (source=%s)", gridDBCheckOnMiss, gridDBCheckSource)

	gridTTL := time.Duration(cfg.GridTTLDays) * 24 * time.Hour
	gridUpdater, ctyUpdater, gridUpdateState, stopGridWriter, gridLookup, gridLookupSync := startGridWriter(gridStoreHandle, time.Duration(cfg.GridFlushSec)*time.Second, metaCache, gridTTL, gridDBCheckOnMiss, ctyLookup)
	defer func() {
		if stopGridWriter != nil {
			stopGridWriter()
		}
	}()
	startGridCheckpointScheduler(ctx, gridStoreHandle, cfg.GridDBPath)
	startGridIntegrityScheduler(ctx, gridStoreHandle)

	if cfg.KnownCalls.Enabled && knownCallsURL != "" && knownCallsPath != "" {
		if knownCalls.Load() != nil {
			startKnownCallScheduler(ctx, cfg.KnownCalls, &knownCalls, gridStoreHandle, metaCache)
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
			if count, err := refreshSkewTable(ctx, cfg.Skew, skewStore); err != nil {
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

	dedupInput := deduplicator.GetInputChannel()
	ingestValidator := newIngestValidator(ctyLookup, metaCache, ctyUpdater, gridUpdater, dedupInput, unlicensedReporter, dropReporter, cfg.CTY.Enabled)
	ingestValidator.Start()
	ingestInput := ingestValidator.Input()

	secondaryFastWindow := time.Duration(cfg.Dedup.SecondaryFastWindowSeconds) * time.Second
	secondaryMedWindow := time.Duration(cfg.Dedup.SecondaryMedWindowSeconds) * time.Second
	secondarySlowWindow := time.Duration(cfg.Dedup.SecondarySlowWindowSeconds) * time.Second
	var secondaryFast *dedup.SecondaryDeduper
	var secondaryMed *dedup.SecondaryDeduper
	var secondarySlow *dedup.SecondaryDeduper
	var archivePeerSecondaryMed *dedup.SecondaryDeduper
	if secondaryFastWindow > 0 {
		secondaryFast = dedup.NewSecondaryDeduper(secondaryFastWindow, cfg.Dedup.SecondaryFastPreferStrong)
		secondaryFast.Start()
		log.Printf("Secondary dedupe (fast) active with %v window", secondaryFastWindow)
	} else {
		log.Println("Secondary dedupe (fast) disabled")
	}
	if secondaryMedWindow > 0 {
		secondaryMed = dedup.NewSecondaryDeduper(secondaryMedWindow, cfg.Dedup.SecondaryMedPreferStrong)
		secondaryMed.Start()
		log.Printf("Secondary dedupe (med) active with %v window", secondaryMedWindow)
	} else {
		log.Println("Secondary dedupe (med) disabled")
	}
	if secondarySlowWindow > 0 {
		secondarySlow = dedup.NewSecondaryDeduperWithKey(secondarySlowWindow, cfg.Dedup.SecondarySlowPreferStrong, dedup.SecondaryKeyCQZone)
		secondarySlow.Start()
		log.Printf("Secondary dedupe (slow) active with %v window", secondarySlowWindow)
	} else {
		log.Println("Secondary dedupe (slow) disabled")
	}
	if cfg.CallCorrection.Enabled && cfg.CallCorrection.StabilizerEnabled {
		archiveWindow := time.Duration(0)
		archivePreferStrong := false
		archivePolicy := "disabled"
		archiveKeyMode := dedup.SecondaryKeyGrid2
		switch {
		case secondaryMedWindow > 0:
			archiveWindow = secondaryMedWindow
			archivePreferStrong = cfg.Dedup.SecondaryMedPreferStrong
			archivePolicy = "med"
		case secondaryFastWindow > 0:
			archiveWindow = secondaryFastWindow
			archivePreferStrong = cfg.Dedup.SecondaryFastPreferStrong
			archivePolicy = "fast-fallback"
		case secondarySlowWindow > 0:
			archiveWindow = secondarySlowWindow
			archivePreferStrong = cfg.Dedup.SecondarySlowPreferStrong
			archivePolicy = "slow-fallback"
			archiveKeyMode = dedup.SecondaryKeyCQZone
		}
		if archiveWindow > 0 {
			if archiveKeyMode == dedup.SecondaryKeyCQZone {
				archivePeerSecondaryMed = dedup.NewSecondaryDeduperWithKey(archiveWindow, archivePreferStrong, archiveKeyMode)
			} else {
				archivePeerSecondaryMed = dedup.NewSecondaryDeduper(archiveWindow, archivePreferStrong)
			}
			archivePeerSecondaryMed.Start()
			log.Printf("Archive/peer secondary dedupe (%s) active with %v window (stabilizer split)", archivePolicy, archiveWindow)
		}
	}
	if secondaryFastWindow <= 0 && secondaryMedWindow <= 0 && secondarySlowWindow <= 0 {
		log.Println("Warning: secondary dedupe disabled (fast+med+slow=0); spots broadcast without secondary suppression")
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
		pm, err := peer.NewManager(cfg.Peering, cfg.Peering.LocalCallsign, ingestInput, cfg.SpotPolicy.MaxAgeSeconds, dropReporter)
		if err != nil {
			log.Printf("Failed to init peering manager: %v", err)
			return
		}
		if err := pm.Start(ctx); err != nil {
			log.Printf("Failed to start peering manager: %v", err)
			return
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
	processor := commands.NewProcessor(spotBuffer, archiveWriter, ingestInput, ctyLookup, repGate, repDropReporter)

	// Create and start telnet server
	telnetServer := telnet.NewServer(telnet.ServerOptions{
		Port:                    cfg.Telnet.Port,
		WelcomeMessage:          cfg.Telnet.WelcomeMessage,
		DuplicateLoginMsg:       cfg.Telnet.DuplicateLoginMsg,
		LoginGreeting:           cfg.Telnet.LoginGreeting,
		LoginPrompt:             cfg.Telnet.LoginPrompt,
		LoginEmptyMessage:       cfg.Telnet.LoginEmptyMessage,
		LoginInvalidMessage:     cfg.Telnet.LoginInvalidMessage,
		InputTooLongMessage:     cfg.Telnet.InputTooLongMessage,
		InputInvalidCharMessage: cfg.Telnet.InputInvalidCharMessage,
		DialectWelcomeMessage:   cfg.Telnet.DialectWelcomeMessage,
		DialectSourceDefault:    cfg.Telnet.DialectSourceDefaultLabel,
		DialectSourcePersisted:  cfg.Telnet.DialectSourcePersistedLabel,
		PathStatusMessage:       cfg.Telnet.PathStatusMessage,
		ClusterCall:             cfg.Server.NodeID,
		MaxConnections:          cfg.Telnet.MaxConnections,
		BroadcastWorkers:        cfg.Telnet.BroadcastWorkers,
		BroadcastQueue:          cfg.Telnet.BroadcastQueue,
		WorkerQueue:             cfg.Telnet.WorkerQueue,
		ClientBuffer:            cfg.Telnet.ClientBuffer,
		ControlQueue:            cfg.Telnet.ControlQueueSize,
		BroadcastBatchInterval:  time.Duration(cfg.Telnet.BroadcastBatchIntervalMS) * time.Millisecond,
		Transport:               cfg.Telnet.Transport,
		EchoMode:                cfg.Telnet.EchoMode,
		SkipHandshake:           cfg.Telnet.SkipHandshake,
		ReadIdleTimeout:         time.Duration(cfg.Telnet.ReadIdleTimeoutSeconds) * time.Second,
		LoginTimeout:            time.Duration(cfg.Telnet.LoginTimeoutSeconds) * time.Second,
		LoginLineLimit:          cfg.Telnet.LoginLineLimit,
		CommandLineLimit:        cfg.Telnet.CommandLineLimit,
		DropExtremeRate:         cfg.Telnet.DropExtremeRate,
		DropExtremeWindow:       time.Duration(cfg.Telnet.DropExtremeWindowSeconds) * time.Second,
		DropExtremeMinAttempts:  cfg.Telnet.DropExtremeMinAttempts,
		ReputationGate:          repGate,
		PathPredictor:           pathPredictor,
		PathDisplayEnabled:      pathCfg.DisplayEnabled,
		NoiseOffsets:            pathCfg.NoiseOffsets,
		GridLookup:              gridLookup,
		CTYLookup:               ctyLookup,
		DedupeFastEnabled:       secondaryFastWindow > 0,
		DedupeMedEnabled:        secondaryMedWindow > 0,
		DedupeSlowEnabled:       secondarySlowWindow > 0,
		NearbyLoginWarning:      cfg.Telnet.NearbyLoginWarning,
		SolarWeather:            solarMgr,
	}, processor)

	err = telnetServer.Start()
	if err != nil {
		log.Printf("Failed to start telnet server: %v", err)
		return
	}
	telnetServer.SetClientListListener(func() {
		if surface == nil {
			return
		}
		lines := formatNetworkLines(telnetServer, telnetServer.ListClientCallsigns())
		if len(lines) == 0 {
			return
		}
		surface.UpdateNetworkStatus(lines[0], lines[1:])
	})
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
	var secondaryStageCount atomic.Uint64
	// Purpose: Run the single-threaded output pipeline for deduped spots.
	// Key aspects: Handles corrections, licensing, secondary dedupe, and fan-out.
	// Upstream: main startup after wiring dependencies.
	// Downstream: processOutputSpots.
	pathReport := newPathReportMetrics()
	pskrPathOnlyStats := &pathOnlyStats{}
	go processOutputSpots(deduplicator, secondaryFast, secondaryMed, secondarySlow, archivePeerSecondaryMed, &secondaryStageCount, modeAssigner, spotBuffer, telnetServer, peerManager, statsTracker, correctionIndex, cfg.CallCorrection, ctyLookup, metaCache, harmonicDetector, cfg.Harmonics, &knownCalls, freqAverager, cfg.SpotPolicy, surface, gridUpdater, gridLookup, gridLookupSync, unlicensedReporter, corrLogger, callCooldown, adaptiveMinReports, refresher, spotterReliability, spotterReliabilityCW, spotterReliabilityRTTY, confusionModel, recentBandStore, cfg.RBN.KeepSSIDSuffix, archiveWriter, &lastOutput, pathPredictor, pathReport, allowedBandSet)
	startPipelineHealthMonitor(ctx, deduplicator, &lastOutput, peerManager)

	// Connect to RBN CW/RTTY feed if enabled (port 7000)
	// RBN spots go INTO the deduplicator input channel
	var rbnClient *rbn.Client
	if cfg.RBN.Enabled {
		rbnClient = rbn.NewClient(cfg.RBN.Host, cfg.RBN.Port, cfg.RBN.Callsign, cfg.RBN.Name, skewStore, cfg.RBN.KeepSSIDSuffix, cfg.RBN.SlotBuffer)
		rbnClient.SetTelnetTransport(cfg.RBN.TelnetTransport)
		if cfg.RBN.KeepaliveSec > 0 {
			rbnClient.EnableKeepalive(time.Duration(cfg.RBN.KeepaliveSec) * time.Second)
		}
		err = rbnClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN CW/RTTY: %v", err)
		} else {
			// Purpose: Pump CW/RTTY RBN spots into the dedup input channel.
			// Key aspects: Runs in its own goroutine to keep ingest non-blocking.
			// Upstream: main startup after RBN connect.
			// Downstream: processRBNSpots.
			go processRBNSpots(rbnClient, ingestInput, "RBN-CW", cfg.SpotPolicy)
			log.Println("RBN CW/RTTY client feeding spots into unified dedup engine")
		}
	}

	// Connect to RBN Digital feed if enabled (port 7001 - FT4/FT8)
	// RBN Digital spots go INTO the deduplicator input channel
	var rbnDigitalClient *rbn.Client
	if cfg.RBNDigital.Enabled {
		rbnDigitalClient = rbn.NewClient(cfg.RBNDigital.Host, cfg.RBNDigital.Port, cfg.RBNDigital.Callsign, cfg.RBNDigital.Name, skewStore, cfg.RBNDigital.KeepSSIDSuffix, cfg.RBNDigital.SlotBuffer)
		rbnDigitalClient.SetTelnetTransport(cfg.RBNDigital.TelnetTransport)
		if cfg.RBNDigital.KeepaliveSec > 0 {
			rbnDigitalClient.EnableKeepalive(time.Duration(cfg.RBNDigital.KeepaliveSec) * time.Second)
		}
		err = rbnDigitalClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to RBN Digital: %v", err)
		} else {
			// Purpose: Pump FT4/FT8 RBN digital spots into the dedup input channel.
			// Key aspects: Runs in its own goroutine to keep ingest non-blocking.
			// Upstream: main startup after RBN Digital connect.
			// Downstream: processRBNSpots.
			go processRBNSpots(rbnDigitalClient, ingestInput, "RBN-FT", cfg.SpotPolicy)
			log.Println("RBN Digital (FT4/FT8) client feeding spots into unified dedup engine")
		}
	}

	// Connect to human/relay telnet feed if enabled (upstream cluster or operator-submitted spots)
	var humanTelnetClient *rbn.Client
	if cfg.HumanTelnet.Enabled {
		rawPassthrough := make(chan string, 256)
		// Purpose: Forward non-DX lines (WWV/announcements) to telnet clients.
		// Key aspects: Filters by line type; exits when channel closes.
		// Upstream: humanTelnetClient raw passthrough channel.
		// Downstream: telnetServer.BroadcastWWV and BroadcastAnnouncement.
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

		humanTelnetClient = rbn.NewClient(cfg.HumanTelnet.Host, cfg.HumanTelnet.Port, cfg.HumanTelnet.Callsign, cfg.HumanTelnet.Name, skewStore, cfg.HumanTelnet.KeepSSIDSuffix, cfg.HumanTelnet.SlotBuffer)
		humanTelnetClient.SetTelnetTransport(cfg.HumanTelnet.TelnetTransport)
		humanTelnetClient.UseMinimalParser()
		humanTelnetClient.SetRawPassthrough(rawPassthrough)
		if cfg.HumanTelnet.KeepaliveSec > 0 {
			// Prevent idle disconnects on upstream telnet feeds by sending periodic CRLF.
			humanTelnetClient.EnableKeepalive(time.Duration(cfg.HumanTelnet.KeepaliveSec) * time.Second)
		}
		err = humanTelnetClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to human/relay telnet feed: %v", err)
		} else {
			// Purpose: Pump human/relay telnet spots into the dedup input channel.
			// Key aspects: Runs in its own goroutine to keep ingest non-blocking.
			// Upstream: main startup after human telnet connect.
			// Downstream: processHumanTelnetSpots.
			go processHumanTelnetSpots(humanTelnetClient, ingestInput, "HUMAN-TELNET", cfg.SpotPolicy)
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
		mqttQoS12Timeout := time.Duration(cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS) * time.Millisecond
		pskrClient = pskreporter.NewClient(
			cfg.PSKReporter.Broker,
			cfg.PSKReporter.Port,
			pskrTopics,
			cfg.PSKReporter.Modes,
			cfg.PSKReporter.PathOnlyModes,
			cfg.PSKReporter.Name,
			cfg.PSKReporter.Workers,
			cfg.PSKReporter.MQTTInboundWorkers,
			cfg.PSKReporter.MQTTInboundQueueDepth,
			mqttQoS12Timeout,
			skewStore,
			cfg.PSKReporter.AppendSpotterSSID,
			cfg.PSKReporter.SpotChannelSize,
			cfg.PSKReporter.MaxPayloadBytes,
		)
		err = pskrClient.Connect()
		if err != nil {
			log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		} else {
			// Purpose: Pump PSKReporter spots into the dedup input channel.
			// Key aspects: Runs in its own goroutine to keep ingest non-blocking.
			// Upstream: main startup after PSKReporter connect.
			// Downstream: processPSKRSpots.
			go processPSKRSpots(pskrClient, ingestInput, cfg.SpotPolicy)
			// Purpose: Pump PSKReporter path-only spots into the path predictor.
			// Key aspects: Runs in its own goroutine; never touches dedup/broadcast.
			// Upstream: main startup after PSKReporter connect.
			// Downstream: processPSKRPathOnlySpots.
			go processPSKRPathOnlySpots(pskrClient, pathPredictor, pathReport, pskrPathOnlyStats, cfg.SpotPolicy, allowedBandSet)
			log.Println("PSKReporter client feeding spots into unified dedup engine")
		}
	}

	ingestSources := make([]ingestHealthSource, 0, 4)
	if rbnClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(cfg.RBN.Name, "RBN"), rbnClient))
	}
	if rbnDigitalClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(cfg.RBNDigital.Name, "RBN Digital"), rbnDigitalClient))
	}
	if humanTelnetClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(cfg.HumanTelnet.Name, "Human Telnet"), humanTelnetClient))
	}
	if pskrClient != nil {
		ingestSources = append(ingestSources, pskReporterHealthSource(ingestSourceName(cfg.PSKReporter.Name, "PSKReporter"), pskrClient))
	}
	startIngestHealthMonitor(ctx, ingestSources)

	// Start stats display goroutine
	statsInterval := time.Duration(cfg.Stats.DisplayIntervalSeconds) * time.Second
	// Purpose: Periodically emit stats to UI or logs.
	// Key aspects: Runs on ticker interval until shutdown.
	// Upstream: main startup.
	// Downstream: displayStatsWithFCC.
	go displayStatsWithFCC(statsInterval, statsTracker, ingestValidator, deduplicator, secondaryFast, secondaryMed, secondarySlow, &secondaryStageCount, spotBuffer, ctyLookup, metaCache, ctyState, &knownCalls, recentBandStore, knownCallsPath, telnetServer, surface, gridUpdateState, gridStoreHandle, cfg.FCCULS.DBPath, pathPredictor, rbnClient, rbnDigitalClient, pskrClient, pskrPathOnlyStats, peerManager, cfg.Server.NodeID, cfg.Skew.File)
	if pathCfg.Enabled {
		go startPathPredictionLogger(ctx, logMux, telnetServer, pathPredictor, pathReport)
	}

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
	maybeStartContentionProfiling()
	maybeStartHeapLogger()
	maybeStartMapLogger(statsTracker, pathPredictor, deduplicator, secondaryFast, secondaryMed, secondarySlow)
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
	if callCooldown != nil {
		callCooldown.StopCleanup()
	}
	if recentBandStore != nil {
		recentBandStore.StopCleanup()
	}

	// Stop deduplicator
	if deduplicator != nil {
		deduplicator.Stop()
	}
	if peerManager != nil {
		peerManager.Stop()
	}
	if secondaryFast != nil {
		secondaryFast.Stop()
	}
	if secondaryMed != nil {
		secondaryMed.Stop()
	}
	if secondarySlow != nil {
		secondarySlow.Stop()
	}
	if archivePeerSecondaryMed != nil {
		archivePeerSecondaryMed.Stop()
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

// Purpose: Build a reporter callback for unlicensed drops.
// Key aspects: Returns a closure that increments stats and formats output.
// Upstream: main wiring for applyLicenseGate reporting.
// Downstream: tracker.IncrementUnlicensedDrops and dash.AppendUnlicensed/log.Println.
func makeUnlicensedReporter(dash ui.Surface, tracker *stats.Tracker, deduper *dropLogDeduper) func(source, role, call, mode string, freq float64) {
	// Purpose: Emit an unlicensed drop event with consistent formatting.
	// Key aspects: Normalizes fields and routes to UI or log.
	// Upstream: applyLicenseGate.
	// Downstream: tracker.IncrementUnlicensedDrops, dash.AppendUnlicensed, log.Println.
	return func(source, role, call, mode string, freq float64) {
		if tracker != nil {
			tracker.IncrementUnlicensedDrops()
		}
		source = strings.ToUpper(strings.TrimSpace(source))
		role = strings.ToUpper(strings.TrimSpace(role))
		mode = strings.ToUpper(strings.TrimSpace(mode))
		call = strings.TrimSpace(strings.ToUpper(call))

		message := formatUnlicensedDropMessage(role, call, source, mode, freq)
		if dash != nil {
			if line, ok := deduper.Process(message); ok {
				dash.AppendUnlicensed(line)
			}
			return
		}
		if line, ok := deduper.Process(message); ok {
			log.Println(line)
		}
	}
}

func formatUnlicensedDropMessage(role, call, source, mode string, freq float64) string {
	return fmt.Sprintf("Unlicensed US %s %s dropped from %s %s @ %.1f kHz", role, call, source, mode, freq)
}

// Purpose: Build a reporter callback for dropped events.
// Key aspects: Routes to dropped pane when UI is active, otherwise logs.
// Upstream: CTY/PC61/reputation drop paths.
// Downstream: dash.AppendDropped and log.Print.
func makeDroppedReporter(dash ui.Surface, deduper *dropLogDeduper) func(line string) {
	return func(line string) {
		if line == "" {
			return
		}
		processed, ok := deduper.Process(line)
		if !ok {
			return
		}
		if dash != nil {
			dash.AppendDropped(processed)
			return
		}
		log.Print(processed)
	}
}

// Purpose: Build a reporter for reputation gate drops.
// Key aspects: Updates counters and routes to the dropped pane or logs.
// Upstream: Reputation gate in telnet command path.
// Downstream: stats tracker and dropped/system logs.
func makeReputationDropReporter(dash ui.Surface, dropReporter func(string), tracker *stats.Tracker, cfg config.ReputationConfig) func(reputation.DropEvent) {
	if tracker == nil {
		return nil
	}
	sampleEvery := sampleEveryN(cfg.DropLogSampleRate)
	var counter atomic.Uint64
	return func(ev reputation.DropEvent) {
		tracker.IncrementReputationDrop(string(ev.Reason))
		if !cfg.ConsoleDropDisplay || sampleEvery == 0 {
			return
		}
		if sampleEvery > 1 {
			if counter.Add(1)%uint64(sampleEvery) != 0 {
				return
			}
		}
		line := formatReputationDropLine(ev)
		if dash != nil {
			dash.AppendReputation(line)
			return
		}
		if dropReporter != nil {
			dropReporter(line)
			return
		}
		log.Print(line)
	}
}

func formatReputationDropLine(ev reputation.DropEvent) string {
	call := strings.TrimSpace(ev.Call)
	if max := spot.MaxCallsignLength(); max > 0 && len(call) > max {
		call = call[:max]
	}
	band := spot.NormalizeBand(ev.Band)
	if band == "" {
		band = "???"
	}
	reason := string(ev.Reason)
	if reason == "" {
		reason = "unknown"
	}
	flags := formatPenaltyFlags(ev.Flags)
	asn := strings.TrimSpace(ev.ASN)
	country := strings.TrimSpace(ev.CountryCode)
	if country == "" {
		country = strings.TrimSpace(ev.CountryName)
	}
	prefix := strings.TrimSpace(ev.Prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	return fmt.Sprintf("Reputation drop: %s band=%s reason=%s ip=%s asn=%s country=%s flags=%s",
		call, band, reason, prefix, emptyOr(asn, "unknown"), emptyOr(country, "unknown"), flags)
}

func formatHarmonicSuppressedMessage(dxCall string, from, to float64, corroborators, deltaDB int) string {
	return fmt.Sprintf("Harmonic suppressed: %s %.1f -> %.1f kHz (%d / %d dB)", dxCall, from, to, corroborators, deltaDB)
}

func formatCallCorrectedMessage(fromCall, toCall string, freq float64, supporters, correctedConfidence int) string {
	return fmt.Sprintf("Call corrected: %s -> %s at %.1f kHz (%d / %d%%)",
		fromCall, toCall, freq, supporters, correctedConfidence)
}

func formatPenaltyFlags(flags reputation.PenaltyFlags) string {
	if flags == 0 {
		return "none"
	}
	out := make([]string, 0, 4)
	if flags.Has(reputation.PenaltyCountryMismatch) {
		out = append(out, "mismatch")
	}
	if flags.Has(reputation.PenaltyASNReset) {
		out = append(out, "asn_new")
	}
	if flags.Has(reputation.PenaltyGeoFlip) {
		out = append(out, "geo_flip")
	}
	if flags.Has(reputation.PenaltyDisagreement) {
		out = append(out, "disagree")
	}
	if flags.Has(reputation.PenaltyUnknown) {
		out = append(out, "unknown")
	}
	return strings.Join(out, ",")
}

func sampleEveryN(rate float64) int {
	if rate <= 0 {
		return 0
	}
	if rate >= 1 {
		return 1
	}
	n := int(1.0 / rate)
	if n < 1 {
		return 1
	}
	return n
}

func emptyOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func formatCorrectionDecisionSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "CorrGate: n/a"
	}
	total := tracker.CorrectionDecisionTotal()
	applied := tracker.CorrectionDecisionApplied()
	rejected := tracker.CorrectionDecisionRejected()
	fallback := tracker.CorrectionFallbackApplied()
	prior := tracker.CorrectionPriorBonusUsed()
	reasons := formatTopCounterSummary(tracker.CorrectionDecisionReasons(), 2)
	paths := formatTopCounterSummary(tracker.CorrectionDecisionPaths(), 2)
	return fmt.Sprintf("CorrGate: %s (T) / %s (A) / %s (R) / %s (FB) / %s (PB) [%s] [%s]",
		humanize.Comma(int64(total)),
		humanize.Comma(int64(applied)),
		humanize.Comma(int64(rejected)),
		humanize.Comma(int64(fallback)),
		humanize.Comma(int64(prior)),
		reasons,
		paths,
	)
}

func formatStabilizerSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "Stabilizer: n/a"
	}
	held := tracker.StabilizerHeld()
	immediate := tracker.StabilizerReleasedImmediate()
	delayed := tracker.StabilizerReleasedDelayed()
	suppressed := tracker.StabilizerSuppressedTimeout()
	overflow := tracker.StabilizerOverflowRelease()
	return fmt.Sprintf("Stabilizer: %s (H) / %s (I) / %s (D) / %s (S) / %s (O)",
		humanize.Comma(int64(held)),
		humanize.Comma(int64(immediate)),
		humanize.Comma(int64(delayed)),
		humanize.Comma(int64(suppressed)),
		humanize.Comma(int64(overflow)),
	)
}

func formatTopCounterSummary(counts map[string]uint64, limit int) string {
	if len(counts) == 0 {
		return "none"
	}
	type pair struct {
		key   string
		count uint64
	}
	items := make([]pair, 0, len(counts))
	for key, count := range counts {
		items = append(items, pair{key: key, count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].key < items[j].key
		}
		return items[i].count > items[j].count
	})
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	var b strings.Builder
	for i := 0; i < limit; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%d", items[i].key, items[i].count)
	}
	return b.String()
}

// Purpose: Periodically emit stats with FCC metadata refresh.
// Key aspects: Uses a ticker, diff counters, and optional secondary dedupe stats.
// Upstream: main stats goroutine.
// Downstream: tracker accessors, loadFCCSnapshot, and UI/log output.
func displayStatsWithFCC(interval time.Duration, tracker *stats.Tracker, ingestStats *ingestValidator, dedup *dedup.Deduplicator, secondaryFast *dedup.SecondaryDeduper, secondaryMed *dedup.SecondaryDeduper, secondarySlow *dedup.SecondaryDeduper, secondaryStage *atomic.Uint64, buf *buffer.RingBuffer, ctyLookup func() *cty.CTYDatabase, metaCache *callMetaCache, ctyState *ctyRefreshState, knownPtr *atomic.Pointer[spot.KnownCallsigns], recentBandStore *spot.RecentBandStore, knownCallsPath string, telnetSrv *telnet.Server, dash ui.Surface, gridStats *gridMetrics, gridDB *gridStoreHandle, fccDBPath string, pathPredictor *pathreliability.Predictor, rbnClient *rbn.Client, rbnDigitalClient *rbn.Client, pskrClient *pskreporter.Client, pskrPathOnly *pathOnlyStats, peerManager *peer.Manager, clusterCall string, skewPath string) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prevSourceCounts := make(map[string]uint64)
	prevSourceModeCounts := make(map[string]uint64)
	var prevPathOnly pathOnlySnapshot
	var fccSnap *fccSnapshot
	if !uls.RefreshInProgress() {
		fccSnap = loadFCCSnapshot(fccDBPath)
	}
	gcWindow := &gcPauseWindow{}
	{
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		_, _, _ = gcWindow.snapshot(&mem)
	}

	for range ticker.C {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		gcP99, gcCount, _ := gcWindow.snapshot(&mem)
		gcP99Label := "n/a"
		if gcCount > 0 {
			gcP99Label = formatDurationMillis(gcP99)
		}

		// Refresh FCC snapshot each interval to reflect completed downloads/builds.
		// Skip reads while a refresh/swap is active to avoid holding the DB open.
		if !uls.RefreshInProgress() {
			fccSnap = loadFCCSnapshot(fccDBPath)
		}

		sourceTotals := tracker.GetSourceCounts()
		sourceModeTotals := tracker.GetSourceModeCounts()

		rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4 :=
			rbnIngestDeltas(sourceTotals, prevSourceCounts, sourceModeTotals, prevSourceModeCounts)

		// PSKReporter includes a per-mode breakdown in the stats ticker.
		pskTotal := diffCounter(sourceTotals, prevSourceCounts, "PSKREPORTER")
		pskCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "CW")
		pskRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "RTTY")
		pskFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT8")
		pskFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT4")
		pskMSK144 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "MSK144")
		// Peer ingest is summarized under the synthetic P92 label (stats key is PEER).
		p92Total := diffCounter(sourceTotals, prevSourceCounts, "PEER")

		totalCorrections := tracker.CallCorrections()
		totalUnlicensed := tracker.UnlicensedDrops()
		totalFreqCorrections := tracker.FrequencyCorrections()
		totalHarmonics := tracker.HarmonicSuppressions()
		reputationTotal := tracker.ReputationDrops()
		corrDecisionLine := formatCorrectionDecisionSummary(tracker)
		stabilizerLine := formatStabilizerSummary(tracker)

		ingestTotal := uint64(0)
		if ingestStats != nil {
			ingestTotal = ingestStats.IngestCount()
		}

		var pipelineLine string
		if dedup == nil {
			pipelineLine = "Pipeline: primary dedup disabled"
		} else {
			primaryProcessed, _, _ := dedup.GetStats()

			secondaryStageCount := uint64(0)
			if secondaryStage != nil {
				secondaryStageCount = secondaryStage.Load()
			}

			fastForwarded := secondaryStageCount
			medForwarded := secondaryStageCount
			slowForwarded := secondaryStageCount
			if secondaryFast != nil {
				secProcessed, secDupes, _ := secondaryFast.GetStats()
				if secDupes < secProcessed {
					fastForwarded = secProcessed - secDupes
				} else {
					fastForwarded = 0
				}
			}
			if secondaryMed != nil {
				secProcessed, secDupes, _ := secondaryMed.GetStats()
				if secDupes < secProcessed {
					medForwarded = secProcessed - secDupes
				} else {
					medForwarded = 0
				}
			}
			if secondarySlow != nil {
				secProcessed, secDupes, _ := secondarySlow.GetStats()
				if secDupes < secProcessed {
					slowForwarded = secProcessed - secDupes
				} else {
					slowForwarded = 0
				}
			}
			fallbackForwarded := fastForwarded
			if secondaryFast == nil {
				if secondaryMed != nil {
					fallbackForwarded = medForwarded
				} else if secondarySlow != nil {
					fallbackForwarded = slowForwarded
				}
			}
			if secondaryMed == nil {
				medForwarded = fallbackForwarded
			}
			if secondarySlow == nil {
				slowForwarded = fallbackForwarded
			}

			fastPercent := 0
			medPercent := 0
			slowPercent := 0
			if ingestTotal > 0 {
				fastPercent = int((fastForwarded * 100) / ingestTotal)
				medPercent = int((medForwarded * 100) / ingestTotal)
				slowPercent = int((slowForwarded * 100) / ingestTotal)
			}
			pipelineLine = fmt.Sprintf("Pipeline: %s | %s | %s/%d%% (F) / %s/%d%% (M) / %s/%d%% (S)",
				humanize.Comma(int64(ingestTotal)),
				humanize.Comma(int64(primaryProcessed)),
				humanize.Comma(int64(fastForwarded)),
				fastPercent,
				humanize.Comma(int64(medForwarded)),
				medPercent,
				humanize.Comma(int64(slowForwarded)),
				slowPercent)
		}

		var queueDrops, clientDrops, senderFailures uint64
		var clientCount int
		var clientList []string
		if telnetSrv != nil {
			queueDrops, clientDrops, senderFailures = telnetSrv.BroadcastMetricSnapshot()
			clientCount = telnetSrv.GetClientCount()
			clientList = telnetSrv.ListClientCallsigns()
		}

		combinedRBN := rbnTotal + rbnFTTotal
		now := time.Now().UTC()
		pskSnap := pskreporter.HealthSnapshot{}
		pathOnlyLine := "[yellow]Path[-]: n/a"
		if pskrClient != nil {
			pskSnap = pskrClient.HealthSnapshot()
			if pskSnap.PathOnlyQueueCap > 0 {
				current := snapshotPathOnly(pskrPathOnly)
				delta := diffPathOnly(current, prevPathOnly)
				prevPathOnly = current
				pathOnlyLine = fmt.Sprintf("[yellow]Path[-]: %s (U) / %s (S) / %s (N) / %s (G) / %s (H) / %s (B) / %s (M)",
					humanize.Comma(int64(delta.updates)),
					humanize.Comma(int64(delta.stale)),
					humanize.Comma(int64(delta.noSNR)),
					humanize.Comma(int64(delta.noGrid)),
					humanize.Comma(int64(delta.badH3)),
					humanize.Comma(int64(delta.badBand)),
					humanize.Comma(int64(delta.mode)),
				)
			}
		}

		rbnLive := rbnFeedsLive(rbnClient, rbnDigitalClient)
		pskLive := pskReporterLive(pskSnap, now)
		p92Live := peerManager != nil && peerManager.ActiveSessionCount() > 0

		lines := make([]string, 0, 11)
		lines = append(lines,
			fmt.Sprintf("%s   %s", formatUptimeLine(tracker.GetUptime()), formatMemoryLine(buf, dedup, secondaryFast, secondaryMed, secondarySlow, metaCache, knownPtr)), // 1
			formatGridLineOrPlaceholder(gridStats, gridDB, pathPredictor), // 2
			formatDataLineOrPlaceholder(ctyLookup, ctyState, fccSnap),     // 3
			fmt.Sprintf("%s: %d TOTAL / %d CW / %d RTTY / %d FT8 / %d FT4",
				withIngestStatusLabel("RBN", rbnLive), combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4), // 4
			fmt.Sprintf("%s: %s TOTAL / %s CW / %s RTTY / %s FT8 / %s FT4 / %s MSK144",
				withIngestStatusLabel("PSKReporter", pskLive),
				humanize.Comma(int64(pskTotal)),
				humanize.Comma(int64(pskCW)),
				humanize.Comma(int64(pskRTTY)),
				humanize.Comma(int64(pskFT8)),
				humanize.Comma(int64(pskFT4)),
				humanize.Comma(int64(pskMSK144)),
			), // 5
			fmt.Sprintf("%s: %s TOTAL", withIngestStatusLabel("P92", p92Live), humanize.Comma(int64(p92Total))), // 6
		)
		lines = append(lines, pathOnlyLine)
		lines = append(lines,
			fmt.Sprintf("Calls: %d (C) / %d (U) / %d (F) / %d (H) / %d (R)", totalCorrections, totalUnlicensed, totalFreqCorrections, totalHarmonics, reputationTotal), // 6
			corrDecisionLine,
			stabilizerLine,
			pipelineLine, // 7
			fmt.Sprintf("Telnet: %d clients. Drops: %d (Q) / %d (C) / %d (W)", clientCount, queueDrops, clientDrops, senderFailures), // 8
		)

		prevSourceCounts = sourceTotals
		prevSourceModeCounts = sourceModeTotals

		if dash != nil {
			dash.SetStats(lines)
			overviewLines := buildOverviewLines(tracker, dedup, secondaryFast, secondaryMed, secondarySlow, metaCache, knownPtr, recentBandStore, ctyState, knownCallsPath, fccSnap, gridStats, gridDB, pathPredictor, telnetSrv, clusterCall,
				rbnLive, pskLive, p92Live,
				combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4,
				pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskMSK144,
				p92Total,
				totalCorrections, totalUnlicensed, totalHarmonics, reputationTotal,
				pathOnlyLine,
				skewPath,
				&mem,
				gcP99Label,
			)
			ingestLines := []string{}
			if len(overviewLines) > 0 {
				ingestLines = append(ingestLines, overviewLines[0], "")
			}
			if len(overviewLines) > 7 {
				ingestLines = append(ingestLines, overviewLines[3], overviewLines[4], overviewLines[5], overviewLines[6], overviewLines[7])
			}
			snapshot := ui.Snapshot{
				GeneratedAt:   time.Now().UTC(),
				OverviewLines: overviewLines,
				IngestLines:   ingestLines,
				PipelineLines: []string{
					pipelineLine,
					fmt.Sprintf("Corrections: %d  Unlicensed: %d  Freq: %d  Harmonics: %d  Reputation: %d",
						totalCorrections, totalUnlicensed, totalFreqCorrections, totalHarmonics, reputationTotal),
					stabilizerLine,
				},
				NetworkLines: formatNetworkLines(telnetSrv, clientList),
			}
			dash.SetSnapshot(snapshot)
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
// Purpose: Feed RBN spots into the unified deduplicator input.
// Key aspects: Drops stale spots and avoids blocking on dedup input.
// Upstream: RBN client ingest goroutine.
// Downstream: deduplicator.GetInputChannel and isStale.
func processRBNSpots(client *rbn.Client, ingest chan<- *spot.Spot, source string, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		if isStale(spot, spotPolicy) {
			continue
		}
		// Non-blocking send to avoid wedging ingest if dedup blocks.
		select {
		case ingest <- spot:
		default:
			count := drops.Add(1)
			if count == 1 || count%100 == 0 {
				log.Printf("%s: Ingest input full, dropping spot (total drops=%d)", source, count)
			}
		}
	}
	log.Printf("%s: Spot processing stopped", source)
}

// processHumanTelnetSpots marks incoming telnet spots as human-sourced and sends them into dedup.
// Purpose: Feed upstream human telnet spots into dedup after tagging as human.
// Key aspects: Ensures SourceType/Mode defaults and enforces staleness guard.
// Upstream: human telnet client ingest.
// Downstream: deduplicator.GetInputChannel and isStale.
func processHumanTelnetSpots(client *rbn.Client, ingest chan<- *spot.Spot, source string, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
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
		case ingest <- sp:
		default:
			count := drops.Add(1)
			if count == 1 || count%100 == 0 {
				log.Printf("%s: Ingest input full, dropping spot (total drops=%d)", source, count)
			}
		}
	}
	log.Printf("%s: Spot processing stopped", source)
}

// processPSKRSpots receives spots from PSKReporter and sends to deduplicator
// PSKReporter → Deduplicator Input Channel
// Purpose: Feed PSKReporter spots into the unified deduplicator input.
// Key aspects: Drops stale spots and avoids blocking on dedup input.
// Upstream: PSKReporter client worker pool.
// Downstream: deduplicator.GetInputChannel and isStale.
func processPSKRSpots(client *pskreporter.Client, ingest chan<- *spot.Spot, spotPolicy config.SpotPolicy) {
	spotChan := client.GetSpotChannel()
	var drops atomic.Uint64

	for spot := range spotChan {
		if isStale(spot, spotPolicy) {
			continue
		}
		// Non-blocking send to avoid backing up the PSK worker pool when dedup is slow.
		select {
		case ingest <- spot:
		default:
			count := drops.Add(1)
			if count == 1 || count%100 == 0 {
				log.Printf("PSKReporter: Ingest input full, dropping spot (total drops=%d)", count)
			}
		}
	}
}

type pathOnlyStats struct {
	updates atomic.Uint64
	drops   atomic.Uint64
	stale   atomic.Uint64
	noSNR   atomic.Uint64
	noGrid  atomic.Uint64
	badH3   atomic.Uint64
	badBand atomic.Uint64
	off     atomic.Uint64
	mode    atomic.Uint64
}

type pathOnlySnapshot struct {
	updates uint64
	drops   uint64
	stale   uint64
	noSNR   uint64
	noGrid  uint64
	badH3   uint64
	badBand uint64
	off     uint64
	mode    uint64
}

type pathOnlyDropReason uint8

const (
	pathOnlyDropStale pathOnlyDropReason = iota
	pathOnlyDropNoSNR
	pathOnlyDropNoGrid
	pathOnlyDropBadH3
	pathOnlyDropBadBand
	pathOnlyDropOff
	pathOnlyDropMode
)

func snapshotPathOnly(stats *pathOnlyStats) pathOnlySnapshot {
	if stats == nil {
		return pathOnlySnapshot{}
	}
	return pathOnlySnapshot{
		updates: stats.updates.Load(),
		drops:   stats.drops.Load(),
		stale:   stats.stale.Load(),
		noSNR:   stats.noSNR.Load(),
		noGrid:  stats.noGrid.Load(),
		badH3:   stats.badH3.Load(),
		badBand: stats.badBand.Load(),
		off:     stats.off.Load(),
		mode:    stats.mode.Load(),
	}
}

func diffPathOnly(current, prev pathOnlySnapshot) pathOnlySnapshot {
	return pathOnlySnapshot{
		updates: diffCounterRaw(current.updates, prev.updates),
		drops:   diffCounterRaw(current.drops, prev.drops),
		stale:   diffCounterRaw(current.stale, prev.stale),
		noSNR:   diffCounterRaw(current.noSNR, prev.noSNR),
		noGrid:  diffCounterRaw(current.noGrid, prev.noGrid),
		badH3:   diffCounterRaw(current.badH3, prev.badH3),
		badBand: diffCounterRaw(current.badBand, prev.badBand),
		off:     diffCounterRaw(current.off, prev.off),
		mode:    diffCounterRaw(current.mode, prev.mode),
	}
}

// processPSKRPathOnlySpots receives path-only spots from PSKReporter and updates the path predictor.
// Purpose: Use WSPR (and other path-only modes) exclusively for path reliability ingestion.
// Key aspects: No CTY validation, no dedup/broadcast/archive; drops on missing grids or disabled predictor.
// Upstream: PSKReporter path-only channel.
// Downstream: pathreliability.Predictor.Update, pathReportMetrics.
func processPSKRPathOnlySpots(client *pskreporter.Client, predictor *pathreliability.Predictor, pathReport *pathReportMetrics, stats *pathOnlyStats, spotPolicy config.SpotPolicy, allowedBands map[string]struct{}) {
	if client == nil {
		return
	}
	spotChan := client.GetPathOnlyChannel()
	if spotChan == nil {
		return
	}
	for s := range spotChan {
		if s == nil {
			continue
		}
		if isStale(s, spotPolicy) {
			recordPathOnlyDrop(stats, pathOnlyDropStale)
			continue
		}
		if predictor == nil || !predictor.Config().Enabled {
			recordPathOnlyDrop(stats, pathOnlyDropOff)
			continue
		}
		if !s.HasReport {
			recordPathOnlyDrop(stats, pathOnlyDropNoSNR)
			continue
		}
		mode := s.ModeNorm
		if strings.TrimSpace(mode) == "" {
			mode = s.Mode
		}
		ft8, ok := pathreliability.FT8Equivalent(mode, s.Report, predictor.Config())
		if !ok {
			recordPathOnlyDrop(stats, pathOnlyDropMode)
			continue
		}
		bucket := pathreliability.BucketForIngest(mode)
		if bucket == pathreliability.BucketNone {
			recordPathOnlyDrop(stats, pathOnlyDropMode)
			continue
		}
		dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
		deGrid := strings.TrimSpace(s.DEMetadata.Grid)
		if dxGrid == "" || deGrid == "" {
			recordPathOnlyDrop(stats, pathOnlyDropNoGrid)
			continue
		}
		dxCell := pathreliability.EncodeCell(dxGrid)
		deCell := pathreliability.EncodeCell(deGrid)
		dxCoarse := pathreliability.EncodeCoarseCell(dxGrid)
		deCoarse := pathreliability.EncodeCoarseCell(deGrid)
		if (dxCell == pathreliability.InvalidCell || deCell == pathreliability.InvalidCell) &&
			(dxCoarse == pathreliability.InvalidCell || deCoarse == pathreliability.InvalidCell) {
			recordPathOnlyDrop(stats, pathOnlyDropBadH3)
			continue
		}
		band := s.BandNorm
		if strings.TrimSpace(band) == "" {
			band = s.Band
		}
		if strings.TrimSpace(band) == "" || band == "???" {
			band = spot.FreqToBand(s.Frequency)
		}
		band = strings.TrimSpace(spot.NormalizeBand(band))
		if band == "" || band == "???" {
			recordPathOnlyDrop(stats, pathOnlyDropBadBand)
			continue
		}
		if !allowedBand(allowedBands, band) {
			recordPathOnlyDrop(stats, pathOnlyDropBadBand)
			continue
		}
		spotTime := s.Time.UTC()
		if spotTime.IsZero() {
			spotTime = time.Now().UTC()
		}
		if pathReport != nil {
			pathReport.Observe(s, spotTime)
		}
		// Spot SNR reflects DX -> DE (spotter is the receiver).
		predictor.Update(bucket, deCell, dxCell, deCoarse, dxCoarse, band, ft8, 1.0, spotTime, s.IsBeacon)
		if stats != nil {
			stats.updates.Add(1)
		}
	}
}

// Purpose: Record a path-only drop and its reason.
// Key aspects: Increments total drop counter alongside reason-specific counter.
// Upstream: processPSKRPathOnlySpots drop paths.
// Downstream: atomic counters in pathOnlyStats.
func recordPathOnlyDrop(stats *pathOnlyStats, reason pathOnlyDropReason) {
	if stats == nil {
		return
	}
	stats.drops.Add(1)
	switch reason {
	case pathOnlyDropStale:
		stats.stale.Add(1)
	case pathOnlyDropNoSNR:
		stats.noSNR.Add(1)
	case pathOnlyDropNoGrid:
		stats.noGrid.Add(1)
	case pathOnlyDropBadH3:
		stats.badH3.Add(1)
	case pathOnlyDropBadBand:
		stats.badBand.Add(1)
	case pathOnlyDropOff:
		stats.off.Add(1)
	case pathOnlyDropMode:
		stats.mode.Add(1)
	}
}

// isStale enforces the global max_age_seconds guard before deduplication so old
// spots are dropped early and do not consume dedupe/window resources.
// Purpose: Enforce the global max_age_seconds guard.
// Key aspects: Drops old spots early to reduce work.
// Upstream: ingest pipelines and output stage.
// Downstream: time.Since and policy.MaxAgeSeconds.
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
// Purpose: Process deduplicated spots and distribute to ring buffer and outputs.
// Key aspects: Applies corrections, caching, licensing, secondary dedupe, and fan-out.
// Upstream: deduplicator output channel.
// Downstream: grid updates, telnet broadcast, archive writer, peer publish.
func processOutputSpots(
	deduplicator *dedup.Deduplicator,
	secondaryFast *dedup.SecondaryDeduper,
	secondaryMed *dedup.SecondaryDeduper,
	secondarySlow *dedup.SecondaryDeduper,
	archivePeerSecondaryMed *dedup.SecondaryDeduper,
	secondaryStage *atomic.Uint64,
	modeAssigner *spot.ModeAssigner,
	buf *buffer.RingBuffer,
	telnet *telnet.Server,
	peerManager *peer.Manager,
	tracker *stats.Tracker,
	correctionIdx *spot.CorrectionIndex,
	correctionCfg config.CallCorrectionConfig,
	ctyLookup func() *cty.CTYDatabase,
	metaCache *callMetaCache,
	harmonicDetector *spot.HarmonicDetector,
	harmonicCfg config.HarmonicConfig,
	knownCalls *atomic.Pointer[spot.KnownCallsigns],
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
	dash ui.Surface,
	gridUpdate func(call, grid string),
	gridLookup func(call string) (string, bool, bool),
	gridLookupSync func(call string) (string, bool, bool),
	unlicensedReporter func(source, role, call, mode string, freq float64),
	corrLogger spot.CorrectionTraceLogger,
	callCooldown *spot.CallCooldown,
	adaptiveMinReports *spot.AdaptiveMinReports,
	refresher *adaptiveRefresher,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore *spot.RecentBandStore,
	broadcastKeepSSID bool,
	archiveWriter *archive.Writer,
	lastOutput *atomic.Int64,
	pathPredictor *pathreliability.Predictor,
	pathReport *pathReportMetrics,
	allowedBands map[string]struct{},
) {
	outputChan := deduplicator.GetOutputChannel()
	secondaryActive := secondaryFast != nil || secondaryMed != nil || secondarySlow != nil
	stabilizerEnabled := telnet != nil && correctionCfg.Enabled && correctionCfg.StabilizerEnabled && recentBandStore != nil
	var telnetStabilizer *telnetSpotStabilizer
	if stabilizerEnabled {
		telnetStabilizer = newTelnetSpotStabilizer(
			time.Duration(correctionCfg.StabilizerDelaySeconds)*time.Second,
			correctionCfg.StabilizerMaxPending,
		)
		telnetStabilizer.Start()
		defer telnetStabilizer.Stop()
	}
	var familySuppressor *telnetFamilySuppressor
	if telnet != nil && correctionCfg.Enabled && correctionCfg.FamilyPolicy.TelnetSuppression.Enabled {
		// Output-only suppression state for less-specific call-family variants.
		// Archive/peer pipelines are unaffected by this map.
		familySuppressor = newTelnetFamilySuppressor(
			time.Duration(correctionCfg.FamilyPolicy.TelnetSuppression.WindowSeconds)*time.Second,
			correctionCfg.FamilyPolicy.TelnetSuppression.MaxEntries,
			spot.CorrectionFamilyPolicy{
				Configured:                 true,
				TruncationEnabled:          correctionCfg.FamilyPolicy.Truncation.Enabled,
				TruncationMaxLengthDelta:   correctionCfg.FamilyPolicy.Truncation.MaxLengthDelta,
				TruncationMinShorterLength: correctionCfg.FamilyPolicy.Truncation.MinShorterLength,
				TruncationAllowPrefix:      correctionCfg.FamilyPolicy.Truncation.AllowPrefixMatch,
				TruncationAllowSuffix:      correctionCfg.FamilyPolicy.Truncation.AllowSuffixMatch,
			},
			correctionCfg.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz,
		)
	}

	computeTelnetAllows := func(s *spot.Spot) (allowFast bool, allowMed bool, allowSlow bool) {
		allowFast = true
		if secondaryFast != nil {
			allowFast = secondaryFast.ShouldForward(s)
		}
		allowMed = true
		if secondaryMed != nil {
			allowMed = secondaryMed.ShouldForward(s)
		}
		allowSlow = true
		if secondarySlow != nil {
			allowSlow = secondarySlow.ShouldForward(s)
		}
		fallbackAllowed := allowFast
		if secondaryFast == nil {
			if secondaryMed != nil {
				fallbackAllowed = allowMed
			} else if secondarySlow != nil {
				fallbackAllowed = allowSlow
			}
		}
		if secondaryFast == nil {
			allowFast = fallbackAllowed
		}
		if secondaryMed == nil {
			allowMed = fallbackAllowed
		}
		if secondarySlow == nil {
			allowSlow = fallbackAllowed
		}
		return allowFast, allowMed, allowSlow
	}

	if stabilizerEnabled {
		go func() {
			releaseCh := telnetStabilizer.ReleaseChan()
			for delayed := range releaseCh {
				if delayed == nil {
					continue
				}
				ctyDB := ctyLookup()
				var knownCallset *spot.KnownCallsigns
				if knownCalls != nil {
					knownCallset = knownCalls.Load()
				}
				if suppress := maybeApplyCallCorrectionWithLogger(delayed, correctionIdx, correctionCfg, ctyDB, metaCache, tracker, dash, corrLogger, callCooldown, adaptiveMinReports, spotterReliability, spotterReliabilityCW, spotterReliabilityRTTY, confusionModel, recentBandStore, knownCallset); suppress {
					continue
				}
				applyKnownCallFloor(delayed, knownCalls, recentBandStore, correctionCfg)
				delayed.RefreshBeaconFlag()
				delayed.EnsureNormalized()
				if applyLicenseGate(delayed, ctyDB, metaCache, unlicensedReporter) {
					continue
				}
				stillRisky := shouldDelayTelnetByStabilizer(delayed, recentBandStore, correctionCfg, time.Now().UTC())
				if !shouldRecordRecentBandAfterStabilizerDelay(correctionCfg.StabilizerTimeoutAction, stillRisky) {
					if tracker != nil {
						tracker.IncrementStabilizerSuppressedTimeout()
					}
					continue
				}
				recordRecentBandObservation(delayed, recentBandStore, correctionCfg)
				allowFast, allowMed, allowSlow := computeTelnetAllows(delayed)
				if !allowFast && !allowMed && !allowSlow {
					telnet.DeliverSelfSpot(delayed)
					continue
				}
				if familySuppressor != nil && familySuppressor.ShouldSuppress(delayed, correctionCfg, time.Now().UTC()) {
					// Keep self visibility semantics consistent with secondary dedupe drops.
					telnet.DeliverSelfSpot(delayed)
					continue
				}
				if tracker != nil {
					tracker.IncrementStabilizerReleasedDelayed()
				}
				if lastOutput != nil {
					lastOutput.Store(time.Now().UTC().UnixNano())
				}
				telnet.BroadcastSpot(delayed, allowFast, allowMed, allowSlow)
			}
		}()
	}

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

			// CTY validation happens in the ingest gate before deduplication.

			s.RefreshBeaconFlag()

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
				var knownCallset *spot.KnownCallsigns
				if knownCalls != nil {
					knownCallset = knownCalls.Load()
				}
				suppress = maybeApplyCallCorrectionWithLogger(s, correctionIdx, correctionCfg, ctyDB, metaCache, tracker, dash, corrLogger, callCooldown, adaptiveMinReports, spotterReliability, spotterReliabilityCW, spotterReliabilityRTTY, confusionModel, recentBandStore, knownCallset)
				if suppress {
					return
				}
				// Call correction can change the DX call; recompute beacon flag accordingly.
				s.RefreshBeaconFlag()
				dirty = true
			}

			if !s.IsBeacon && harmonicDetector != nil && harmonicCfg.Enabled {
				if drop, fundamental, corroborators, deltaDB := harmonicDetector.ShouldDrop(s, time.Now().UTC()); drop {
					harmonicMsg := formatHarmonicSuppressedMessage(s.DXCall, s.Frequency, fundamental, corroborators, deltaDB)
					if tracker != nil {
						tracker.IncrementHarmonicSuppressions()
					}
					if dash != nil {
						dash.AppendHarmonic(harmonicMsg)
					} else {
						log.Println(harmonicMsg)
					}
					return
				}
			}

			if !s.IsBeacon && freqAvg != nil && shouldAverageFrequency(s) {
				window := frequencyAverageWindow(spotPolicy)
				tolerance := frequencyAverageTolerance(spotPolicy)
				dxCall := s.DXCallNorm
				if dxCall == "" {
					dxCall = s.DXCall
				}
				avg, corroborators, _ := freqAvg.Average(dxCall, s.Frequency, time.Now().UTC(), window, tolerance)
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

			// Ensure confidence-capable modes carry at least a placeholder glyph when no correction applied.
			if !s.IsBeacon {
				if modeSupportsConfidenceGlyph(modeUpper) && strings.TrimSpace(s.Confidence) == "" {
					s.Confidence = "?"
					dirty = true
				}
				if applyKnownCallFloor(s, knownCalls, recentBandStore, correctionCfg) {
					dirty = true
				}
			}

			if dirty {
				s.EnsureNormalized()
			}
			// Final license gate runs after corrections so busted calls can be fixed first.
			if applyLicenseGate(s, ctyDB, metaCache, unlicensedReporter) {
				return
			}
			// License gate refreshes metadata; normalize once more before stats/broadcast.
			if !dirty {
				// applyLicenseGate mutates metadata; mark dirty for final normalization.
				dirty = true
			}

			if dirty {
				s.EnsureNormalized()
			}

			if tracker != nil {
				modeKey := modeUpper
				if modeKey == "" {
					modeKey = string(s.SourceType)
				}
				tracker.IncrementMode(modeKey)

				sourceLabel := sourceStatsLabel(s)
				tracker.IncrementSource(sourceLabel)
				tracker.IncrementSourceMode(sourceLabel, modeKey)
			}

			if !broadcastKeepSSID {
				base := s.DECallNorm
				if base == "" {
					base = s.DECall
				}
				stripped := collapseSSIDForBroadcast(base)
				s.DECallStripped = stripped
				s.DECallNormStripped = stripped
			}

			// Keep test spots out of the ring buffer so SHOW DX history stays production-only.
			if buf != nil && shouldBufferSpot(s) {
				buf.Add(s)
			}

			// Ensure DE metadata is populated before secondary dedupe. Upstream CTY lookups
			// can be bypassed when spotters carry SSID tokens or CTY is missing; refresh
			// here so secondary dedupe has DXCC/zone available.
			if secondaryActive && (s.DEMetadata.ADIF <= 0 || s.DEMetadata.CQZone <= 0) && ctyDB != nil {
				call := s.DECallNorm
				if call == "" {
					call = s.DECall
				}
				call = normalizeCallForMetadata(call)
				if info := effectivePrefixInfo(ctyDB, metaCache, call); info != nil {
					deGrid := strings.TrimSpace(s.DEMetadata.Grid)
					deGridDerived := s.DEMetadata.GridDerived
					s.DEMetadata = metadataFromPrefix(info)
					if deGrid != "" {
						s.DEMetadata.Grid = deGrid
						s.DEMetadata.GridDerived = deGridDerived
					}
					// Metadata refresh can change continent/grid; clear cached norms and rebuild.
					s.InvalidateMetadataCache()
					s.EnsureNormalized()
				}
			}

			// Final fan-out guards (symmetry with peer belt-and-suspenders): do not
			// deliver stale spots to any downstream sink, even if an upstream stage
			// failed to drop them.
			if isStale(s, spotPolicy) {
				return
			}

			// Backfill grids before secondary dedupe so path reliability can use them.
			if gridLookup != nil || gridLookupSync != nil {
				gridBackfilled := false
				dxCall := s.DXCallNorm
				if dxCall == "" {
					dxCall = s.DXCall
				}
				if strings.TrimSpace(s.DXMetadata.Grid) == "" {
					if grid, derived, ok := lookupGridUnified(dxCall, gridLookupSync, gridLookup); ok {
						s.DXMetadata.Grid = grid
						s.DXMetadata.GridDerived = derived
						gridBackfilled = true
					}
				}
				deCall := s.DECallNorm
				if deCall == "" {
					deCall = s.DECall
				}
				if strings.TrimSpace(s.DEMetadata.Grid) == "" {
					if grid, derived, ok := lookupGridUnified(deCall, gridLookupSync, gridLookup); ok {
						s.DEMetadata.Grid = grid
						s.DEMetadata.GridDerived = derived
						gridBackfilled = true
					}
				}
				if gridBackfilled {
					s.InvalidateMetadataCache()
					s.EnsureNormalized()
				}
			}

			if pathPredictor != nil && pathPredictor.Config().Enabled {
				// Populate cached cells even when we skip updates so broadcast can reuse them.
				if s.DXCellID == 0 {
					s.DXCellID = uint16(pathreliability.EncodeCell(strings.TrimSpace(s.DXMetadata.Grid)))
				}
				if s.DECellID == 0 {
					s.DECellID = uint16(pathreliability.EncodeCell(strings.TrimSpace(s.DEMetadata.Grid)))
				}
				if s.HasReport {
					mode := s.ModeNorm
					if strings.TrimSpace(mode) == "" {
						mode = s.Mode
					}
					if ft8, ok := pathreliability.FT8Equivalent(mode, s.Report, pathPredictor.Config()); ok {
						dxCell := pathreliability.CellID(s.DXCellID)
						deCell := pathreliability.CellID(s.DECellID)
						dxCoarse := pathreliability.EncodeCoarseCell(s.DXMetadata.Grid)
						deCoarse := pathreliability.EncodeCoarseCell(s.DEMetadata.Grid)
						band := s.BandNorm
						if strings.TrimSpace(band) == "" {
							band = s.Band
						}
						band = strings.TrimSpace(spot.NormalizeBand(band))
						if band == "" || band == "???" {
							band = strings.TrimSpace(spot.NormalizeBand(spot.FreqToBand(s.Frequency)))
						}
						if band != "" && band != "???" && allowedBand(allowedBands, band) {
							spotTime := s.Time.UTC()
							if spotTime.IsZero() {
								spotTime = time.Now().UTC()
							}
							bucket := pathreliability.BucketForIngest(mode)
							if bucket != pathreliability.BucketNone {
								if pathReport != nil {
									pathReport.Observe(s, spotTime)
								}
								// Spot SNR reflects DX -> DE (spotter is the receiver).
								pathPredictor.Update(bucket, deCell, dxCell, deCoarse, dxCoarse, band, ft8, 1.0, spotTime, s.IsBeacon)
							}
						}
					}
				}
			}

			if secondaryStage != nil {
				secondaryStage.Add(1)
			}
			archivePeerAllowMed := true
			if archivePeerSecondaryMed != nil {
				archivePeerAllowMed = archivePeerSecondaryMed.ShouldForward(s)
			}

			allowFast, allowMed, allowSlow := true, true, true
			telnetDeliverNow := telnet != nil
			delayedQueued := false
			if !stabilizerEnabled {
				allowFast, allowMed, allowSlow = computeTelnetAllows(s)
				archivePeerAllowMed = allowMed
				recordRecentBandObservation(s, recentBandStore, correctionCfg)
				// Broadcast-only dedupe: ring/history already updated above for non-test spots.
				if !allowFast && !allowMed && !allowSlow {
					telnet.DeliverSelfSpot(s)
					return
				}
			} else {
				telnetDeliverNow = false
				if shouldDelayTelnetByStabilizer(s, recentBandStore, correctionCfg, time.Now().UTC()) {
					delayed := cloneSpotForTelnetStabilizer(s)
					if delayed != nil && telnetStabilizer.Enqueue(delayed) {
						delayedQueued = true
						if tracker != nil {
							tracker.IncrementStabilizerHeld()
						}
					} else {
						allowFast, allowMed, allowSlow = computeTelnetAllows(s)
						telnetDeliverNow = true
						if tracker != nil {
							tracker.IncrementStabilizerOverflowRelease()
							tracker.IncrementStabilizerReleasedImmediate()
						}
					}
				} else {
					allowFast, allowMed, allowSlow = computeTelnetAllows(s)
					telnetDeliverNow = true
					if tracker != nil {
						tracker.IncrementStabilizerReleasedImmediate()
					}
				}
				if shouldRecordRecentBandInMainLoop(stabilizerEnabled, delayedQueued) {
					recordRecentBandObservation(s, recentBandStore, correctionCfg)
				}
				if telnetDeliverNow && !allowFast && !allowMed && !allowSlow {
					telnet.DeliverSelfSpot(s)
					telnetDeliverNow = false
				}
				if telnetDeliverNow && familySuppressor != nil && familySuppressor.ShouldSuppress(s, correctionCfg, time.Now().UTC()) {
					// Keep self visibility semantics consistent with secondary dedupe drops.
					telnet.DeliverSelfSpot(s)
					telnetDeliverNow = false
				}
			}

			if gridUpdate != nil {
				if dxGrid := strings.TrimSpace(s.DXMetadata.Grid); dxGrid != "" && !s.DXMetadata.GridDerived {
					dxCall := s.DXCallNorm
					if dxCall == "" {
						dxCall = s.DXCall
					}
					gridUpdate(dxCall, dxGrid)
				}
				if deGrid := strings.TrimSpace(s.DEMetadata.Grid); deGrid != "" && !s.DEMetadata.GridDerived {
					deCall := s.DECallNorm
					if deCall == "" {
						deCall = s.DECall
					}
					gridUpdate(deCall, deGrid)
				}
			}

			emittedNow := false
			if archiveWriter != nil && archivePeerAllowMed && shouldArchiveSpot(s) {
				archiveWriter.Enqueue(s)
				emittedNow = true
			}
			if telnetDeliverNow {
				telnet.BroadcastSpot(s, allowFast, allowMed, allowSlow)
				emittedNow = true
			}
			if peerManager != nil && archivePeerAllowMed && shouldPublishToPeers(s) {
				peerSpot := cloneSpotForPeerPublish(s)
				peerManager.PublishDX(peerSpot)
				emittedNow = true
			}
			if emittedNow && lastOutput != nil {
				lastOutput.Store(time.Now().UTC().UnixNano())
			}
		}()
	}
}

func cloneSpotForTelnetStabilizer(s *spot.Spot) *spot.Spot {
	if s == nil {
		return nil
	}
	clone := s.CloneWithComment(s.Comment)
	clone.InvalidateMetadataCache()
	clone.EnsureNormalized()
	return clone
}

// shouldDelayTelnetByStabilizer reports whether a spot should be held before
// telnet broadcast. Risk is "not heard recently on same band+mode" for
// correction-eligible, non-beacon spots, except P-confidence spots which are
// treated as sufficiently corroborated for immediate pass-through.
func shouldDelayTelnetByStabilizer(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig, now time.Time) bool {
	if s == nil || store == nil || !cfg.StabilizerEnabled || s.IsBeacon {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(s.Confidence), "P") {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if strings.TrimSpace(call) == "" {
		return false
	}
	band := s.BandNorm
	if strings.TrimSpace(band) == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	minUnique := cfg.RecentBandRecordMinUniqueSpotters
	if minUnique <= 0 {
		minUnique = 2
	}
	return !store.HasRecentSupport(call, band, mode, minUnique, now)
}

// shouldRecordRecentBandInMainLoop controls recent-on-band admission timing for
// the main output path. Delayed spots must only contribute after delay
// resolution in the stabilizer release path.
func shouldRecordRecentBandInMainLoop(stabilizerEnabled bool, delayedQueued bool) bool {
	return !stabilizerEnabled || !delayedQueued
}

// shouldRecordRecentBandAfterStabilizerDelay controls recent-on-band admission
// for delayed spots. If a delayed spot is still risky and timeout action is
// suppress, it must not reinforce recent-on-band support.
func shouldRecordRecentBandAfterStabilizerDelay(timeoutAction string, stillRisky bool) bool {
	if !stillRisky {
		return true
	}
	return !strings.EqualFold(strings.TrimSpace(timeoutAction), stabilizerTimeoutSuppress)
}

// Purpose: Gate ring-buffer storage for test spotters.
// Key aspects: Test spots are excluded so SHOW DX stays production-only.
// Upstream: processOutputSpots.
// Downstream: ring buffer Add.
func shouldBufferSpot(s *spot.Spot) bool {
	return s != nil && !s.IsTestSpotter
}

// Purpose: Gate archive persistence for test spotters.
// Key aspects: Test spots are excluded from Pebble history.
// Upstream: processOutputSpots.
// Downstream: archive.Writer.Enqueue.
func shouldArchiveSpot(s *spot.Spot) bool {
	return s != nil && !s.IsTestSpotter
}

// Purpose: Decide whether a spot should be forwarded to peers.
// Key aspects: Excludes upstream/peer sources and test spotters.
// Upstream: processOutputSpots.
// Downstream: peer.Manager.PublishDX.
func shouldPublishToPeers(s *spot.Spot) bool {
	if s == nil || s.IsTestSpotter {
		return false
	}
	switch s.SourceType {
	case spot.SourceUpstream, spot.SourcePeer:
		return false
	default:
		return true
	}
}

// startPipelineHealthMonitor logs warnings when the output pipeline or dedup
// goroutine appear stalled. It is intentionally lightweight and non-blocking.
// Purpose: Warn when dedup/output pipelines appear stalled.
// Key aspects: Periodic ticker checks without blocking hot paths.
// Upstream: main startup after pipeline wiring.
// Downstream: log.Printf, dedup.LastProcessedAt, peerManager.ReconnectCount.
func startPipelineHealthMonitor(ctx context.Context, dedup *dedup.Deduplicator, lastOutput *atomic.Int64, peerManager *peer.Manager) {
	const (
		checkInterval      = 30 * time.Second
		outputStallWarning = 2 * time.Minute
	)
	ticker := time.NewTicker(checkInterval)
	// Purpose: Periodically check for stalled output and dedup activity.
	// Key aspects: Exits on context cancellation and emits warnings.
	// Upstream: startPipelineHealthMonitor.
	// Downstream: ticker.Stop and log.Printf.
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

func startPathPredictionLogger(ctx context.Context, logMux *logFanout, srv *telnet.Server, predictor *pathreliability.Predictor, pathReport *pathReportMetrics) {
	// Purpose: Periodically report path prediction outcomes, bucket counts, and weight histograms.
	// Key aspects: Uses atomic snapshot/reset; exits on context cancellation.
	// Upstream: main startup.
	// Downstream: telnet.Server.PathPredictionStatsSnapshot, pathreliability.Predictor stats/histograms.
	if srv == nil && predictor == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	type ge10Sample struct {
		ts  time.Time
		val int
	}
	ge10Window := make(map[string][]ge10Sample)
	const ge10WindowDuration = time.Hour

	pruneGe10 := func(now time.Time, band string) {
		samples := ge10Window[band]
		if len(samples) == 0 {
			return
		}
		cutoff := now.Add(-ge10WindowDuration)
		n := 0
		for _, s := range samples {
			if s.ts.After(cutoff) || s.ts.Equal(cutoff) {
				samples[n] = s
				n++
			}
		}
		if n == 0 {
			delete(ge10Window, band)
			return
		}
		ge10Window[band] = samples[:n]
	}

	ge10Stats := func(band string) (min, med, p75, max int, ok bool) {
		samples := ge10Window[band]
		if len(samples) == 0 {
			return 0, 0, 0, 0, false
		}
		vals := make([]int, 0, len(samples))
		for _, s := range samples {
			vals = append(vals, s.val)
		}
		sort.Ints(vals)
		min = vals[0]
		max = vals[len(vals)-1]
		mid := len(vals) / 2
		if len(vals)%2 == 1 {
			med = vals[mid]
		} else {
			med = int(math.Round(float64(vals[mid-1]+vals[mid]) / 2))
		}
		if len(vals) == 1 {
			p75 = vals[0]
		} else {
			pos := int(math.Round(0.75 * float64(len(vals)-1)))
			if pos < 0 {
				pos = 0
			}
			if pos >= len(vals) {
				pos = len(vals) - 1
			}
			p75 = vals[pos]
		}
		return min, med, p75, max, true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			fileOnly := func(line string) {
				if logMux == nil {
					return
				}
				logMux.WriteFileOnlyLine(line, now)
			}
			if srv != nil {
				stats := srv.PathPredictionStatsSnapshot()
				if stats.Total > 0 {
					fileOnly(fmt.Sprintf(
						"Path predictions (5m): total=%s derived=%s combined=%s insufficient=%s no_sample=%s low_weight=%s override_r=%s override_g=%s",
						humanize.Comma(int64(stats.Total)),
						humanize.Comma(int64(stats.Derived)),
						humanize.Comma(int64(stats.Combined)),
						humanize.Comma(int64(stats.Insufficient)),
						humanize.Comma(int64(stats.NoSample)),
						humanize.Comma(int64(stats.LowWeight)),
						humanize.Comma(int64(stats.OverrideR)),
						humanize.Comma(int64(stats.OverrideG)),
					))
				}
			}
			if pathReport != nil {
				sourceCounts := pathReport.SnapshotSources()
				if len(sourceCounts) > 0 {
					total := uint64(0)
					for _, v := range sourceCounts {
						total += v
					}
					if total > 0 {
						var s strings.Builder
						s.WriteString("Path source mix (5m):")
						fmt.Fprintf(&s, " total=%s", humanize.Comma(int64(total)))
						for _, label := range []string{"RBN", "RBN-FT", "PSK", "HUMAN", "PEER", "UPSTREAM", "OTHER"} {
							if val, ok := sourceCounts[label]; ok {
								fmt.Fprintf(&s, " %s=%s", label, humanize.Comma(int64(val)))
							} else {
								fmt.Fprintf(&s, " %s=0", label)
							}
						}
						fileOnly(s.String())
					}
				}
			}
			if predictor == nil || !predictor.Config().Enabled {
				continue
			}
			bandStats := predictor.StatsByBand(now)
			if len(bandStats) == 0 {
				continue
			}
			var b strings.Builder
			b.WriteString("Path buckets (5m):")
			for _, entry := range bandStats {
				fmt.Fprintf(&b, " %s f=%s c=%s",
					entry.Band,
					humanize.Comma(int64(entry.Fine)),
					humanize.Comma(int64(entry.Coarse)))
			}
			fileOnly(b.String())
			hist := predictor.WeightHistogramByBand(now)
			if len(hist.Bands) == 0 || len(hist.Edges) == 0 {
				continue
			}
			var h strings.Builder
			h.WriteString("Path weight dist (5m):")
			for _, entry := range hist.Bands {
				if entry.Total == 0 {
					continue
				}
				fmt.Fprintf(&h, " %s t=%s", entry.Band, humanize.Comma(int64(entry.Total)))
				for i, count := range entry.Bins {
					label := weightBinLabel(i, hist.Edges)
					fmt.Fprintf(&h, " %s=%s", label, humanize.Comma(int64(count)))
				}
			}
			if h.Len() > len("Path weight dist (5m):") {
				fileOnly(h.String())
			}
			if len(hist.Bands) > 0 {
				var v strings.Builder
				v.WriteString("Path ge10 variance (5m):")
				for _, entry := range hist.Bands {
					band := entry.Band
					if band == "" {
						continue
					}
					ge10 := 0
					if len(entry.Bins) > 0 {
						ge10 = entry.Bins[len(entry.Bins)-1]
					}
					ge10Window[band] = append(ge10Window[band], ge10Sample{ts: now, val: ge10})
					pruneGe10(now, band)
					min, med, p75, max, ok := ge10Stats(band)
					if !ok {
						continue
					}
					deg := 0
					if max == 0 {
						deg = 1
					}
					fmt.Fprintf(&v, " %s min=%d med=%d p75=%d max=%d deg=%d", band, min, med, p75, max, deg)
				}
				if v.Len() > len("Path ge10 variance (5m):") {
					fileOnly(v.String())
				}
			}
			if pathReport != nil {
				hourKey, spotters, gridPairs := pathReport.HourlyCounts(now)
				bandOrder := make([]string, 0, len(bandStats))
				for _, entry := range bandStats {
					bandOrder = append(bandOrder, entry.Band)
				}
				if len(bandOrder) == 0 {
					for band := range spotters {
						bandOrder = append(bandOrder, band)
					}
					sort.Strings(bandOrder)
				}
				if len(spotters) > 0 {
					var u strings.Builder
					u.WriteString("Path unique spotters (hour):")
					if hourKey != "" {
						fmt.Fprintf(&u, " hour=%s", hourKey[len(hourKey)-2:]) // HH
					}
					for _, band := range bandOrder {
						fmt.Fprintf(&u, " %s=%s", band, humanize.Comma(int64(spotters[band])))
					}
					fileOnly(u.String())
				}
				if len(gridPairs) > 0 {
					var g strings.Builder
					g.WriteString("Path unique grid pairs (hour):")
					if hourKey != "" {
						fmt.Fprintf(&g, " hour=%s", hourKey[len(hourKey)-2:])
					}
					for _, band := range bandOrder {
						fmt.Fprintf(&g, " %s=%s", band, humanize.Comma(int64(gridPairs[band])))
					}
					fileOnly(g.String())
				}
			}
		}
	}
}

func weightBinLabel(idx int, edges []float64) string {
	if len(edges) == 0 {
		return fmt.Sprintf("b%d", idx)
	}
	if idx <= 0 {
		return fmt.Sprintf("<%g", edges[0])
	}
	if idx < len(edges) {
		return fmt.Sprintf("%g-%g", edges[idx-1], edges[idx])
	}
	return fmt.Sprintf(">=%g", edges[len(edges)-1])
}

// collapseSSIDForBroadcast trims SSID fragments so clients see a single
// skimmer identity (e.g., N2WQ-1-# -> N2WQ-#, N2WQ-1 -> N2WQ).
// It preserves non-numeric suffixes.
// Purpose: Normalize spotter SSIDs before telnet broadcast.
// Key aspects: Collapses numeric suffixes while preserving non-numeric tokens.
// Upstream: processOutputSpots.
// Downstream: stripNumericSSID.
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

// Purpose: Remove a numeric SSID suffix (e.g., "-1") from a callsign.
// Key aspects: Leaves non-numeric suffixes intact.
// Upstream: collapseSSIDForBroadcast.
// Downstream: strings.LastIndexByte.
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

// stripTrailingHyphenSuffix removes any trailing hyphen suffix after the last slash.
// It preserves portable segments (e.g., "K1ABC-1/P") by only trimming when the
// suffix is the final segment.
func stripTrailingHyphenSuffix(call string) string {
	slash := strings.LastIndexByte(call, '/')
	start := 0
	if slash >= 0 {
		start = slash + 1
	}
	idx := strings.IndexByte(call[start:], '-')
	if idx < 0 {
		return call
	}
	trimAt := start + idx
	if trimAt <= 0 {
		return call
	}
	return call[:trimAt]
}

// normalizeCallForMetadata strips skimmer and hyphen suffixes before metadata lookups.
// It preserves portable segments (e.g., "/P") and does not mutate canonical calls.
func normalizeCallForMetadata(call string) string {
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" {
		return call
	}
	return stripTrailingHyphenSuffix(call)
}

// Purpose: Run the shared grid backfill routine (sync first, async fallback).
// Key aspects: Uses the same bounded sync routine for all callers to keep behavior consistent.
// Upstream: processOutputSpots grid backfill path.
// Downstream: gridLookupSync and gridLookup helpers.
func lookupGridUnified(call string, gridLookupSync func(string) (string, bool, bool), gridLookup func(string) (string, bool, bool)) (string, bool, bool) {
	if gridLookupSync != nil {
		if grid, derived, ok := gridLookupSync(call); ok {
			return grid, derived, true
		}
	}
	if gridLookup != nil {
		return gridLookup(call)
	}
	return "", false, false
}

// cloneSpotForPeerPublish ensures manual spots carry an inferred mode to peers
// even when the user omitted a comment. Peers only see the comment field in
// PC61/PC11 frames, so we fall back to the inferred mode when the comment is
// blank. Other sources and spots with comments are passed through as-is.
func cloneSpotForPeerPublish(src *spot.Spot) *spot.Spot {
	if src == nil {
		return nil
	}
	if src.SourceType != spot.SourceManual {
		return src
	}
	if strings.TrimSpace(src.Comment) != "" {
		return src
	}
	mode := strings.TrimSpace(src.Mode)
	if mode == "" {
		return src
	}
	clone := &spot.Spot{
		ID:                 src.ID,
		DXCall:             src.DXCall,
		DECall:             src.DECall,
		Frequency:          src.Frequency,
		Band:               src.Band,
		Mode:               src.Mode,
		Report:             src.Report,
		HasReport:          src.HasReport,
		Time:               src.Time,
		Comment:            src.Comment,
		SourceType:         src.SourceType,
		SourceNode:         src.SourceNode,
		SpotterIP:          src.SpotterIP,
		TTL:                src.TTL,
		IsHuman:            src.IsHuman,
		IsTestSpotter:      src.IsTestSpotter,
		IsBeacon:           src.IsBeacon,
		DXMetadata:         src.DXMetadata,
		DEMetadata:         src.DEMetadata,
		Confidence:         src.Confidence,
		ModeNorm:           src.ModeNorm,
		BandNorm:           src.BandNorm,
		DXCallNorm:         src.DXCallNorm,
		DECallNorm:         src.DECallNorm,
		DXContinentNorm:    src.DXContinentNorm,
		DEContinentNorm:    src.DEContinentNorm,
		DXGridNorm:         src.DXGridNorm,
		DEGridNorm:         src.DEGridNorm,
		DXGrid2:            src.DXGrid2,
		DEGrid2:            src.DEGrid2,
		DECallStripped:     src.DECallStripped,
		DECallNormStripped: src.DECallNormStripped,
	}
	clone.Comment = mode
	return clone
}

// applyLicenseGate runs the FCC license check after all corrections and returns true when the spot should be dropped.
// Purpose: Enforce FCC ULS licensing gates for US base calls (DX only; DE checked at ingest).
// Key aspects: Jurisdiction is derived from the normalized base call; reporter callback on drops.
// Upstream: processOutputSpots before broadcast.
// Downstream: uls.IsLicensedUS, reporter.
func applyLicenseGate(s *spot.Spot, ctyDB *cty.CTYDatabase, metaCache *callMetaCache, reporter func(source, role, call, mode string, freq float64)) bool {
	if s == nil {
		return false
	}
	if s.IsBeacon {
		return false
	}
	if ctyDB == nil {
		return false
	}

	dxCall := s.DXCallNorm
	if dxCall == "" {
		dxCall = s.DXCall
	}
	deCall := s.DECallNorm
	if deCall == "" {
		deCall = s.DECall
	}
	dxLookupCall := normalizeCallForMetadata(dxCall)
	deLookupCall := normalizeCallForMetadata(deCall)
	needsMetadata := s.DXMetadata.ADIF == 0 || s.DEMetadata.ADIF == 0 || s.Confidence == "C"
	if needsMetadata {
		dxInfo := effectivePrefixInfo(ctyDB, metaCache, dxLookupCall)
		deInfo := effectivePrefixInfo(ctyDB, metaCache, deLookupCall)

		// Refresh metadata from the final CTY match but preserve any grid data we already attached.
		dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
		deGrid := strings.TrimSpace(s.DEMetadata.Grid)
		dxGridDerived := s.DXMetadata.GridDerived
		deGridDerived := s.DEMetadata.GridDerived
		s.DXMetadata = metadataFromPrefix(dxInfo)
		s.DEMetadata = metadataFromPrefix(deInfo)
		if dxGrid != "" {
			s.DXMetadata.Grid = dxGrid
			s.DXMetadata.GridDerived = dxGridDerived
		}
		if deGrid != "" {
			s.DEMetadata.Grid = deGrid
			s.DEMetadata.GridDerived = deGridDerived
		}
		// Metadata refresh can change continent/grid; clear cached norms and rebuild.
		s.InvalidateMetadataCache()
		s.EnsureNormalized()
	}

	// License checks use the base callsign (portable segment order-independent) so
	// location prefixes like /VE3 still map to the operator's home license.
	dxLicenseCall := strings.TrimSpace(uls.NormalizeForLicense(dxCall))
	var dxLicenseInfo *cty.PrefixInfo
	if dxLicenseCall != "" {
		dxLicenseInfo = effectivePrefixInfo(ctyDB, metaCache, dxLicenseCall)
	}

	if dxLicenseInfo != nil && dxLicenseInfo.ADIF == 291 {
		callKey := dxLicenseCall
		if callKey == "" {
			callKey = dxCall
		}
		if uls.AllowlistMatch(dxLicenseInfo.ADIF, callKey) {
			return false
		}
		if !uls.IsLicensedUS(callKey) {
			if reporter != nil {
				reporter(s.SourceNode, "DX", callKey, s.ModeNorm, s.Frequency)
			}
			return true
		}
	}
	return false
}

// Purpose: Resolve prefix metadata for a callsign using cache + CTY database.
// Key aspects: Prefers portable slash prefixes (location) over base calls.
// Upstream: processOutputSpots DE metadata refresh and corrections.
// Downstream: callMetaCache.LookupCTY or cty.LookupCallsignPortable.
func effectivePrefixInfo(ctyDB *cty.CTYDatabase, metaCache *callMetaCache, call string) *cty.PrefixInfo {
	if ctyDB == nil {
		return nil
	}
	if call == "" {
		return nil
	}
	if shouldRejectCTYCall(call) {
		return nil
	}
	if metaCache != nil {
		if info, ok, _ := metaCache.LookupCTY(call, ctyDB); ok {
			return info
		}
		return nil
	}
	info, ok := ctyDB.LookupCallsignPortable(call)
	if !ok {
		return nil
	}
	return info
}

// Purpose: Convert CTY prefix info into spot.CallMetadata.
// Key aspects: Copies continent/country/zone fields into a struct.
// Upstream: effectivePrefixInfo consumers.
// Downstream: None (pure mapping).
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

// Purpose: Apply call correction and optionally log decision details.
// Key aspects: Evaluates corrections, updates stats, and can suppress spots.
// Upstream: processOutputSpots call correction stage.
// Downstream: spot.ApplyCallCorrection, traceLogger, tracker updates.
func maybeApplyCallCorrectionWithLogger(spotEntry *spot.Spot, idx *spot.CorrectionIndex, cfg config.CallCorrectionConfig, ctyDB *cty.CTYDatabase, metaCache *callMetaCache, tracker *stats.Tracker, dash ui.Surface, traceLogger spot.CorrectionTraceLogger, cooldown *spot.CallCooldown, adaptive *spot.AdaptiveMinReports, spotterReliability spot.SpotterReliability, spotterReliabilityCW spot.SpotterReliability, spotterReliabilityRTTY spot.SpotterReliability, confusionModel *spot.ConfusionModel, recentBandStore *spot.RecentBandStore, knownCallset *spot.KnownCallsigns) bool {
	if spotEntry == nil {
		return false
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		// Leave any pre-seeded confidence intact for non-correction modes.
		return false
	}
	if idx == nil || !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		return false
	}

	now := time.Now().UTC()
	window := callCorrectionWindowForMode(cfg, spotEntry.Mode)
	defer idx.Add(spotEntry, now, window)

	modeUpper := strings.ToUpper(strings.TrimSpace(spotEntry.Mode))
	// Voice signals are wider, so use sideband-specific correction windows.
	isVoice := modeUpper == "USB" || modeUpper == "LSB"

	if adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") {
		reporter := spotEntry.DECallNorm
		if reporter == "" {
			reporter = spotEntry.DECall
		}
		adaptive.Observe(spotEntry.Band, reporter, now)
	}

	minReports := cfg.MinConsensusReports
	cooldownMinReports := cfg.CooldownMinReporters
	if adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") {
		if dyn := adaptive.MinReportsForBand(spotEntry.Band, now); dyn > 0 {
			minReports = dyn
			cooldownMinReports = dyn
		}
	}
	state := "normal"
	if adaptive != nil {
		state = adaptive.StateForBand(spotEntry.Band, now)
	}
	qualityBinHz := cfg.QualityBinHz
	freqToleranceHz := cfg.FrequencyToleranceHz
	if isVoice {
		freqToleranceHz = cfg.VoiceFrequencyToleranceHz
	} else if params, ok := resolveBandStateParams(cfg.BandStateOverrides, spotEntry.Band, state); ok {
		if params.QualityBinHz > 0 {
			qualityBinHz = params.QualityBinHz
		}
		if params.FrequencyToleranceHz > 0 {
			freqToleranceHz = params.FrequencyToleranceHz
		}
	}

	settings := buildCorrectionSettings(
		cfg,
		minReports,
		cooldownMinReports,
		window,
		freqToleranceHz,
		qualityBinHz,
		traceLogger,
		cooldown,
		spotterReliability,
		spotterReliabilityCW,
		spotterReliabilityRTTY,
		confusionModel,
		recentBandStore,
		knownCallset,
		func(tr spot.CorrectionTrace) {
			if tracker == nil {
				return
			}
			tracker.ObserveCallCorrectionDecision(tr.DecisionPath, tr.Decision, tr.Reason, tr.CandidateRank, tr.PriorBonusApplied)
		},
	)
	candidateWindowKHz := freqToleranceHz / 1000.0
	if candidateWindowKHz <= 0 {
		candidateWindowKHz = 0.5
	}
	if isVoice && cfg.VoiceCandidateWindowKHz > 0 {
		candidateWindowKHz = cfg.VoiceCandidateWindowKHz
	}
	others := idx.Candidates(spotEntry, now, window, candidateWindowKHz)
	entries := spotsToEntries(others)
	corrected, supporters, correctedConfidence, subjectConfidence, totalReporters, ok := spot.SuggestCallCorrection(spotEntry, entries, settings, now)

	spotEntry.Confidence = formatConfidence(subjectConfidence, totalReporters)

	if !ok {
		return false
	}

	message := formatCallCorrectedMessage(spotEntry.DXCall, corrected, spotEntry.Frequency, supporters, correctedConfidence)

	correctedNorm := spot.NormalizeCallsign(corrected)
	if shouldRejectCTYCall(correctedNorm) {
		log.Printf("Call correction rejected (invalid base call): suggested %s at %.1f kHz", corrected, spotEntry.Frequency)
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			log.Printf("Call correction suppression engaged: dropping spot from %s at %.1f kHz", spotEntry.DXCall, spotEntry.Frequency)
			return true
		}
		spotEntry.Confidence = "B"
		return false
	}

	if ctyDB != nil {
		if info := effectivePrefixInfo(ctyDB, metaCache, correctedNorm); info != nil {
			if dash != nil {
				dash.AppendCall(message)
			} else {
				log.Println(message)
			}
			spotEntry.DXCall = correctedNorm
			spotEntry.DXCallNorm = correctedNorm
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
		dash.AppendCall(message)
	} else {
		log.Println(message)
	}
	spotEntry.DXCall = correctedNorm
	spotEntry.DXCallNorm = correctedNorm
	spotEntry.Confidence = "C"
	if tracker != nil {
		tracker.IncrementCallCorrections()
	}

	return false
}

// Purpose: Compute the time window for call correction recency.
// Key aspects: Uses config recency defaults and overrides.
// Upstream: main correctionIndex cleanup scheduling.
// Downstream: time.Duration math.
func callCorrectionWindowForMode(cfg config.CallCorrectionConfig, mode string) time.Duration {
	baseSeconds := cfg.RecencySeconds
	if baseSeconds <= 0 {
		baseSeconds = 45
	}
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "CW":
		if cfg.RecencySecondsCW > 0 {
			baseSeconds = cfg.RecencySecondsCW
		}
	case "RTTY":
		if cfg.RecencySecondsRTTY > 0 {
			baseSeconds = cfg.RecencySecondsRTTY
		}
	}
	return time.Duration(baseSeconds) * time.Second
}

func callCorrectionCleanupWindow(cfg config.CallCorrectionConfig) time.Duration {
	maxSeconds := cfg.RecencySeconds
	if maxSeconds <= 0 {
		maxSeconds = 45
	}
	if cfg.RecencySecondsCW > maxSeconds {
		maxSeconds = cfg.RecencySecondsCW
	}
	if cfg.RecencySecondsRTTY > maxSeconds {
		maxSeconds = cfg.RecencySecondsRTTY
	}
	return time.Duration(maxSeconds) * time.Second
}

func buildCorrectionSettings(
	cfg config.CallCorrectionConfig,
	minReports int,
	cooldownMinReports int,
	window time.Duration,
	freqToleranceHz float64,
	qualityBinHz int,
	traceLogger spot.CorrectionTraceLogger,
	cooldown *spot.CallCooldown,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore *spot.RecentBandStore,
	knownCallset *spot.KnownCallsigns,
	decisionObserver spot.CorrectionDecisionObserver,
) spot.CorrectionSettings {
	return spot.CorrectionSettings{
		MinConsensusReports: minReports,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
		SlashPrecedenceMinReports: cfg.FamilyPolicy.SlashPrecedenceMinReports,
		CandidateEvalTopK:         cfg.CandidateEvalTopK,
		MinAdvantage:              cfg.MinAdvantage,
		TruncationAdvantagePolicy: spot.CorrectionTruncationAdvantagePolicy{
			Configured:                true,
			Enabled:                   cfg.FamilyPolicy.Truncation.RelaxAdvantage.Enabled,
			MinAdvantage:              cfg.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage,
			RequireCandidateValidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated,
			RequireSubjectUnvalidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated,
		},
		MinConfidencePercent:              cfg.MinConfidencePercent,
		MaxEditDistance:                   cfg.MaxEditDistance,
		RecencyWindow:                     window,
		Strategy:                          cfg.Strategy,
		MinSNRCW:                          cfg.MinSNRCW,
		MinSNRRTTY:                        cfg.MinSNRRTTY,
		MinSNRVoice:                       cfg.MinSNRVoice,
		DistanceModelCW:                   cfg.DistanceModelCW,
		DistanceModelRTTY:                 cfg.DistanceModelRTTY,
		Distance3ExtraReports:             cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:           cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence:          cfg.Distance3ExtraConfidence,
		DebugLog:                          cfg.DebugLog,
		TraceLogger:                       traceLogger,
		FreqGuardMinSeparationKHz:         cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:            cfg.FreqGuardRunnerUpRatio,
		FrequencyToleranceHz:              freqToleranceHz,
		QualityBinHz:                      qualityBinHz,
		QualityGoodThreshold:              cfg.QualityGoodThreshold,
		QualityNewCallIncrement:           cfg.QualityNewCallIncrement,
		QualityBustedDecrement:            cfg.QualityBustedDecrement,
		SpotterReliability:                spotterReliability,
		SpotterReliabilityCW:              spotterReliabilityCW,
		SpotterReliabilityRTTY:            spotterReliabilityRTTY,
		MinSpotterReliability:             cfg.MinSpotterReliability,
		ConfusionModel:                    confusionModel,
		ConfusionWeight:                   cfg.ConfusionModelWeight,
		RecentBandBonusEnabled:            cfg.RecentBandBonusEnabled,
		RecentBandWindow:                  time.Duration(cfg.RecentBandWindowSeconds) * time.Second,
		RecentBandBonusMax:                cfg.RecentBandBonusMax,
		RecentBandRecordMinUniqueSpotters: cfg.RecentBandRecordMinUniqueSpotters,
		RecentBandStore:                   recentBandStore,
		PriorBonusEnabled:                 cfg.PriorBonusEnabled,
		PriorBonusMax:                     cfg.PriorBonusMax,
		PriorBonusDistanceMax:             cfg.PriorBonusDistanceMax,
		PriorBonusRequiresSCP:             cfg.PriorBonusRequiresSCP,
		PriorBonusApplyTo:                 cfg.PriorBonusApplyTo,
		PriorBonusKnownCallset:            knownCallset,
		Cooldown:                          cooldown,
		CooldownMinReporters:              cooldownMinReports,
		DecisionObserver:                  decisionObserver,
	}
}

type bandStateParams struct {
	QualityBinHz         int
	FrequencyToleranceHz float64
}

// resolveBandStateParams returns per-band, per-state overrides when defined; otherwise false.
// Purpose: Resolve per-band/state overrides for call correction parameters.
// Key aspects: Matches band/state in override list; returns ok on match.
// Upstream: maybeApplyCallCorrectionWithLogger.
// Downstream: band/state override scanning.
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

// Purpose: Compute the frequency averaging look-back window.
// Key aspects: Uses policy defaults with a minimum of zero.
// Upstream: processOutputSpots frequency averaging path.
// Downstream: time.Duration math.
func frequencyAverageWindow(policy config.SpotPolicy) time.Duration {
	seconds := policy.FrequencyAveragingSeconds
	if seconds <= 0 {
		seconds = 45
	}
	return time.Duration(seconds) * time.Second
}

// Purpose: Compute the frequency averaging tolerance in kHz.
// Key aspects: Converts Hz config to kHz float.
// Upstream: processOutputSpots frequency averaging path.
// Downstream: float math.
func frequencyAverageTolerance(policy config.SpotPolicy) float64 {
	toleranceHz := policy.FrequencyAveragingToleranceHz
	if toleranceHz <= 0 {
		toleranceHz = 300
	}
	return toleranceHz / 1000.0
}

// Purpose: Decide whether a mode should carry confidence glyphs.
// Key aspects: Treats USB/LSB as voice modes; digital modes remain exempt.
// Upstream: processOutputSpots confidence seeding and fallback.
// Downstream: strings.ToUpper/TrimSpace.
func modeSupportsConfidenceGlyph(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "CW", "RTTY", "USB", "LSB":
		return true
	default:
		return false
	}
}

// recordRecentBandObservation records accepted spots for recent-on-band
// corroboration. This is intentionally post-gate (after correction/harmonic/
// license filters) so the cache favors calls that already survived core checks.
// Stabilizer-delayed spots are recorded only after delay resolution.
func recordRecentBandObservation(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig) {
	if s == nil || store == nil || s.IsBeacon {
		return
	}
	if !cfg.RecentBandBonusEnabled && !cfg.StabilizerEnabled {
		return
	}
	mode := s.ModeNorm
	if strings.TrimSpace(mode) == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	spotter := s.DECallNorm
	if spotter == "" {
		spotter = s.DECall
	}
	seenAt := s.Time.UTC()
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}
	store.Record(call, band, mode, spotter, seenAt)
}

// Purpose: Apply confidence floor only when confidence is still unknown.
// Key aspects: If confidence is '?', upgrade to 'S' when DX call is in
// known-calls (SCP) or admitted by recent-on-band evidence.
// Upstream: processOutputSpots after correction/confidence assignment.
// Downstream: KnownCallsigns.Contains, RecentBandStore.HasRecentSupport, and modeSupportsConfidenceGlyph.
func applyKnownCallFloor(
	s *spot.Spot,
	knownCalls *atomic.Pointer[spot.KnownCallsigns],
	recentBandStore *spot.RecentBandStore,
	corrCfg config.CallCorrectionConfig,
) bool {
	if s == nil || s.IsBeacon {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !modeSupportsConfidenceGlyph(mode) {
		return false
	}
	if strings.TrimSpace(s.Confidence) != "?" {
		return false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if call == "" {
		return false
	}

	knownHit := false
	if knownCalls != nil {
		if known := knownCalls.Load(); known != nil && known.Contains(call) {
			knownHit = true
		}
	}

	recentHit := false
	if recentBandStore != nil && corrCfg.RecentBandBonusEnabled {
		band := s.BandNorm
		if band == "" || band == "???" {
			band = spot.FreqToBand(s.Frequency)
		}
		recentHit = recentBandStore.HasRecentSupport(call, band, mode, corrCfg.RecentBandRecordMinUniqueSpotters, time.Now().UTC())
	}

	if knownHit || recentHit {
		s.Confidence = "S"
		return true
	}
	return false
}

func sourceStatsLabel(s *spot.Spot) string {
	if s == nil {
		return "OTHER"
	}
	switch s.SourceType {
	case spot.SourceManual:
		return "HUMAN"
	case spot.SourceRBN:
		return rbnStatsLabel(s.SourceNode)
	case spot.SourceFT8, spot.SourceFT4:
		return "RBN-FT"
	case spot.SourcePSKReporter:
		return "PSKREPORTER"
	case spot.SourcePeer:
		return "PEER"
	case spot.SourceUpstream:
		return "UPSTREAM"
	}

	node := strings.ToUpper(strings.TrimSpace(s.SourceNode))
	switch node {
	case "RBN-DIGITAL":
		return "RBN-DIGITAL"
	case "RBN":
		return "RBN"
	case "PSKREPORTER":
		return "PSKREPORTER"
	case "P92":
		return "PEER"
	case "UPSTREAM":
		return "UPSTREAM"
	}
	return "OTHER"
}

func rbnStatsLabel(sourceNode string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(sourceNode))
	if trimmed == "RBN-DIGITAL" {
		return "RBN-DIGITAL"
	}
	return "RBN"
}

// spotsToEntries converts []*spot.Spot to bandmap.SpotEntry using Hz units for frequency.
// Purpose: Convert spots into bandmap entries.
// Key aspects: Maps DX call, frequency, and time into bandmap format.
// Upstream: bandmap updates in processOutputSpots.
// Downstream: bandmap.SpotEntry allocation.
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

// Purpose: Format the confidence string for corrected calls.
// Key aspects: Encodes percent-only consensus buckets (P/V/?); SCP floor applied later.
// Upstream: maybeApplyCallCorrectionWithLogger.
// Downstream: None (pure mapping).
func formatConfidence(percent int, totalReporters int) string {
	if totalReporters <= 1 {
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

// Purpose: Decide whether a spot is eligible for frequency averaging.
// Key aspects: Skips digital modes and requires a valid call/frequency.
// Upstream: processOutputSpots frequency averaging path.
// Downstream: string checks on mode and call.
func shouldAverageFrequency(s *spot.Spot) bool {
	mode := strings.ToUpper(strings.TrimSpace(s.Mode))
	return mode == "CW" || mode == "RTTY"
}

// Purpose: Download and load the RBN skew correction table.
// Key aspects: Uses configured URL and refreshes the in-memory store.
// Upstream: startSkewScheduler and startup initialization.
// Downstream: skew.Download, skew.LoadBytes, store.Set.
func refreshSkewTable(ctx context.Context, cfg config.SkewConfig, store *skew.Store) (int, error) {
	if store == nil {
		return 0, errors.New("skew: store is nil")
	}
	if ctx == nil {
		return 0, errors.New("skew: nil context")
	}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
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

// Purpose: Periodically refresh the skew table based on configured schedule.
// Key aspects: Sleeps until next refresh time and exits on ctx.Done.
// Upstream: main startup when skew is enabled.
// Downstream: refreshSkewTable and nextSkewRefreshDelay.
func startSkewScheduler(ctx context.Context, cfg config.SkewConfig, store *skew.Store) {
	if store == nil {
		return
	}
	// Purpose: Background refresh loop for skew table updates.
	// Key aspects: Waits for computed delays and respects context cancellation.
	// Upstream: startSkewScheduler.
	// Downstream: refreshSkewTable and time.NewTimer.
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
			if count, err := refreshSkewTable(ctx, cfg, store); err != nil {
				log.Printf("Warning: scheduled RBN skew download failed: %v", err)
			} else {
				log.Printf("Scheduled RBN skew download complete (%d entries)", count)
			}
		}
	}()
}

// Purpose: Compute delay until the next skew refresh time.
// Key aspects: Uses configured hour/minute and wraps to next day.
// Upstream: startSkewScheduler.
// Downstream: skewRefreshHourMinute and time math.
func nextSkewRefreshDelay(cfg config.SkewConfig, now time.Time) time.Duration {
	hour, minute := skewRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

// Purpose: Resolve the target hour/minute for skew refresh.
// Key aspects: Defaults to 02:00 UTC when unset.
// Upstream: nextSkewRefreshDelay.
// Downstream: None.
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

// Purpose: Periodically refresh the known callsigns dataset.
// Key aspects: Scheduled daily refresh and atomic pointer swap.
// Upstream: main startup when known calls are enabled.
// Downstream: refreshKnownCallsigns, seedKnownCalls, and time.NewTimer.
// startKnownCallScheduler downloads the known-calls file at the configured UTC
// time every day and updates the in-memory cache pointer after each refresh.
func startKnownCallScheduler(ctx context.Context, cfg config.KnownCallsConfig, knownPtr *atomic.Pointer[spot.KnownCallsigns], store *gridStoreHandle, metaCache *callMetaCache) {
	if knownPtr == nil {
		return
	}
	// Purpose: Background refresh loop for known callsigns.
	// Key aspects: Waits until next scheduled time; exits on ctx.Done.
	// Upstream: startKnownCallScheduler.
	// Downstream: refreshKnownCallsigns and time.NewTimer.
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
			if fresh, updated, err := refreshKnownCallsigns(ctx, cfg); err != nil {
				log.Printf("Warning: scheduled known calls download failed: %v", err)
			} else if updated && fresh != nil {
				knownPtr.Store(fresh)
				log.Printf("Scheduled known calls download complete (%d entries)", fresh.Count())
				if store != nil {
					if db := store.Store(); db != nil {
						if err := seedKnownCalls(db, fresh); err != nil {
							log.Printf("Warning: failed to reseed known calls into grid database: %v", err)
						}
					}
				}
				if metaCache != nil {
					metaCache.Clear()
				}
			} else {
				log.Printf("Scheduled known calls download: up to date (%s)", cfg.File)
			}
		}
	}()
}

// Purpose: Download and parse the known calls file when updated.
// Key aspects: Uses conditional HTTP download and returns (cache, updated).
// Upstream: startKnownCallScheduler and startup.
// Downstream: download.Download and spot.LoadKnownCallsigns.
// refreshKnownCallsigns downloads the known calls file, writes it to disk, and
// returns the parsed cache when the remote content changed.
func refreshKnownCallsigns(ctx context.Context, cfg config.KnownCallsConfig) (*spot.KnownCallsigns, bool, error) {
	url := strings.TrimSpace(cfg.URL)
	path := strings.TrimSpace(cfg.File)
	if url == "" {
		return nil, false, errors.New("known calls: URL is empty")
	}
	if path == "" {
		return nil, false, errors.New("known calls: file path is empty")
	}
	if ctx == nil {
		return nil, false, errors.New("known calls: nil context")
	}
	result, err := download.Download(ctx, download.Request{
		URL:         url,
		Destination: path,
		Timeout:     1 * time.Minute,
	})
	if err != nil {
		return nil, false, fmt.Errorf("known calls: %w", err)
	}
	if result.Status != download.StatusUpdated {
		return nil, false, nil
	}
	known, err := spot.LoadKnownCallsigns(path)
	if err != nil {
		return nil, true, err
	}
	return known, true, nil
}

// Purpose: Compute delay until the next known calls refresh time.
// Key aspects: Uses configured hour/minute and wraps to next day.
// Upstream: startKnownCallScheduler.
// Downstream: knownCallRefreshHourMinute and time math.
func nextKnownCallRefreshDelay(cfg config.KnownCallsConfig, now time.Time) time.Duration {
	hour, minute := knownCallRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

// Purpose: Resolve the target hour/minute for known call refresh.
// Key aspects: Defaults to 03:00 UTC when unset.
// Upstream: nextKnownCallRefreshDelay.
// Downstream: time.Parse.
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

// Purpose: Periodically refresh the CTY database from remote URL.
// Key aspects: Scheduled daily refresh, retry with backoff, atomic pointer swap.
// Upstream: main startup when CTY is enabled.
// Downstream: refreshCTYDatabase and time.NewTimer.
// startCTYScheduler downloads cty.plist at the configured UTC time every day and
// updates the in-memory CTY database pointer after each refresh.
func startCTYScheduler(ctx context.Context, cfg config.CTYConfig, ctyPtr *atomic.Pointer[cty.CTYDatabase], metaCache *callMetaCache, state *ctyRefreshState) {
	if ctyPtr == nil {
		return
	}
	// Purpose: Background refresh loop for CTY database updates.
	// Key aspects: Waits until next scheduled time, retries with backoff, records age/failures.
	// Upstream: startCTYScheduler.
	// Downstream: refreshCTYDatabase and time.NewTimer.
	go func() {
		const (
			ctyRetryBase = 1 * time.Minute
			ctyRetryMax  = 30 * time.Minute
		)
		for {
			delay := nextCTYRefreshDelay(cfg, time.Now().UTC())
			if !sleepWithContext(ctx, delay) {
				return
			}

			backoff := ctyRetryBase
			attempt := 0
			for {
				fresh, updated, err := refreshCTYDatabase(ctx, cfg)
				if err == nil {
					if updated && fresh != nil {
						ctyPtr.Store(fresh)
						if metaCache != nil {
							metaCache.Clear()
						}
						log.Printf("Scheduled CTY download complete (%d prefixes)", len(fresh.Keys))
					} else {
						log.Printf("Scheduled CTY download: up to date (%s)", cfg.File)
					}
					if state != nil {
						state.recordSuccess(time.Now().UTC())
					}
					break
				}
				attempt++
				if state != nil {
					state.recordFailure(time.Now().UTC(), err)
				}
				lastAge := "unknown"
				if state != nil {
					if age, ok := state.age(time.Now().UTC()); ok {
						lastAge = formatDurationShort(age)
					}
				}
				log.Printf("Warning: scheduled CTY download failed (attempt=%d last_success=%s next_retry=%s): %v", attempt, lastAge, backoff, err)
				if !sleepWithContext(ctx, backoff) {
					return
				}
				backoff *= 2
				if backoff > ctyRetryMax {
					backoff = ctyRetryMax
				}
			}
		}
	}()
}

// Purpose: Sleep for a duration unless the context is canceled.
// Key aspects: Timer-based wait with cancellation.
// Upstream: CTY refresh scheduler.
// Downstream: time.NewTimer.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Purpose: Enqueue daily propagation reports on a fixed UTC schedule.
// Key aspects: Uses configured refresh time, skips when logging is disabled, and
// verifies the prior-day log file exists before enqueueing.
// Upstream: main prop-report initialization.
// Downstream: nextDailyUTC, propReportScheduler.Enqueue.
func startPropReportDailyScheduler(ctx context.Context, cfg config.PropReportConfig, logCfg config.LoggingConfig, scheduler *propReportScheduler) {
	if scheduler == nil || !cfg.Enabled {
		return
	}
	if !logCfg.Enabled {
		return
	}
	logDir := strings.TrimSpace(logCfg.Dir)
	if logDir == "" {
		log.Printf("Prop report daily scheduler disabled (logging.dir empty)")
		return
	}

	go func() {
		for {
			delay := nextDailyUTC(cfg.RefreshUTC, time.Now().UTC(), 0, 5)
			if !sleepWithContext(ctx, delay) {
				return
			}
			now := time.Now().UTC()
			reportDate := dateOnly(now).AddDate(0, 0, -1)
			logPath := filepath.Join(logDir, logFileNameForDate(reportDate))
			if _, err := os.Stat(logPath); err != nil {
				if os.IsNotExist(err) {
					log.Printf("Prop report skip: log file missing for %s (%s)", reportDate.Format("2006-01-02"), logPath)
				} else {
					log.Printf("Prop report skip: log file stat failed for %s (%s): %v", reportDate.Format("2006-01-02"), logPath, err)
				}
				continue
			}
			scheduler.Enqueue(propReportJob{
				Date:    reportDate,
				LogPath: logPath,
			})
		}
	}()
}

// Purpose: Download and parse the CTY database when updated.
// Key aspects: Uses conditional HTTP download and returns (db, updated).
// Upstream: startCTYScheduler and startup.
// Downstream: download.Download and cty.LoadCTYDatabase.
// refreshCTYDatabase downloads cty.plist, writes it atomically, and returns the parsed DB.
func refreshCTYDatabase(ctx context.Context, cfg config.CTYConfig) (*cty.CTYDatabase, bool, error) {
	url := strings.TrimSpace(cfg.URL)
	path := strings.TrimSpace(cfg.File)
	if url == "" {
		return nil, false, errors.New("cty: URL is empty")
	}
	if path == "" {
		return nil, false, errors.New("cty: file path is empty")
	}
	if ctx == nil {
		return nil, false, errors.New("cty: nil context")
	}
	result, err := download.Download(ctx, download.Request{
		URL:         url,
		Destination: path,
		Timeout:     1 * time.Minute,
	})
	if err != nil {
		return nil, false, fmt.Errorf("cty: %w", err)
	}
	if result.Status != download.StatusUpdated {
		return nil, false, nil
	}
	db, err := cty.LoadCTYDatabase(path)
	if err != nil {
		return nil, true, fmt.Errorf("cty: load: %w", err)
	}
	return db, true, nil
}

// Purpose: Compute delay until the next CTY refresh time.
// Key aspects: Uses configured hour/minute and wraps to next day.
// Upstream: startCTYScheduler.
// Downstream: ctyRefreshHourMinute and time math.
func nextCTYRefreshDelay(cfg config.CTYConfig, now time.Time) time.Duration {
	hour, minute := ctyRefreshHourMinute(cfg)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

// Purpose: Resolve the target hour/minute for CTY refresh.
// Key aspects: Defaults to 04:00 UTC when unset.
// Upstream: nextCTYRefreshDelay.
// Downstream: time.Parse.
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

// Purpose: Format the grid database status line for stats output.
// Key aspects: Uses cache hit rate, lookup rate, and drop counts when available.
// Upstream: displayStatsWithFCC.
// Downstream: gridStoreHandle.Store().Count and humanize.Comma.
func formatGridLine(metrics *gridMetrics, store *gridStoreHandle, predictor *pathreliability.Predictor) string {
	updatesSinceStart := metrics.learnedTotal.Load()
	cacheLookups := metrics.cacheLookups.Load()
	cacheHits := metrics.cacheHits.Load()
	asyncDrops := metrics.asyncDrops.Load()
	syncDrops := metrics.syncDrops.Load()

	dbTotal := int64(-1)
	if store != nil {
		if db := store.Store(); db != nil {
			if count, err := db.Count(); err == nil {
				dbTotal = count
			} else {
				log.Printf("Warning: gridstore count failed: %v", err)
			}
		}
	}
	hitRate := 0.0
	if cacheLookups > 0 {
		hitRate = float64(cacheHits) * 100 / float64(cacheLookups)
	}
	hitPercent := int(math.Ceil(hitRate))

	lookupsPerMin := int64(0)
	now := time.Now().UTC()
	metrics.rateMu.Lock()
	if !metrics.lastSample.IsZero() {
		elapsed := now.Sub(metrics.lastSample)
		if elapsed > 0 && cacheLookups >= metrics.lastLookupCount {
			delta := cacheLookups - metrics.lastLookupCount
			lookupsPerMin = int64(math.Round(float64(delta) / elapsed.Minutes()))
		}
	}
	metrics.lastLookupCount = cacheLookups
	metrics.lastSample = now
	metrics.rateMu.Unlock()

	var propPairs string
	if predictor != nil {
		stats := predictor.Stats(time.Now().UTC())
		if stats.CombinedFine > 0 || stats.CombinedCoarse > 0 {
			propPairs = fmt.Sprintf(" | Path pairs %s (L2) / %s (L1)",
				humanize.Comma(int64(stats.CombinedFine)),
				humanize.Comma(int64(stats.CombinedCoarse)))
		}
	}
	drops := fmt.Sprintf(" | Drop a%s s%s",
		humanize.Comma(int64(asyncDrops)),
		humanize.Comma(int64(syncDrops)))
	lookupRate := humanize.Comma(lookupsPerMin)
	if dbTotal >= 0 {
		return fmt.Sprintf("Grids: %s TOTAL / %d%% / %s%s%s",
			humanize.Comma(dbTotal),
			hitPercent,
			lookupRate,
			drops,
			propPairs)
	}
	return fmt.Sprintf("Grids: %s UPDATED / %d%% / %s%s%s",
		humanize.Comma(int64(updatesSinceStart)),
		hitPercent,
		lookupRate,
		drops,
		propPairs)
}

type fccSnapshot struct {
	HDCount   int64
	AMCount   int64
	DBSize    int64
	UpdatedAt time.Time
	Path      string
}

type ctyRefreshState struct {
	lastSuccess  atomic.Int64
	lastFailure  atomic.Int64
	failureCount atomic.Int64
	lastError    atomic.Value
}

func newCTYRefreshState() *ctyRefreshState {
	state := &ctyRefreshState{}
	state.lastError.Store("")
	return state
}

func (s *ctyRefreshState) recordSuccess(now time.Time) {
	if s == nil {
		return
	}
	s.lastSuccess.Store(now.Unix())
	s.failureCount.Store(0)
	s.lastError.Store("")
}

func (s *ctyRefreshState) recordFailure(now time.Time, err error) {
	if s == nil {
		return
	}
	s.lastFailure.Store(now.Unix())
	s.failureCount.Add(1)
	if err != nil {
		s.lastError.Store(err.Error())
	}
}

func (s *ctyRefreshState) age(now time.Time) (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	ts := s.lastSuccess.Load()
	if ts <= 0 {
		return 0, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.Sub(time.Unix(ts, 0)), true
}

func (s *ctyRefreshState) lastSuccessTime() (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	ts := s.lastSuccess.Load()
	if ts <= 0 {
		return time.Time{}, false
	}
	return time.Unix(ts, 0).UTC(), true
}

// Purpose: Format combined CTY + FCC last-updated line for stats output.
// Key aspects: Omits FCC record counts; always uses UTC timestamps.
// Upstream: displayStatsWithFCC.
// Downstream: time formatting.
func formatDataLineOrPlaceholder(ctyLookup func() *cty.CTYDatabase, state *ctyRefreshState, fcc *fccSnapshot) string {
	ctyPart := "CTY (not loaded)"
	if ctyLookup != nil && ctyLookup() != nil {
		ctyPart = "CTY (loaded)"
		if state != nil {
			if ts := state.lastSuccess.Load(); ts > 0 {
				ctyPart = "CTY " + formatUpdatedTimestamp(time.Unix(ts, 0))
			} else {
				ctyPart = "CTY (loaded, time unknown)"
			}
		}
	}
	fccPart := "FCC ULS (not available)"
	if fcc != nil {
		if !fcc.UpdatedAt.IsZero() {
			fccPart = "FCC ULS " + formatUpdatedTimestamp(fcc.UpdatedAt)
		} else {
			fccPart = "FCC ULS (time unknown)"
		}
	}
	return fmt.Sprintf("Data: %s | %s", ctyPart, fccPart)
}

func formatUpdatedTimestamp(ts time.Time) string {
	if ts.IsZero() {
		return "unknown"
	}
	return ts.UTC().Format("01-02-2006 15:04Z")
}

// Purpose: Format grid status or a placeholder when disabled/unavailable.
// Key aspects: Falls back to a placeholder when metrics/store missing.
// Upstream: displayStatsWithFCC.
// Downstream: formatGridLine.
func formatGridLineOrPlaceholder(metrics *gridMetrics, store *gridStoreHandle, predictor *pathreliability.Predictor) string {
	if metrics == nil {
		return "Grids: (not available)"
	}
	return formatGridLine(metrics, store, predictor)
}

// Purpose: Seed the grid database with known calls.
// Key aspects: Writes known call grids and logs failures.
// Upstream: main startup and known calls refresh scheduler.
// Downstream: gridstore.Store.Set and known calls iterator.
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

// Purpose: Start the grid writer pipeline and return update hooks.
// Key aspects: Provides enqueue, metrics, stop, lookup functions, and CTY-derived grids on misses.
// Upstream: main grid store setup.
// Downstream: gridstore.Store and callMetaCache methods.
func startGridWriter(storeHandle *gridStoreHandle, flushInterval time.Duration, cache *callMetaCache, ttl time.Duration, dbCheckOnMiss bool, ctyLookup func() *cty.CTYDatabase) (func(call, grid string), func(call string, info *cty.PrefixInfo), *gridMetrics, func(), func(call string) (string, bool, bool), func(call string) (string, bool, bool)) {
	if storeHandle == nil {
		noop := func(string, string) {}
		noopCTY := func(string, *cty.PrefixInfo) {}
		noopLookup := func(string) (string, bool, bool) { return "", false, false }
		return noop, noopCTY, &gridMetrics{}, func() {}, noopLookup, noopLookup
	}
	if flushInterval <= 0 {
		flushInterval = 60 * time.Second
	}
	metrics := &gridMetrics{}
	// dbCheckOnMiss gates async cache backfill and tight-timeout sync lookups.
	// Sync lookups are opt-in per caller so the main hot path stays non-blocking.
	asyncLookupEnabled := dbCheckOnMiss
	syncLookupEnabled := dbCheckOnMiss && gridSyncLookupWorkers > 0 && gridSyncLookupQueueDepth > 0 && gridSyncLookupTimeout > 0
	getStore := func() *gridstore.Store {
		return storeHandle.Store()
	}
	storeAvailable := func() bool {
		return storeHandle.Available()
	}
	updates := make(chan gridstore.Record, 8192)
	done := make(chan struct{})
	lookupQueue := make(chan gridLookupRequest, 4096)
	lookupDone := make(chan struct{})
	var lookupPendingMu sync.Mutex
	lookupPending := make(map[string]struct{})
	var syncLookupQueue chan gridLookupRequest
	var syncLookupWG sync.WaitGroup
	if syncLookupEnabled {
		syncLookupQueue = make(chan gridLookupRequest, gridSyncLookupQueueDepth)
	}

	mergePending := func(existing, incoming gridstore.Record) gridstore.Record {
		merged := existing
		if incoming.Grid.Valid {
			merged.Grid = incoming.Grid
		}
		if incoming.IsKnown {
			merged.IsKnown = true
		}
		if incoming.CTYValid {
			merged.CTYValid = true
			merged.CTYADIF = incoming.CTYADIF
			merged.CTYCQZone = incoming.CTYCQZone
			merged.CTYITUZone = incoming.CTYITUZone
			merged.CTYContinent = incoming.CTYContinent
			merged.CTYCountry = incoming.CTYCountry
		}
		if incoming.Observations > 0 {
			merged.Observations += incoming.Observations
		}
		if !incoming.FirstSeen.IsZero() {
			if merged.FirstSeen.IsZero() || incoming.FirstSeen.Before(merged.FirstSeen) {
				merged.FirstSeen = incoming.FirstSeen
			}
		}
		if !incoming.UpdatedAt.IsZero() {
			if merged.UpdatedAt.IsZero() || incoming.UpdatedAt.After(merged.UpdatedAt) {
				merged.UpdatedAt = incoming.UpdatedAt
			}
		}
		if incoming.ExpiresAt != nil {
			merged.ExpiresAt = incoming.ExpiresAt
		}
		return merged
	}

	lookupRecord := func(baseCall, rawCall string) (*gridstore.Record, error) {
		var baseRec *gridstore.Record
		if baseCall == "" {
			return baseRec, nil
		}
		store := getStore()
		if store != nil {
			rec, err := store.Get(baseCall)
			if err != nil {
				return nil, err
			}
			if rec != nil {
				if rec.Grid.Valid {
					return rec, nil
				}
				baseRec = rec
			}
			if rawCall != "" && rawCall != baseCall {
				rec, err = store.Get(rawCall)
				if err != nil {
					return nil, err
				}
				if rec != nil {
					if rec.Grid.Valid {
						return rec, nil
					}
					if baseRec == nil {
						baseRec = rec
					}
				}
			}
		}
		if ctyLookup == nil {
			return baseRec, nil
		}
		ctyDB := ctyLookup()
		if ctyDB == nil {
			return baseRec, nil
		}
		info := effectivePrefixInfo(ctyDB, cache, baseCall)
		if info == nil {
			return baseRec, nil
		}
		derivedGrid, ok := cty.Grid4FromLatLon(info.Latitude, info.Longitude)
		if !ok {
			return baseRec, nil
		}
		now := time.Now().UTC()
		derivedRec := gridstore.Record{
			Call:         baseCall,
			Grid:         sqlNullString(derivedGrid),
			GridDerived:  true,
			Observations: 1,
			FirstSeen:    now,
			UpdatedAt:    now,
		}
		if baseRec != nil {
			derivedRec.IsKnown = baseRec.IsKnown
			if baseRec.CTYValid {
				derivedRec.CTYValid = true
				derivedRec.CTYADIF = baseRec.CTYADIF
				derivedRec.CTYCQZone = baseRec.CTYCQZone
				derivedRec.CTYITUZone = baseRec.CTYITUZone
				derivedRec.CTYContinent = baseRec.CTYContinent
				derivedRec.CTYCountry = baseRec.CTYCountry
			}
		}
		if storeAvailable() {
			select {
			case updates <- derivedRec:
			default:
				// Drop silently to avoid backpressure on the lookup pipeline.
			}
		}
		return &derivedRec, nil
	}

	// Purpose: Background writer loop to batch metadata updates and periodic TTL purges.
	// Key aspects: Flushes on size/interval and closes done on exit.
	// Upstream: startGridWriter.
	// Downstream: store.UpsertBatch, store.PurgeOlderThan, and metrics updates.
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

		pending := make(map[string]gridstore.Record)
		// Purpose: Flush pending updates to the database in a batch.
		// Key aspects: Retains batch on busy errors; clears on success.
		// Upstream: update loop and ticker ticks.
		// Downstream: store.UpsertBatch, metrics.learnedTotal.
		flush := func() {
			if len(pending) == 0 {
				return
			}
			store := getStore()
			if store == nil {
				clear(pending)
				return
			}
			batch := make([]gridstore.Record, 0, len(pending))
			gridUpdates := 0
			for call := range pending {
				rec := pending[call]
				if rec.Grid.Valid {
					gridUpdates++
				}
				batch = append(batch, rec)
			}
			start := time.Now().UTC()
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
			if gridUpdates > 0 {
				metrics.learnedTotal.Add(uint64(gridUpdates))
			}
			clear(pending)
		}

		for {
			select {
			case rec, ok := <-updates:
				if !ok {
					flush()
					return
				}
				call := normalizeCallForMetadata(rec.Call)
				if call == "" {
					continue
				}
				rec.Call = call
				if existing, exists := pending[call]; exists {
					pending[call] = mergePending(existing, rec)
				} else {
					pending[call] = rec
				}
				if len(pending) >= 500 {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ttlCh:
				store := getStore()
				if store == nil {
					continue
				}
				cutoff := time.Now().UTC().Add(-ttl)
				if removed, err := store.PurgeOlderThan(cutoff); err != nil {
					log.Printf("Warning: gridstore TTL purge failed: %v", err)
				} else if removed > 0 {
					log.Printf("Gridstore: purged %d entries older than %v", removed, ttl)
				}
			}
		}
	}()

	// Purpose: Enqueue a grid update without blocking the output pipeline.
	// Key aspects: Normalizes call/grid, uses cache to suppress duplicates.
	// Upstream: processOutputSpots gridUpdate hook.
	// Downstream: cache.UpdateGrid and updates channel.
	gridUpdateFn := func(call, grid string) {
		call = normalizeCallForMetadata(call)
		grid = strings.TrimSpace(strings.ToUpper(grid))
		if call == "" || len(grid) < 4 {
			return
		}
		if cache != nil && !cache.UpdateGrid(call, grid, false) {
			return
		}
		if !storeAvailable() {
			return
		}
		now := time.Now().UTC()
		rec := gridstore.Record{
			Call:         call,
			Grid:         sqlNullString(grid),
			Observations: 1,
			FirstSeen:    now,
			UpdatedAt:    now,
		}
		select {
		case updates <- rec:
		default:
			// Drop silently to avoid backpressure on the spot pipeline.
		}
	}

	// Purpose: Enqueue CTY metadata updates without blocking ingest.
	// Key aspects: Stores CTY fields only; relies on gridstore merge for retention.
	// Upstream: ingest validation cache misses.
	// Downstream: updates channel.
	ctyUpdateFn := func(call string, info *cty.PrefixInfo) {
		if info == nil {
			return
		}
		call = normalizeCallForMetadata(call)
		if call == "" {
			return
		}
		if !storeAvailable() {
			return
		}
		now := time.Now().UTC()
		rec := gridstore.Record{
			Call:         call,
			CTYValid:     true,
			CTYADIF:      info.ADIF,
			CTYCQZone:    info.CQZone,
			CTYITUZone:   info.ITUZone,
			CTYContinent: info.Continent,
			CTYCountry:   info.Country,
			FirstSeen:    now,
			UpdatedAt:    now,
		}
		select {
		case updates <- rec:
		default:
			// Drop silently to avoid backpressure on the ingest pipeline.
		}
	}

	// Purpose: Stop the grid writer and optional async lookup goroutine.
	// Key aspects: Closes channels and waits for clean shutdown.
	// Upstream: main shutdown path.
	// Downstream: channel close and done waits.
	stopFn := func() {
		close(updates)
		<-done
		if asyncLookupEnabled {
			close(lookupQueue)
			<-lookupDone
		}
		if syncLookupEnabled {
			close(syncLookupQueue)
			syncLookupWG.Wait()
		}
	}

	// Purpose: Lookup a grid entry with optional async backfill on cache miss.
	// Key aspects: Avoids synchronous DB reads on the output pipeline.
	// Upstream: processOutputSpots gridLookup hook.
	// Downstream: cache.LookupGrid and lookupQueue enqueue.
	lookupFn := func(call string) (string, bool, bool) {
		rawCall := strings.ToUpper(strings.TrimSpace(call))
		baseCall := normalizeCallForMetadata(rawCall)
		if baseCall == "" {
			return "", false, false
		}
		if cache != nil {
			if grid, derived, ok := cache.LookupGrid(baseCall, metrics); ok {
				return grid, derived, true
			}
		}
		// Cache miss: enqueue async lookup to avoid blocking output.
		if asyncLookupEnabled && storeAvailable() {
			lookupPendingMu.Lock()
			if _, exists := lookupPending[baseCall]; !exists {
				lookupPending[baseCall] = struct{}{}
				select {
				case lookupQueue <- gridLookupRequest{baseCall: baseCall, rawCall: rawCall}:
				default:
					delete(lookupPending, baseCall)
					metrics.asyncDrops.Add(1)
				}
			}
			lookupPendingMu.Unlock()
		}
		return "", false, false
	}

	if asyncLookupEnabled {
		// Purpose: Background cache backfill for grid lookups.
		// Key aspects: Reads from lookupQueue and populates cache entries.
		// Upstream: lookupFn enqueue path.
		// Downstream: store.Get and cache.ApplyRecord.
		go func() {
			defer close(lookupDone)
			for req := range lookupQueue {
				rec, err := lookupRecord(req.baseCall, req.rawCall)
				if err == nil && rec != nil {
					if cache != nil {
						cache.ApplyRecord(*rec)
					}
				} else if err != nil && !gridstore.IsBusyError(err) {
					log.Printf("Warning: gridstore async lookup failed for %s: %v", req.baseCall, err)
				}
				lookupPendingMu.Lock()
				delete(lookupPending, req.baseCall)
				lookupPendingMu.Unlock()
			}
		}()
	} else {
		close(lookupDone)
	}

	resolveGrid := func(rec *gridstore.Record) (string, bool, bool) {
		if rec == nil || !rec.Grid.Valid {
			return "", false, false
		}
		grid := strings.TrimSpace(strings.ToUpper(rec.Grid.String))
		if grid == "" {
			return "", false, false
		}
		return grid, rec.GridDerived, true
	}

	if syncLookupEnabled {
		// Purpose: Bounded synchronous backfill worker pool for tight-timeout lookups.
		// Key aspects: Uses a fixed worker count and buffered queue to cap concurrency.
		// Upstream: gridLookupSync enqueue path.
		// Downstream: store.Get and cache.ApplyRecord.
		for i := 0; i < gridSyncLookupWorkers; i++ {
			syncLookupWG.Add(1)
			go func() {
				defer syncLookupWG.Done()
				for req := range syncLookupQueue {
					rec, err := lookupRecord(req.baseCall, req.rawCall)
					if err == nil && rec != nil {
						if cache != nil {
							cache.ApplyRecord(*rec)
						}
						if req.resp != nil {
							grid, derived, ok := resolveGrid(rec)
							req.resp <- gridLookupResult{grid: grid, derived: derived, ok: ok}
						}
						continue
					}
					if err != nil && !gridstore.IsBusyError(err) {
						log.Printf("Warning: gridstore sync lookup failed for %s: %v", req.baseCall, err)
					}
					if req.resp != nil {
						req.resp <- gridLookupResult{}
					}
				}
			}()
		}
	}

	var lookupSyncFn func(call string) (string, bool, bool)
	if syncLookupEnabled {
		// Purpose: Provide a tight-timeout synchronous lookup path for first-spot grids.
		// Key aspects: Bounds enqueue time and total wait time; falls back to miss on timeout.
		// Upstream: processOutputSpots grid backfill for RBN.
		// Downstream: syncLookupQueue and cache.ApplyRecord.
		lookupSyncFn = func(call string) (string, bool, bool) {
			rawCall := strings.ToUpper(strings.TrimSpace(call))
			baseCall := normalizeCallForMetadata(rawCall)
			if baseCall == "" {
				return "", false, false
			}
			if cache != nil {
				if grid, derived, ok := cache.LookupGrid(baseCall, metrics); ok {
					return grid, derived, true
				}
			}
			if !storeAvailable() {
				return "", false, false
			}
			resp := make(chan gridLookupResult, 1)
			req := gridLookupRequest{baseCall: baseCall, rawCall: rawCall, resp: resp}
			deadline := time.Now().UTC().Add(gridSyncLookupTimeout)
			timer := time.NewTimer(gridSyncLookupTimeout)
			defer timer.Stop()

			select {
			case syncLookupQueue <- req:
			case <-timer.C:
				metrics.syncDrops.Add(1)
				return "", false, false
			}

			remaining := time.Until(deadline)
			if remaining <= 0 {
				return "", false, false
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(remaining)

			select {
			case res := <-resp:
				return res.grid, res.derived, res.ok
			case <-timer.C:
				return "", false, false
			}
		}
	}

	return gridUpdateFn, ctyUpdateFn, metrics, stopFn, lookupFn, lookupSyncFn
}

func startGridCheckpointScheduler(ctx context.Context, storeHandle *gridStoreHandle, dbPath string) {
	if storeHandle == nil || strings.TrimSpace(dbPath) == "" {
		return
	}
	go func() {
		for {
			delay := nextGridCheckpointDelay(time.Now().UTC())
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			store := storeHandle.Store()
			if store == nil {
				log.Printf("Gridstore: checkpoint skipped (store unavailable)")
				continue
			}
			root := gridCheckpointRoot(dbPath)
			if err := os.MkdirAll(root, 0o755); err != nil {
				log.Printf("Gridstore: checkpoint mkdir failed: %v", err)
				continue
			}
			ts := time.Now().UTC().Format(gridCheckpointNameLayoutUTC)
			tmp := filepath.Join(root, ".tmp-"+ts)
			dest := filepath.Join(root, ts)
			if err := store.Checkpoint(tmp); err != nil {
				log.Printf("Gridstore: checkpoint failed: %v", err)
				_ = os.RemoveAll(tmp)
				continue
			}
			if err := os.Rename(tmp, dest); err != nil {
				log.Printf("Gridstore: checkpoint rename failed: %v", err)
				_ = os.RemoveAll(tmp)
				continue
			}
			log.Printf("Gridstore: checkpoint created at %s", dest)
			if removed, err := cleanupGridCheckpoints(root, time.Now().UTC()); err != nil {
				log.Printf("Gridstore: checkpoint cleanup failed: %v", err)
			} else if removed > 0 {
				log.Printf("Gridstore: checkpoint cleanup removed %d old checkpoint(s)", removed)
			}
		}
	}()
}

func startGridIntegrityScheduler(ctx context.Context, storeHandle *gridStoreHandle) {
	if storeHandle == nil {
		return
	}
	go func() {
		for {
			delay := nextDailyUTC(gridIntegrityScanUTC, time.Now().UTC(), 5, 0)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			store := storeHandle.Store()
			if store == nil {
				log.Printf("Gridstore: integrity scan skipped (store unavailable)")
				continue
			}
			scanCtx, cancel := context.WithTimeout(ctx, gridIntegrityScanTimeout)
			stats, err := store.Verify(scanCtx, gridIntegrityScanTimeout)
			cancel()
			if err != nil {
				log.Printf("Gridstore: integrity scan failed: %v", err)
				continue
			}
			countNote := "count_meta=missing"
			if stats.CountMetaValid {
				countNote = fmt.Sprintf("count_meta=%d", stats.CountMeta)
			} else if stats.CountMetaErr != nil {
				countNote = fmt.Sprintf("count_meta_err=%v", stats.CountMetaErr)
			}
			log.Printf("Gridstore: integrity scan ok (records=%d duration=%s %s)", stats.Records, stats.Duration, countNote)
		}
	}()
}

func startGridStoreRecovery(ctx context.Context, storeHandle *gridStoreHandle, dbPath string, opts gridstore.Options, knownPtr *atomic.Pointer[spot.KnownCallsigns], metaCache *callMetaCache) {
	if storeHandle == nil {
		return
	}
	storeHandle.Set(nil)
	log.Printf("Gridstore: running without persistence during checkpoint restore (updates dropped)")
	go func() {
		start := time.Now().UTC()
		timer := time.NewTimer(gridRestoreWarnAfter)
		defer timer.Stop()
		restoreDone := make(chan struct{})
		defer close(restoreDone)
		go func() {
			select {
			case <-timer.C:
				log.Printf("Gridstore: restore still in progress after %s", gridRestoreWarnAfter)
			case <-restoreDone:
			}
		}()

		store, checkpointPath, err := restoreGridStoreFromCheckpoint(ctx, dbPath, opts)
		if err != nil {
			log.Printf("Gridstore: checkpoint restore failed: %v", err)
			return
		}
		if ctx.Err() != nil {
			_ = store.Close()
			return
		}
		storeHandle.Set(store)
		elapsed := time.Since(start)
		log.Printf("Gridstore: restored from %s in %s", checkpointPath, elapsed)
		if knownPtr != nil {
			if known := knownPtr.Load(); known != nil {
				if err := seedKnownCalls(store, known); err != nil {
					log.Printf("Warning: failed to seed known calls after restore: %v", err)
				}
			}
		}
		if metaCache != nil {
			metaCache.Clear()
		}
	}()
}

type gridCheckpointEntry struct {
	path string
	at   time.Time
}

func gridCheckpointRoot(dbPath string) string {
	return filepath.Join(dbPath, gridCheckpointDirName)
}

func listGridCheckpoints(root string) ([]gridCheckpointEntry, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var checkpoints []gridCheckpointEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".tmp-") {
			continue
		}
		at, err := time.Parse(gridCheckpointNameLayoutUTC, name)
		if err != nil {
			continue
		}
		checkpoints = append(checkpoints, gridCheckpointEntry{
			path: filepath.Join(root, name),
			at:   at,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].at.After(checkpoints[j].at)
	})
	return checkpoints, nil
}

func cleanupGridCheckpoints(root string, now time.Time) (int, error) {
	if root == "" {
		return 0, nil
	}
	checkpoints, err := listGridCheckpoints(root)
	if err != nil {
		return 0, err
	}
	cutoff := now.Add(-gridCheckpointRetention)
	removed := 0
	for _, checkpoint := range checkpoints {
		if checkpoint.at.Before(cutoff) {
			if err := os.RemoveAll(checkpoint.path); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}

func nextGridCheckpointDelay(now time.Time) time.Duration {
	target := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC).Add(gridCheckpointInterval)
	if !target.After(now) {
		target = target.Add(gridCheckpointInterval)
	}
	return target.Sub(now)
}

func parseUTCHourMinute(value string, fallbackHour int, fallbackMinute int) (int, int) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallbackHour, fallbackMinute
	}
	parsed, err := time.Parse("15:04", trimmed)
	if err != nil {
		return fallbackHour, fallbackMinute
	}
	return parsed.Hour(), parsed.Minute()
}

func nextDailyUTC(refreshUTC string, now time.Time, fallbackHour int, fallbackMinute int) time.Duration {
	hour, minute := parseUTCHourMinute(refreshUTC, fallbackHour, fallbackMinute)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func restoreGridStoreFromCheckpoint(ctx context.Context, dbPath string, opts gridstore.Options) (*gridstore.Store, string, error) {
	root := gridCheckpointRoot(dbPath)
	checkpoints, err := listGridCheckpoints(root)
	if err != nil {
		return nil, "", fmt.Errorf("gridstore: list checkpoints: %w", err)
	}
	if len(checkpoints) == 0 {
		return nil, "", errors.New("gridstore: no checkpoints available")
	}
	for _, checkpoint := range checkpoints {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		stats, err := gridstore.VerifyCheckpoint(ctx, checkpoint.path, gridCheckpointVerifyTimeout)
		if err != nil {
			log.Printf("Gridstore: checkpoint verify failed (%s): %v", checkpoint.path, err)
			continue
		}
		if stats.CountMetaErr != nil && !stats.CountMetaValid {
			log.Printf("Gridstore: checkpoint %s count metadata warning: %v", checkpoint.path, stats.CountMetaErr)
		}
		if err := restoreGridStoreFromPath(ctx, dbPath, checkpoint.path); err != nil {
			log.Printf("Gridstore: checkpoint restore failed (%s): %v", checkpoint.path, err)
			continue
		}
		store, err := gridstore.Open(dbPath, opts)
		if err != nil {
			log.Printf("Gridstore: open after restore failed (%s): %v", checkpoint.path, err)
			continue
		}
		return store, checkpoint.path, nil
	}
	return nil, "", errors.New("gridstore: no valid checkpoints")
}

func restoreGridStoreFromPath(ctx context.Context, dbPath string, checkpointPath string) error {
	if strings.TrimSpace(dbPath) == "" {
		return errors.New("gridstore: db path is empty")
	}
	if strings.TrimSpace(checkpointPath) == "" {
		return errors.New("gridstore: checkpoint path is empty")
	}
	if ctx == nil {
		return errors.New("gridstore: nil context")
	}
	parent := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	if strings.TrimSpace(base) == "" || strings.TrimSpace(parent) == "" {
		return fmt.Errorf("gridstore: invalid db path %q", dbPath)
	}
	ts := time.Now().UTC().Format(gridCheckpointNameLayoutUTC)
	tempDir := filepath.Join(parent, base+".restore-"+ts)
	backupDir := filepath.Join(parent, base+".backup-"+ts)
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return fmt.Errorf("gridstore: create restore dir: %w", err)
	}
	if err := copyDirCtx(ctx, checkpointPath, tempDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return err
	}
	if ctx.Err() != nil {
		_ = os.RemoveAll(tempDir)
		return ctx.Err()
	}

	hadExisting := false
	if info, err := os.Stat(dbPath); err == nil {
		if !info.IsDir() {
			_ = os.RemoveAll(tempDir)
			return fmt.Errorf("gridstore: db path %s is not a directory", dbPath)
		}
		hadExisting = true
	} else if !os.IsNotExist(err) {
		_ = os.RemoveAll(tempDir)
		return fmt.Errorf("gridstore: stat db path: %w", err)
	}

	if hadExisting {
		if err := os.Rename(dbPath, backupDir); err != nil {
			_ = os.RemoveAll(tempDir)
			return fmt.Errorf("gridstore: backup existing db: %w", err)
		}
	}
	if err := os.Rename(tempDir, dbPath); err != nil {
		if hadExisting {
			if rollbackErr := os.Rename(backupDir, dbPath); rollbackErr != nil {
				log.Printf("Gridstore: rollback restore failed: %v", rollbackErr)
			}
		}
		_ = os.RemoveAll(tempDir)
		return fmt.Errorf("gridstore: finalize restore dir: %w", err)
	}

	if hadExisting {
		backupCheckpoint := filepath.Join(backupDir, gridCheckpointDirName)
		newCheckpoint := filepath.Join(dbPath, gridCheckpointDirName)
		if _, err := os.Stat(backupCheckpoint); err == nil {
			if _, err := os.Stat(newCheckpoint); os.IsNotExist(err) {
				if err := os.Rename(backupCheckpoint, newCheckpoint); err != nil {
					log.Printf("Gridstore: checkpoint carry-forward failed: %v", err)
				}
			}
		}
		if err := os.RemoveAll(backupDir); err != nil {
			log.Printf("Gridstore: cleanup backup dir failed: %v", err)
		}
	}
	return nil
}

func copyDirCtx(ctx context.Context, src string, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFileCtx(ctx, path, target, info.Mode())
	})
}

func copyFileCtx(ctx context.Context, src string, dst string, mode os.FileMode) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(dst)
		}
	}()
	buf := make([]byte, 128*1024)
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		n, readErr := in.Read(buf)
		if n > 0 {
			if _, err = out.Write(buf[:n]); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	if err = out.Sync(); err != nil {
		return err
	}
	return nil
}

// Purpose: Load FCC ULS database stats for dashboard display.
// Key aspects: Opens SQLite, counts tables, and records file metadata.
// Upstream: displayStatsWithFCC.
// Downstream: sql.Open, db.QueryRow, os.Stat.
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
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&c); err != nil {
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

// Purpose: Convert a string into sql.NullString.
// Key aspects: Returns invalid when the input is empty.
// Upstream: startGridWriter batch creation.
// Downstream: sql.NullString initialization.
func sqlNullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

// formatMemoryLine reports memory-ish metrics in order:
// exec alloc / ring buffer / primary dedup (dup%) / secondary dedup (fast+med+slow) / call meta cache (hit%) / known calls (hit%).
// Purpose: Format the memory/status line for the stats pane.
// Key aspects: Reports ring buffer occupancy and cache hit stats.
// Upstream: displayStatsWithFCC.
// Downstream: buffer.RingBuffer stats and cache lookups.
func formatMemoryLine(buf *buffer.RingBuffer, dedup *dedup.Deduplicator, secondaryFast *dedup.SecondaryDeduper, secondaryMed *dedup.SecondaryDeduper, secondarySlow *dedup.SecondaryDeduper, metaCache *callMetaCache, knownPtr *atomic.Pointer[spot.KnownCallsigns]) string {
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
	if secondaryFast != nil {
		_, _, cacheSize := secondaryFast.GetStats()
		secondaryMB += bytesToMB(uint64(cacheSize * dedupeEntryBytes))
	}
	if secondaryMed != nil {
		_, _, cacheSize := secondaryMed.GetStats()
		secondaryMB += bytesToMB(uint64(cacheSize * dedupeEntryBytes))
	}
	if secondarySlow != nil {
		_, _, cacheSize := secondarySlow.GetStats()
		secondaryMB += bytesToMB(uint64(cacheSize * dedupeEntryBytes))
	}

	metaMB := 0.0
	metaRatio := 0.0
	if metaCache != nil {
		entries := metaCache.EntryCount()
		metaMB = bytesToMB(uint64(entries * callMetaEntryBytes))
		metrics := metaCache.CTYMetrics()
		if metrics.Lookups > 0 {
			metaRatio = float64(metrics.Hits) / float64(metrics.Lookups) * 100
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
		execMB, ringMB, dedupeMB, dedupeRatio, secondaryMB, metaMB, metaRatio, knownMB, knownRatio)
}

// Purpose: Format a human-readable uptime line.
// Key aspects: Uses days/hours/minutes formatting.
// Upstream: displayStatsWithFCC.
// Downstream: time.Duration math.
func formatUptimeLine(uptime time.Duration) string {
	hours := int(uptime.Hours())
	minutes := int(uptime.Minutes()) % 60
	return fmt.Sprintf("Uptime: %02d:%02d", hours, minutes)
}

func formatUptimeShort(uptime time.Duration) string {
	if uptime < 0 {
		uptime = -uptime
	}
	days := int(uptime / (24 * time.Hour))
	uptime -= time.Duration(days) * 24 * time.Hour
	hours := int(uptime / time.Hour)
	uptime -= time.Duration(hours) * time.Hour
	minutes := int(uptime / time.Minute)
	if days > 0 {
		return fmt.Sprintf("%dd %02d:%02d", days, hours, minutes)
	}
	return fmt.Sprintf("%02d:%02d", hours, minutes)
}

// Purpose: Format a short duration for stats display.
// Key aspects: Uses d/h/m/s units with coarse granularity.
// Upstream: CTY stats line formatting.
// Downstream: time.Duration math.
func formatDurationShort(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	days := int(d / (24 * time.Hour))
	d -= time.Duration(days) * 24 * time.Hour
	hours := int(d / time.Hour)
	d -= time.Duration(hours) * time.Hour
	minutes := int(d / time.Minute)
	d -= time.Duration(minutes) * time.Minute
	if days > 0 {
		return fmt.Sprintf("%dd%dh", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm", minutes)
	}
	seconds := int(d / time.Second)
	return fmt.Sprintf("%ds", seconds)
}

func formatDurationMillis(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func formatDateShortZ(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format("2006-01-02")
}

func formatPercent(numer, denom uint64) string {
	if denom == 0 {
		return "n/a"
	}
	pct := float64(numer) / float64(denom) * 100
	return fmt.Sprintf("%.1f%%", pct)
}

func percentValue(numer, denom uint64) float64 {
	if denom == 0 {
		return 0
	}
	return float64(numer) / float64(denom) * 100
}

func formatPercentString(pct float64) string {
	if pct <= 0 {
		return "0.0%"
	}
	if pct > 100 {
		pct = 100
	}
	return fmt.Sprintf("%.1f%%", pct)
}

func formatPercentBarWithLabel(pct float64, width int, label string) string {
	if width <= 0 {
		return "[]"
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(math.Round(float64(width) * pct / 100))
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	if width == 0 {
		return "[]"
	}
	return "[" + buildBarWithLabel(width, filled, label) + "]"
}

func buildBarWithLabel(width, filled int, label string) string {
	if width <= 0 {
		return ""
	}
	if label == "" {
		return buildBarSegment(0, width, filled)
	}
	labelRunes := []rune(label)
	if len(labelRunes) > width {
		labelRunes = labelRunes[:width]
	}
	labelLen := len(labelRunes)
	labelStart := 1
	labelEnd := labelStart + labelLen
	if labelEnd > width {
		labelStart = 0
		labelEnd = labelLen
	}

	var b strings.Builder
	for i := 0; i < width; i++ {
		if i == labelStart {
			b.WriteString("[black:white]")
			b.WriteString(string(labelRunes))
			b.WriteString("[-:-]")
			i = labelEnd - 1
			continue
		}
		if i < filled {
			b.WriteString("[white:white] [-:-]")
		} else {
			b.WriteString("[-:-] ")
		}
	}
	return b.String()
}

func buildBarSegment(start, end, filled int) string {
	var b strings.Builder
	for i := start; i < end; i++ {
		if i < filled {
			b.WriteString("[white:white] [-:-]")
		} else {
			b.WriteString("[-:-] ")
		}
	}
	return b.String()
}

func buildOverviewLines(
	tracker *stats.Tracker,
	dedup *dedup.Deduplicator,
	secondaryFast *dedup.SecondaryDeduper,
	secondaryMed *dedup.SecondaryDeduper,
	secondarySlow *dedup.SecondaryDeduper,
	metaCache *callMetaCache,
	knownPtr *atomic.Pointer[spot.KnownCallsigns],
	recentBandStore *spot.RecentBandStore,
	ctyState *ctyRefreshState,
	knownCallsPath string,
	fccSnap *fccSnapshot,
	gridStats *gridMetrics,
	gridDB *gridStoreHandle,
	pathPredictor *pathreliability.Predictor,
	telnetSrv *telnet.Server,
	clusterCall string,
	rbnLive, pskLive, p92Live bool,
	rbnTotal, rbnCW, rbnRTTY, rbnFT8, rbnFT4 uint64,
	pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskMSK144 uint64,
	p92Total uint64,
	totalCorrections, totalUnlicensed, totalHarmonics, reputationTotal uint64,
	pathOnlyLine string,
	skewPath string,
	mem *runtime.MemStats,
	gcP99Label string,
) []string {
	now := time.Now().UTC()
	if mem == nil {
		mem = &runtime.MemStats{}
		runtime.ReadMemStats(mem)
	}
	heap := humanize.Bytes(mem.HeapAlloc)
	sys := humanize.Bytes(mem.Sys)
	lastGC := time.Duration(0)
	if mem.LastGC > 0 {
		lastGC = now.Sub(time.Unix(0, int64(mem.LastGC)))
	}
	if strings.TrimSpace(gcP99Label) == "" {
		gcP99Label = "n/a"
	}
	uptime := time.Duration(0)
	if tracker != nil {
		uptime = tracker.GetUptime()
	}

	gridLookups := uint64(0)
	gridHits := uint64(0)
	if gridStats != nil {
		gridLookups = gridStats.cacheLookups.Load()
		gridHits = gridStats.cacheHits.Load()
	}
	metaLookups := uint64(0)
	metaHits := uint64(0)
	metaCount := 0
	if metaCache != nil {
		metaCount = metaCache.EntryCount()
		metaMetrics := metaCache.CTYMetrics()
		metaLookups = metaMetrics.Lookups
		metaHits = metaMetrics.Hits
	}

	var knownCount int
	var knownLookups, knownHits uint64
	if knownPtr != nil {
		if known := knownPtr.Load(); known != nil {
			knownCount = known.Count()
			knownLookups, knownHits = known.Stats()
		}
	}

	gridCount := int64(-1)
	if gridDB != nil {
		if store := gridDB.Store(); store != nil {
			if count, err := store.Count(); err == nil {
				gridCount = count
			}
		}
	}
	gridHitPct := percentValue(gridHits, gridLookups)
	metaHitPct := percentValue(metaHits, metaLookups)
	knownHitPct := percentValue(knownHits, knownLookups)
	recentOnBandLabel := "n/a"
	if recentBandStore != nil {
		recentOnBandLabel = humanize.Comma(int64(recentBandStore.ActiveCallCount(now)))
	}

	gridSizeLabel := humanize.Comma(gridCount)
	metaSizeLabel := humanize.Comma(int64(metaCount))
	knownSizeLabel := humanize.Comma(int64(knownCount))
	cacheBars := []string{
		fmt.Sprintf("[yellow]Grid cache[-]:  %s %s", formatPercentBarWithLabel(gridHitPct, 20, gridSizeLabel), formatPercentString(gridHitPct)),
		fmt.Sprintf("[yellow]Meta cache[-]:  %s %s", formatPercentBarWithLabel(metaHitPct, 20, metaSizeLabel), formatPercentString(metaHitPct)),
		fmt.Sprintf("[yellow]Known calls[-]: %s %s", formatPercentBarWithLabel(knownHitPct, 20, knownSizeLabel), formatPercentString(knownHitPct)),
		"",
	}
	cacheBars = append(cacheBars, formatRecentOnBandLines(recentBandStore, now, recentOnBandLabel)...)

	ctyTime := "n/a"
	if ctyState != nil {
		if ts, ok := ctyState.lastSuccessTime(); ok {
			ctyTime = formatDateShortZ(ts)
		}
	}

	scpTime := "n/a"
	if knownCallsPath != "" {
		if info, err := os.Stat(knownCallsPath); err == nil {
			scpTime = formatDateShortZ(info.ModTime())
		}
	}

	fccTime := "n/a"
	if fccSnap != nil && !fccSnap.UpdatedAt.IsZero() {
		fccTime = formatDateShortZ(fccSnap.UpdatedAt)
	}

	skewTime := "n/a"
	if strings.TrimSpace(skewPath) != "" {
		if info, err := os.Stat(skewPath); err == nil {
			skewTime = formatDateShortZ(info.ModTime())
		}
	}

	primaryDupPct := "n/a"
	if dedup != nil {
		processed, duplicates, _ := dedup.GetStats()
		if processed > 0 && duplicates <= processed {
			primaryDupPct = formatPercent(duplicates, processed)
		}
	}
	secondarySummary := "F-- M-- S--"
	if secondaryFast != nil || secondaryMed != nil || secondarySlow != nil {
		secondarySummary = fmt.Sprintf("F%s M%s S%s",
			formatSecondaryPercent(secondaryFast),
			formatSecondaryPercent(secondaryMed),
			formatSecondaryPercent(secondarySlow),
		)
	}

	var clientList []string
	if telnetSrv != nil {
		clientList = telnetSrv.ListClientCallsigns()
	}

	if strings.TrimSpace(clusterCall) == "" {
		clusterCall = "unknown"
	}

	lines := make([]string, 0, 16+len(cacheBars))
	lines = append(lines,
		fmt.Sprintf("[yellow]Cluster[-]: %s  [yellow]Version[-]: %s  [yellow]Uptime[-]: %s", clusterCall, Version, formatUptimeShort(uptime)),
		"MEMORY / GC",
		fmt.Sprintf("[yellow]Heap[-]: %s  [yellow]Sys[-]: %s  [yellow]GC p99 (interval)[-]: %s  [yellow]Last GC[-]: %s ago  [yellow]Goroutines[-]: %d", heap, sys, gcP99Label, formatDurationShort(lastGC), runtime.NumGoroutine()),
		"INGEST RATES (per min)",
		formatIngestLine(withIngestStatusLabel("[yellow]RBN[-]", rbnLive), rbnTotal, rbnCW, rbnRTTY, rbnFT8, rbnFT4, 0, false),
		formatIngestLine(withIngestStatusLabel("[yellow]PSK[-]", pskLive), pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskMSK144, true),
		fmt.Sprintf("%s: %s", withIngestStatusLabel("[yellow]P92[-]", p92Live), humanize.Comma(int64(p92Total))),
		pathOnlyLine,
		fmt.Sprintf("[yellow]Primary Dedupe[-]: %s | [yellow]Secondary[-]: %s", primaryDupPct, secondarySummary),
		fmt.Sprintf("[yellow]Corrections[-]: %s | [yellow]Unlicensed[-]: %s | [yellow]Harmonics[-]: %s | [yellow]Reputation[-]: %s",
			humanize.Comma(int64(totalCorrections)),
			humanize.Comma(int64(totalUnlicensed)),
			humanize.Comma(int64(totalHarmonics)),
			humanize.Comma(int64(reputationTotal)),
		),
		fmt.Sprintf("[yellow]Stabilizer[-]: %s", strings.TrimPrefix(formatStabilizerSummary(tracker), "Stabilizer: ")),
		"CACHES & DATA FRESHNESS",
	)
	lines = append(lines, cacheBars...)
	lines = append(lines,
		"",
		fmt.Sprintf("[yellow]CTY[-]: %s  [yellow]SCP[-]: %s  [yellow]FCC[-]: %s  [yellow]Skew[-]: %s", ctyTime, scpTime, fccTime, skewTime),
		"PATH PREDICTIONS",
	)
	lines = append(lines, formatPathLines(pathPredictor, now)...)
	lines = append(lines, "NETWORK")
	lines = append(lines, formatNetworkLines(telnetSrv, clientList)...)
	return lines
}

func formatNetworkSummaryLine(telnetSrv *telnet.Server) string {
	if telnetSrv == nil {
		return "[yellow]Telnet[-]: 0 clients   [yellow]Drops[-]: Q0 C0 W0"
	}
	queueDrops, clientDrops, senderFailures := telnetSrv.BroadcastMetricSnapshot()
	clientCount := telnetSrv.GetClientCount()
	return fmt.Sprintf("[yellow]Telnet[-]: %d clients   [yellow]Drops[-]: Q%d C%d W%d",
		clientCount, queueDrops, clientDrops, senderFailures,
	)
}

func formatNetworkLatencyLines(telnetSrv *telnet.Server) []string {
	if telnetSrv == nil {
		return []string{"[yellow]Latency[-]: enq n/a  first n/a", "[yellow]Write stall[-]: n/a"}
	}
	enq, first, stall := telnetSrv.LatencySnapshots()
	formatPair := func(s telnet.LatencySnapshot) string {
		if s.N == 0 {
			return "n/a"
		}
		return fmt.Sprintf("p50 %s p99 %s", formatDurationMillis(s.P50), formatDurationMillis(s.P99))
	}
	return []string{
		fmt.Sprintf("[yellow]Latency[-]: enq %s  first %s", formatPair(enq), formatPair(first)),
		fmt.Sprintf("[yellow]Write stall[-]: %s", formatPair(stall)),
	}
}

func formatNetworkLines(telnetSrv *telnet.Server, clientList []string) []string {
	latencyLines := formatNetworkLatencyLines(telnetSrv)
	clientLines := formatClientListLines(clientList)
	lines := make([]string, 0, 1+len(latencyLines)+len(clientLines))
	lines = append(lines, formatNetworkSummaryLine(telnetSrv))
	lines = append(lines, latencyLines...)
	lines = append(lines, clientLines...)
	return lines
}

func padRight(s string, width int) string {
	if width <= 0 {
		return s
	}
	if visibleLen(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-visibleLen(s))
}

func padLeft(s string, width int) string {
	if width <= 0 {
		return s
	}
	if visibleLen(s) >= width {
		return s
	}
	return strings.Repeat(" ", width-visibleLen(s)) + s
}

func visibleLen(s string) int {
	if s == "" {
		return 0
	}
	count := 0
	for i := 0; i < len(s); {
		if s[i] == '[' {
			if j := strings.IndexByte(s[i:], ']'); j >= 0 {
				i += j + 1
				continue
			}
		}
		count++
		i++
	}
	return count
}

func formatIngestLine(label string, total, cw, rtty, ft8, ft4, msk uint64, includeMSK bool) string {
	totalStr := padRight(humanize.Comma(int64(total)), 7)
	fields := []string{
		fmt.Sprintf("%s: %s", label, totalStr),
		formatIngestField("[yellow]CW[-]", cw),
		formatIngestField("[yellow]RTTY[-]", rtty),
		formatIngestField("[yellow]FT8[-]", ft8),
		formatIngestField("[yellow]FT4[-]", ft4),
	}
	if includeMSK {
		fields = append(fields, formatIngestField("[yellow]MSK[-]", msk))
	}
	return strings.Join(fields, " | ")
}

func formatIngestField(label string, value uint64) string {
	val := padRight(humanize.Comma(int64(value)), 6)
	return fmt.Sprintf("%s %s", label, val)
}

func withIngestStatusLabel(label string, live bool) string {
	return fmt.Sprintf("%s %s", label, ingestStatusMarker(live))
}

func ingestStatusMarker(live bool) string {
	if live {
		return "[green]ON[-]"
	}
	return "[red]OFF[-]"
}

func formatPathLines(predictor *pathreliability.Predictor, now time.Time) []string {
	const (
		colsPerRow = 4
	)
	lines := make([]string, 0, 1)
	if predictor == nil || !predictor.Config().Enabled {
		lines = append(lines, "[yellow]Path pairs[-]: n/a")
		return lines
	}
	stats := predictor.Stats(now)
	lines = append(lines, fmt.Sprintf("[yellow]Path pairs[-]: %s (L2) / %s (L1)",
		humanize.Comma(int64(stats.CombinedFine)),
		humanize.Comma(int64(stats.CombinedCoarse)),
	))
	lines = append(lines, "")
	bands := predictor.StatsByBand(now)
	if len(bands) == 0 {
		return lines
	}
	maxBand := 0
	maxFine := 0
	maxCoarse := 0
	for _, entry := range bands {
		if len(entry.Band) > maxBand {
			maxBand = len(entry.Band)
		}
		fineStr := humanize.Comma(int64(entry.Fine))
		coarseStr := humanize.Comma(int64(entry.Coarse))
		if len(fineStr) > maxFine {
			maxFine = len(fineStr)
		}
		if len(coarseStr) > maxCoarse {
			maxCoarse = len(coarseStr)
		}
	}
	rows := (len(bands) + colsPerRow - 1) / colsPerRow
	cols := make([]string, 0, len(bands))
	for r := 0; r < rows; r++ {
		cols = cols[:0]
		for c := 0; c < colsPerRow; c++ {
			idx := c*rows + r
			if idx >= len(bands) {
				continue
			}
			entry := bands[idx]
			bandCol := padLeft(entry.Band, maxBand)
			fineCol := padLeft(humanize.Comma(int64(entry.Fine)), maxFine)
			coarseCol := padLeft(humanize.Comma(int64(entry.Coarse)), maxCoarse)
			col := fmt.Sprintf("[yellow]%s[-]: %s / %s", bandCol, fineCol, coarseCol)
			cols = append(cols, col)
		}
		if len(cols) == 0 {
			continue
		}
		if len(cols) == 1 {
			lines = append(lines, cols[0])
			continue
		}
		colWidth := 0
		for _, col := range cols {
			if w := visibleLen(col); w > colWidth {
				colWidth = w
			}
		}
		colWidth += 2
		var b strings.Builder
		for i, col := range cols {
			if i < len(cols)-1 {
				b.WriteString(padRight(col, colWidth))
			} else {
				b.WriteString(col)
			}
		}
		lines = append(lines, b.String())
	}
	return lines
}

func formatRecentOnBandLines(store *spot.RecentBandStore, now time.Time, totalLabel string) []string {
	lines := []string{fmt.Sprintf("[yellow]Recent on band[-]: %s", totalLabel)}
	if store == nil {
		return lines
	}
	counts := store.ActiveCallCountsByBand(now)
	if len(counts) == 0 {
		return lines
	}
	bandOrder := spot.SupportedBandNames()
	seen := make(map[string]struct{}, len(bandOrder))
	entries := make([]struct {
		band  string
		count int
	}, 0, len(counts))
	for _, band := range bandOrder {
		count := counts[band]
		if count <= 0 {
			continue
		}
		entries = append(entries, struct {
			band  string
			count int
		}{band: band, count: count})
		seen[band] = struct{}{}
	}
	for band, count := range counts {
		if count <= 0 {
			continue
		}
		if _, ok := seen[band]; ok {
			continue
		}
		entries = append(entries, struct {
			band  string
			count int
		}{band: band, count: count})
	}
	if len(entries) == 0 {
		return lines
	}
	const colsPerRow = 6
	maxBand := 0
	maxCount := 0
	for _, entry := range entries {
		if len(entry.band) > maxBand {
			maxBand = len(entry.band)
		}
		countStr := humanize.Comma(int64(entry.count))
		if len(countStr) > maxCount {
			maxCount = len(countStr)
		}
	}
	rows := (len(entries) + colsPerRow - 1) / colsPerRow
	cols := make([]string, 0, colsPerRow)
	for r := 0; r < rows; r++ {
		cols = cols[:0]
		for c := 0; c < colsPerRow; c++ {
			idx := c*rows + r
			if idx >= len(entries) {
				continue
			}
			entry := entries[idx]
			bandCol := padLeft(entry.band, maxBand)
			countCol := padLeft(humanize.Comma(int64(entry.count)), maxCount)
			cols = append(cols, fmt.Sprintf("[yellow]%s[-]: %s", bandCol, countCol))
		}
		if len(cols) == 0 {
			continue
		}
		if len(cols) == 1 {
			lines = append(lines, cols[0])
			continue
		}
		colWidth := 0
		for _, col := range cols {
			if w := visibleLen(col); w > colWidth {
				colWidth = w
			}
		}
		colWidth += 2
		var b strings.Builder
		for i, col := range cols {
			if i < len(cols)-1 {
				b.WriteString(padRight(col, colWidth))
			} else {
				b.WriteString(col)
			}
		}
		lines = append(lines, b.String())
	}
	return lines
}

func formatClientListLines(calls []string) []string {
	if len(calls) == 0 {
		return []string{""}
	}
	const (
		colWidth = 14
		maxRows  = 10
	)
	colsPerRow := clientListColumnsPerRow(colWidth)
	lines := make([]string, 0, 1)
	lines = append(lines, "")
	rows := (len(calls) + colsPerRow - 1) / colsPerRow
	overflow := 0
	maxItems := maxRows * colsPerRow
	if len(calls) > maxItems {
		overflow = len(calls) - maxItems
	}
	for r := 0; r < rows; r++ {
		cols := make([]string, 0, colsPerRow)
		for c := 0; c < colsPerRow; c++ {
			idx := r*colsPerRow + c
			if idx >= len(calls) {
				continue
			}
			cols = append(cols, calls[idx])
		}
		if len(cols) == 0 {
			continue
		}
		if len(cols) == 1 {
			lines = append(lines, cols[0])
			continue
		}
		var b strings.Builder
		for i, col := range cols {
			if i < len(cols)-1 {
				b.WriteString(padRight(col, colWidth))
			} else {
				b.WriteString(col)
			}
		}
		lines = append(lines, b.String())
	}
	if overflow > 0 {
		lines = append(lines, fmt.Sprintf("... +%d more", overflow))
	}
	return lines
}

func clientListColumnsPerRow(colWidth int) int {
	const (
		maxCols          = 6
		minCols          = 1
		borderAndPadding = 3
	)
	if colWidth <= 0 {
		return maxCols
	}
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width <= 0 {
		return maxCols
	}
	usable := width - borderAndPadding
	if usable < colWidth {
		return minCols
	}
	cols := usable / colWidth
	if cols < minCols {
		cols = minCols
	}
	if cols > maxCols {
		cols = maxCols
	}
	return cols
}

func formatSecondaryPercent(d *dedup.SecondaryDeduper) string {
	if d == nil {
		return "--"
	}
	processed, duplicates, _ := d.GetStats()
	if processed == 0 || duplicates > processed {
		return "0%"
	}
	return fmt.Sprintf("%.0f%%", float64(processed-duplicates)/float64(processed)*100)
}

// wwvKindFromLine tags non-DX lines coming from human/relay telnet ingest.
// We only forward WWV/WCY bulletins to telnet clients; upstream keepalives,
// prompts, or other control chatter (e.g., "de N2WQ-22" banners) are dropped.
// Purpose: Extract WWV/WCY bulletin kind token from a raw line.
// Key aspects: Uppercases and trims for display selection.
// Upstream: WWV handling in peer/telnet paths.
// Downstream: strings.TrimSpace/ToUpper.
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
// Purpose: Extract announcement text from a PC93 line.
// Key aspects: Strips known prefix and trims whitespace.
// Upstream: PC93 announcement parsing.
// Downstream: strings.TrimSpace.
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

// Purpose: Compute per-interval delta for a counter map entry.
// Key aspects: Updates previous map in-place.
// Upstream: displayStatsWithFCC.
// Downstream: map access/mutation.
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

// Purpose: Compute per-interval delta for a monotonic counter.
// Key aspects: Returns current when counter resets.
// Upstream: path-only stats deltas.
// Downstream: arithmetic only.
func diffCounterRaw(current, previous uint64) uint64 {
	if current >= previous {
		return current - previous
	}
	return current
}

// Purpose: Normalize allowed bands list and build a fast lookup set.
// Key aspects: Filters invalid bands and preserves canonical ordering.
// Upstream: Path reliability predictor setup.
// Downstream: processOutputSpots and path-only ingest gates.
func normalizeAllowedBands(raw []string) ([]string, map[string]struct{}) {
	allowed := make(map[string]struct{}, len(raw))
	for _, band := range raw {
		normalized := spot.NormalizeBand(band)
		if normalized == "" || !spot.IsValidBand(normalized) {
			continue
		}
		allowed[normalized] = struct{}{}
	}
	if len(allowed) == 0 {
		all := spot.SupportedBandNames()
		for _, band := range all {
			allowed[spot.NormalizeBand(band)] = struct{}{}
		}
		return all, allowed
	}
	out := make([]string, 0, len(allowed))
	for _, band := range spot.SupportedBandNames() {
		normalized := spot.NormalizeBand(band)
		if _, ok := allowed[normalized]; ok {
			out = append(out, band)
		}
	}
	return out, allowed
}

func allowedBand(allowed map[string]struct{}, band string) bool {
	if len(allowed) == 0 {
		return true
	}
	normalized := spot.NormalizeBand(band)
	if normalized == "" {
		return false
	}
	_, ok := allowed[normalized]
	return ok
}

// Purpose: Compute per-interval delta for a source+mode counter.
// Key aspects: Uses sourceModeKey to access map keys.
// Upstream: displayStatsWithFCC.
// Downstream: sourceModeKey and map mutation.
func diffSourceMode(current, previous map[string]uint64, source, mode string) uint64 {
	key := sourceModeKey(source, mode)
	return diffCounter(current, previous, key)
}

// Purpose: Report whether both RBN feeds are connected.
// Key aspects: Returns false if either client is nil or disconnected.
// Upstream: stats ticker liveness.
// Downstream: rbn.Client.HealthSnapshot.
func rbnFeedsLive(rbnClient *rbn.Client, rbnDigital *rbn.Client) bool {
	if rbnClient == nil || rbnDigital == nil {
		return false
	}
	return rbnClient.HealthSnapshot().Connected && rbnDigital.HealthSnapshot().Connected
}

// Purpose: Report whether PSKReporter is connected and delivering messages recently.
// Key aspects: Requires Connected plus recent payload activity within the idle threshold.
// Upstream: stats ticker liveness.
// Downstream: pskreporter.HealthSnapshot timestamps.
func pskReporterLive(snap pskreporter.HealthSnapshot, now time.Time) bool {
	if !snap.Connected || snap.LastPayloadAt.IsZero() {
		return false
	}
	return now.Sub(snap.LastPayloadAt) <= ingestIdleThreshold
}

// Purpose: Compute per-interval RBN ingest deltas (CW/RTTY + FT8/FT4).
// Key aspects: Reads FT counts from the "RBN-FT" source label.
// Upstream: displayStatsWithFCC stats ticker.
// Downstream: diffCounter and diffSourceMode.
func rbnIngestDeltas(
	sourceTotals, prevSourceCounts map[string]uint64,
	sourceModeTotals, prevSourceModeCounts map[string]uint64,
) (rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4 uint64) {
	rbnTotal = diffCounter(sourceTotals, prevSourceCounts, "RBN")
	rbnCW = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "CW")
	rbnRTTY = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "RTTY")

	rbnFTTotal = diffCounter(sourceTotals, prevSourceCounts, "RBN-FT")
	rbnFT8 = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-FT", "FT8")
	rbnFT4 = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-FT", "FT4")
	return
}

// Purpose: Build a stable key for source+mode counters.
// Key aspects: Uppercases and concatenates with a delimiter.
// Upstream: diffSourceMode.
// Downstream: strings.ToUpper.
func sourceModeKey(source, mode string) string {
	source = strings.ToUpper(strings.TrimSpace(source))
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if source == "" || mode == "" {
		return ""
	}
	return source + sourceModeDelimiter + mode
}

// Purpose: Convert bytes to megabytes (MB).
// Key aspects: Uses base-10 MB.
// Upstream: formatMemoryLine.
// Downstream: float math.
func bytesToMB(b uint64) float64 {
	return float64(b) / (1024.0 * 1024.0)
}

// Purpose: Parse the block profiling rate from an env string.
// Key aspects: Accepts Go duration (e.g., "10ms") or integer nanoseconds; rejects negatives.
// Upstream: maybeStartContentionProfiling.
// Downstream: runtime.SetBlockProfileRate.
func parseBlockProfileRate(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty value")
	}
	raw = strings.Join(strings.Fields(raw), "")
	if dur, err := time.ParseDuration(raw); err == nil {
		if dur < 0 {
			return 0, fmt.Errorf("must be >= 0")
		}
		return dur, nil
	}
	nanos, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration (use Go duration like 10ms or integer nanoseconds)")
	}
	if nanos < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return time.Duration(nanos), nil
}

// Purpose: Parse the mutex profiling fraction from an env string.
// Key aspects: Accepts integer fraction (1/N) and allows 0 to disable.
// Upstream: maybeStartContentionProfiling.
// Downstream: runtime.SetMutexProfileFraction.
func parseMutexProfileFraction(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty value")
	}
	fraction, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid fraction (use integer >= 0)")
	}
	if fraction < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return fraction, nil
}

// maybeStartContentionProfiling enables block/mutex profiling when env vars are set.
// Purpose: Allow opt-in contention profiling without recompiling.
// Key aspects: Logs effective settings and keeps defaults when env vars are empty/invalid.
// Upstream: main startup.
// Downstream: runtime.SetBlockProfileRate and runtime.SetMutexProfileFraction.
func maybeStartContentionProfiling() {
	blockRaw := strings.TrimSpace(os.Getenv(envBlockProfileRate))
	mutexRaw := strings.TrimSpace(os.Getenv(envMutexProfileFraction))
	if blockRaw == "" && mutexRaw == "" {
		return
	}

	if blockRaw != "" {
		rate, err := parseBlockProfileRate(blockRaw)
		if err != nil {
			log.Printf("Contention profiling: ignoring invalid %s=%q: %v", envBlockProfileRate, blockRaw, err)
		} else {
			runtime.SetBlockProfileRate(int(rate.Nanoseconds()))
			if rate <= 0 {
				log.Printf("Contention profiling: block profile disabled (%s=%q)", envBlockProfileRate, blockRaw)
			} else {
				log.Printf("Contention profiling: block profile enabled (rate=%s)", rate)
			}
		}
	}

	if mutexRaw != "" {
		fraction, err := parseMutexProfileFraction(mutexRaw)
		if err != nil {
			log.Printf("Contention profiling: ignoring invalid %s=%q: %v", envMutexProfileFraction, mutexRaw, err)
		} else {
			runtime.SetMutexProfileFraction(fraction)
			if fraction <= 0 {
				log.Printf("Contention profiling: mutex profile disabled (%s=%q)", envMutexProfileFraction, mutexRaw)
			} else {
				log.Printf("Contention profiling: mutex profile enabled (fraction=1/%d)", fraction)
			}
		}
	}
}

// maybeStartHeapLogger starts periodic heap logging when DXC_HEAP_LOG_INTERVAL is set
// (e.g., "60s"). Defaults to disabled when the variable is empty or invalid.
// Purpose: Optionally start a periodic heap profile logger.
// Key aspects: Controlled by environment variables.
// Upstream: main startup.
// Downstream: pprof.WriteHeapProfile and time.NewTicker.
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
	// Purpose: Emit periodic heap stats to the log.
	// Key aspects: Runs on ticker cadence until process exit.
	// Upstream: maybeStartHeapLogger.
	// Downstream: runtime.ReadMemStats and log.Printf.
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

// maybeStartMapLogger starts periodic map size logging when DXC_MAP_LOG_INTERVAL is set.
// Purpose: Provide opt-in visibility into map growth for GC diagnostics.
// Key aspects: Logs bounded cardinalities; no impact when disabled.
// Upstream: main startup.
// Downstream: tracker cardinality, dedup stats, predictor bucket counts.
func maybeStartMapLogger(tracker *stats.Tracker, predictor *pathreliability.Predictor, dedup *dedup.Deduplicator, secondaryFast, secondaryMed, secondarySlow *dedup.SecondaryDeduper) {
	intervalStr := strings.TrimSpace(os.Getenv(envMapLogInterval))
	if intervalStr == "" {
		return
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil || interval <= 0 {
		log.Printf("Map logger disabled (invalid %s=%q)", envMapLogInterval, intervalStr)
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		log.Printf("Map logger enabled (every %s)", interval)
		for range ticker.C {
			cq := spot.GlobalCallQualityCounts()
			sourceCount := 0
			sourceModeCount := 0
			if tracker != nil {
				sourceCount = tracker.SourceCardinality()
				sourceModeCount = tracker.SourceModeCardinality()
			}

			primarySize := 0
			if dedup != nil {
				_, _, primarySize = dedup.GetStats()
			}
			fastSize := 0
			if secondaryFast != nil {
				_, _, fastSize = secondaryFast.GetStats()
			}
			medSize := 0
			if secondaryMed != nil {
				_, _, medSize = secondaryMed.GetStats()
			}
			slowSize := 0
			if secondarySlow != nil {
				_, _, slowSize = secondarySlow.GetStats()
			}

			pathBuckets := 0
			if predictor != nil {
				pathBuckets = predictor.TotalBuckets()
			}

			log.Printf("Map sizes: callQuality pinned=%d dynamic=%d; stats sources=%d source-modes=%d; dedup primary=%d secondary fast=%d med=%d slow=%d; path buckets=%d",
				cq.Pinned, cq.Dynamic, sourceCount, sourceModeCount, primarySize, fastSize, medSize, slowSize, pathBuckets)
		}
	}()
}

// maybeStartDiagServer exposes /debug/pprof/* and /debug/heapdump when DXC_PPROF_ADDR is set
// (example: DXC_PPROF_ADDR=localhost:6061). Default is off.
// Purpose: Optionally start the pprof/diagnostic HTTP server.
// Key aspects: Reads env vars and starts http server in background.
// Upstream: main startup.
// Downstream: http.ListenAndServe and net/http/pprof.
func maybeStartDiagServer() {
	addr := strings.TrimSpace(os.Getenv("DXC_PPROF_ADDR"))
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	// Purpose: Serve a heap dump endpoint that writes a pprof file to disk.
	// Key aspects: Creates diagnostics dir, forces GC, and writes heap profile.
	// Upstream: HTTP /debug/heapdump request.
	// Downstream: os.MkdirAll, os.Create, pprof.WriteHeapProfile.
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

	// Purpose: Run the diagnostics HTTP server.
	// Key aspects: Logs startup and reports server errors.
	// Upstream: maybeStartDiagServer.
	// Downstream: http.Server with explicit timeouts.
	go func() {
		log.Printf("Diagnostics server listening on %s (pprof + /debug/heapdump)", addr)
		srv := &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Diagnostics server error: %v", err)
		}
	}()
}
