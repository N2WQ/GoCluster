// File role: Owns live cluster runtime resources, startup wiring, and shutdown.
// Crawler notes: Start here to understand which config-loaded services are
// constructed for the production binary, how ingest/telnet/archive/peer paths
// are connected, and which resources must close in signal-driven order.
// Related docs: README.md, docs/domain-contract.md, docs/dev-runbook.md.
// Related tests: internal/cluster/main_runtime_test.go, internal/cluster/main_test.go.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/dxsummit"
	"dxcluster/filter"
	"dxcluster/floodcontrol"
	"dxcluster/gridstore"
	"dxcluster/internal/toxicity"
	"dxcluster/internal/voacap"
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
)

// clusterRuntime owns startup-built resources for the live cluster binary.
// Invariant: initialize preserves the historical startup order, waitForShutdown
// performs the explicit signal-driven stops, and close replays the former defer
// chain order for background services and durable resources.
type clusterRuntime struct {
	versionInfo       BuildInfo
	cfg               *config.Config
	configSource      string
	configDiagnostics config.LoadDiagnostics
	startupErr        error

	logMux            *logFanout
	propagationLog    lineSink
	droppedCallLogger *droppedCallLogger
	eventFileLogger   *eventFileLogger
	surface           ui.Surface

	ctx    context.Context
	cancel context.CancelFunc

	pathCfg        pathreliability.Config
	pathPredictor  *pathreliability.Predictor
	voacapSSN      *voacap.SunspotMonitor
	voacapFallback *pathreliability.VOACAPClosedFallback
	allowedBandSet map[string]struct{}

	solarCfg solarweather.Config
	solarMgr *solarweather.Manager

	propScheduler *propReportScheduler

	metaCache *callMetaCache

	ctyDB    atomic.Pointer[cty.CTYDatabase]
	ctyState *ctyRefreshState

	spotterReliability     spot.SpotterReliability
	spotterReliabilityCW   spot.SpotterReliability
	spotterReliabilityRTTY spot.SpotterReliability
	confusionModel         *spot.ConfusionModel

	adaptiveMinReports *spot.AdaptiveMinReports
	refresher          *adaptiveRefresher

	statsTracker       *stats.Tracker
	dropLogDeduper     *dropLogDeduper
	dropReporter       func(string)
	unlicensedReporter func(source, role, call, deCall, dxCall, mode string, freq float64)

	repGate         *reputation.Gate
	repDropReporter func(reputation.DropEvent)

	spotBuffer     *buffer.RingBuffer
	signalResolver *spot.SignalResolver

	recentBandStore spot.RecentSupportStore
	customSCPStore  *spot.CustomSCPStore
	whoSpotsMeStore *spot.WhoSpotsMeStore

	gridStoreHandle   *gridStoreHandle
	gridDBCheckOnMiss bool
	gridDBCheckSource string
	gridUpdater       func(call, grid string)
	ctyUpdater        func(call string, info *cty.PrefixInfo)
	gridUpdateState   *gridMetrics
	stopGridWriter    func()
	gridLookup        func(call string) (string, bool, bool)
	gridLookupSync    func(call string) (string, bool, bool)

	skewStore *skew.Store

	freqAverager     *spot.FrequencyAverager
	harmonicDetector *spot.HarmonicDetector

	deduplicator    *dedup.Deduplicator
	floodController *floodcontrol.Controller
	ingestValidator *ingestValidator
	ingestInput     chan<- *spot.Spot

	secondaryFast *dedup.SecondaryDeduper
	secondaryMed  *dedup.SecondaryDeduper
	secondarySlow *dedup.SecondaryDeduper

	modeAssigner       *spot.ModeAssigner
	toxicityClassifier *toxicity.Classifier

	peerManager   *peer.Manager
	archiveWriter *archive.Writer
	processor     *commands.Processor
	telnetServer  *telnet.Server

	lastOutput          atomic.Int64
	secondaryStageCount atomic.Uint64

	pathReport        *pathReportMetrics
	pskrPathOnlyStats *pathOnlyStats

	rbnClient         *rbn.Client
	rbnDigitalClient  *rbn.Client
	humanTelnetClient *rbn.Client

	dxsummitClient *dxsummit.Client
	pskrClient     *pskreporter.Client
	pskrTopics     []string
}

func newClusterRuntime(versionInfo BuildInfo, cfg *config.Config, configSource string, diagnostics config.LoadDiagnostics) *clusterRuntime {
	return &clusterRuntime{
		versionInfo:       versionInfo,
		cfg:               cfg,
		configSource:      configSource,
		configDiagnostics: diagnostics,
		pathReport:        newPathReportMetrics(),
		pskrPathOnlyStats: &pathOnlyStats{},
		gridStoreHandle:   newGridStoreHandle(nil),
	}
}

func (r *clusterRuntime) failStartup(format string, args ...any) bool {
	message := fmt.Sprintf(format, args...)
	log.Print(message)
	r.startupErr = errors.New(message)
	return false
}

func (r *clusterRuntime) initialize() bool {
	if !r.setupLoggingAndUI() {
		return false
	}
	r.startBackgroundServices()
	if !r.loadYAMLReferenceTables() {
		return false
	}
	r.initializeCallCacheAndMeta()
	if !r.initializeReferenceData() {
		return false
	}
	r.initializePipelineCore()
	if !r.initializeServices() {
		return false
	}
	r.connectFeeds()
	r.startMonitors()
	r.logStartup()
	return true
}

func (r *clusterRuntime) setupLoggingAndUI() bool {
	logMux, logErr := setupLogging(r.cfg.Logging, os.Stdout)
	r.logMux = logMux
	// logFanout handles timestamp formatting for each sink.
	log.SetFlags(0)
	log.SetOutput(logMux)
	if logErr != nil {
		log.Printf("Logging: %v", logErr)
	}
	droppedCallLogger, droppedCallErr := newDroppedCallLogger(r.cfg.Logging.DroppedCalls)
	r.droppedCallLogger = droppedCallLogger
	if droppedCallErr != nil {
		log.Printf("Logging: %v", droppedCallErr)
	}
	eventLogger, eventLogErr := newEventFileLogger(r.cfg.Logging)
	r.eventFileLogger = eventLogger
	if eventLogErr != nil {
		log.Printf("Logging: %v", eventLogErr)
	}
	propagationLog, propagationLogErr := newPropagationLogSink(r.cfg.Logging.Propagation)
	r.propagationLog = propagationLog
	if propagationLogErr != nil && !errors.Is(propagationLogErr, errPropagationLoggingDisabled) {
		log.Printf("Logging: %v", propagationLogErr)
	}
	log.Printf("Loaded configuration from %s", r.configSource)
	r.logConfigDiagnostics()
	if err := spot.SetDXClusterLineLength(r.cfg.Telnet.OutputLineLength); err != nil {
		return r.failStartup("Invalid telnet output line length: %v", err)
	}

	if !r.loadPathReliabilityConfig() {
		return false
	}
	r.loadSolarWeatherConfig()
	r.configureSurface()

	log.Printf("DX Cluster Server v%s starting... (commit=%s built=%s)", r.versionInfo.Version, r.versionInfo.Commit, r.versionInfo.BuildTime)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return true
}

func (r *clusterRuntime) logConfigDiagnostics() {
	for _, warning := range r.configDiagnostics.Warnings {
		log.Printf("Config warning: %s", warning)
	}
}

func (r *clusterRuntime) loadPathReliabilityConfig() bool {
	pathCfg := r.cfg.PathReliability
	allowedBands, allowedBandSet := normalizeAllowedBands(pathCfg.AllowedBands)
	pathPredictor := pathreliability.NewPredictor(pathCfg, allowedBands)
	if pathCfg.Enabled {
		if errs := pathreliability.ValidateH3Tables(r.cfg.H3TablePath); len(errs) > 0 {
			messages := make([]string, 0, len(errs))
			for _, err := range errs {
				messages = append(messages, err.Error())
			}
			return r.failStartup("Path reliability H3 table validation failed: %s", strings.Join(messages, "; "))
		}
		if err := pathreliability.InitH3MappingsFromDir(r.cfg.H3TablePath); err != nil {
			return r.failStartup("Path reliability H3 mapping init failed: %v", err)
		}
	}
	r.pathCfg = pathCfg
	r.allowedBandSet = allowedBandSet
	r.pathPredictor = pathPredictor
	if pathCfg.VOACAPFallback.Enabled {
		if err := voacap.NewRunner(pathCfg.VOACAPFallback.VOACAPHome).Validate(); err != nil {
			return r.failStartup("VOACAP fallback validation failed: %v", err)
		}
		ssnMonitor, err := voacap.NewSunspotMonitor(voacap.SunspotMonitorConfig{
			FetchInterval:      time.Duration(pathCfg.VOACAPFallback.SSNFetchIntervalSeconds) * time.Second,
			RequestTimeout:     time.Duration(pathCfg.VOACAPFallback.SSNRequestTimeoutSeconds) * time.Second,
			EWMAHalfLife:       time.Duration(pathCfg.VOACAPFallback.SSNEWMAHalfLifeSeconds) * time.Second,
			RecomputeThreshold: pathCfg.VOACAPFallback.RecomputeDeltaPercent / 100,
		})
		if err != nil {
			return r.failStartup("VOACAP SSN monitor config failed: %v", err)
		}
		forecaster := pathreliability.NewVOACAPRunnerClosedForecaster(pathCfg.VOACAPFallback)
		fallback, err := pathreliability.NewVOACAPClosedFallback(pathCfg, forecaster, ssnMonitor, log.Default())
		if err != nil {
			return r.failStartup("VOACAP closed fallback config failed: %v", err)
		}
		r.voacapSSN = ssnMonitor
		r.voacapFallback = fallback
	}
	return true
}

func (r *clusterRuntime) loadSolarWeatherConfig() {
	r.solarCfg = r.cfg.SolarWeather
}

func (r *clusterRuntime) configureSurface() {
	uiMode := strings.ToLower(strings.TrimSpace(r.cfg.UI.Mode))
	renderAllowed := isStdoutTTY()

	switch uiMode {
	case "headless":
		log.Printf("UI disabled (mode=headless)")
	case "tview":
		if !renderAllowed {
			log.Printf("UI disabled (tview requires an interactive console)")
		} else {
			r.surface = newDashboard(r.cfg.UI, true)
		}
	case "tview-v2":
		if !renderAllowed {
			log.Printf("UI disabled (tview-v2 requires an interactive console)")
		} else {
			r.surface = ui.NewDashboardV2(r.cfg.UI, true)
		}
	case "ansi":
		if !renderAllowed {
			log.Printf("UI disabled (ansi renderer requires an interactive console)")
		} else {
			r.surface = newANSIConsole(r.cfg.UI, renderAllowed)
		}
	default:
		log.Printf("UI mode %q not recognized; defaulting to headless", uiMode)
	}

	if r.surface != nil {
		r.surface.WaitReady()
		if r.logMux != nil {
			// UI surfaces render their own timestamps; keep log lines raw.
			r.logMux.SetConsoleSink(r.surface.SystemWriter(), false)
		}
		r.surface.SetStats([]string{"Initializing..."})
		return
	}
	if r.logMux != nil {
		r.logMux.SetConsoleSink(os.Stdout, true)
	}
}

func (r *clusterRuntime) startBackgroundServices() {
	if r.voacapSSN != nil {
		r.voacapSSN.Start(r.ctx)
		log.Printf("VOACAP SSN monitor enabled")
	}
	if r.voacapFallback != nil {
		r.voacapFallback.Start(r.ctx)
		log.Printf("VOACAP closed fallback enabled")
	}

	if r.solarCfg.Enabled {
		r.solarMgr = solarweather.NewManager(r.solarCfg, log.Default())
		r.solarMgr.Start(r.ctx)
		log.Printf("Solar weather overrides enabled")
	}

	if r.cfg.PropReport.Enabled {
		if !r.cfg.Logging.Propagation.Enabled || r.propagationLog == nil {
			log.Printf("Prop report enabled, but propagation logging is disabled; no propagation log rotation events will trigger reports")
		}
		propRunner := newPropReportGenerator(r.configSource, log.Default())
		r.propScheduler = newPropReportScheduler(true, propRunner, log.Default(), propReportTimeout)
		r.propScheduler.Start(r.ctx)
		if setter, ok := r.propagationLog.(rotateHookSetter); ok {
			setter.SetRotateHook(func(prevDate time.Time, prevPath, newPath string) {
				if prevDate.IsZero() {
					return
				}
				r.propScheduler.Enqueue(propReportJob{
					Date:    prevDate,
					LogPath: prevPath,
				})
			})
		}
		startPropReportDailyScheduler(r.ctx, r.cfg.PropReport, r.cfg.Logging.Propagation, r.propScheduler)
		log.Printf("Prop report scheduler enabled")
	} else {
		log.Printf("Prop report scheduler disabled")
	}

	r.startPathPredictorMaintenance()
}

func (r *clusterRuntime) startPathPredictorMaintenance() {
	if !r.pathCfg.Enabled || r.pathPredictor == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		compactInterval := time.Hour
		compactMinPeak := 1000
		compactRatio := 0.5
		var lastCompact time.Time
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().UTC()
				r.pathPredictor.PurgeStale(now)
				if lastCompact.IsZero() || now.Sub(lastCompact) >= compactInterval {
					if compacted := r.pathPredictor.Compact(compactMinPeak, compactRatio); compacted > 0 {
						log.Printf("Path predictor compacted %d shards", compacted)
					}
					lastCompact = now
				}
			}
		}
	}()
}

func (r *clusterRuntime) initializeCallCacheAndMeta() {
	callCacheTTL := time.Duration(r.cfg.CallCache.TTLSeconds) * time.Second
	spot.ConfigureNormalizeCallCache(r.cfg.CallCache.Size, callCacheTTL)
	rbn.ConfigureCallCache(r.cfg.CallCache.Size, callCacheTTL)
	pskreporter.ConfigureCallCache(r.cfg.CallCache.Size, callCacheTTL)
	filter.SetDefaultModeSelection(r.cfg.Filter.DefaultModes)
	filter.SetDefaultSourceSelection(r.cfg.Filter.DefaultSources)
	if err := filter.EnsureUserDataDir(); err != nil {
		log.Printf("Warning: unable to initialize filter directory: %v", err)
	}

	r.metaCache = newCallMetaCache(r.cfg.GridCacheSize, time.Duration(r.cfg.GridCacheTTLSec)*time.Second)
	if r.surface == nil {
		r.cfg.Print()
		return
	}
	log.Printf("Configuration loaded for %s (%s)", r.cfg.Server.Name, r.cfg.Server.NodeID)
}

func (r *clusterRuntime) initializeReferenceData() bool {
	r.initializeULSAndCTY()
	r.initializeCorrectionModels()
	r.initializeObservabilityState()
	if !r.initializeSignalResolverAndRecentStore() {
		return false
	}
	r.initializeWhoSpotsMeStore()
	if !r.initializeGridStore() {
		return false
	}
	r.initializeSkewStore()
	return true
}

func (r *clusterRuntime) loadYAMLReferenceTables() bool {
	taxonomyPath := filepath.Join(r.configSource, "spot_taxonomy.yaml")
	taxonomy, warnings, err := spot.LoadTaxonomyFileWithWarnings(taxonomyPath)
	for _, warning := range warnings {
		log.Printf("Config warning: %s", warning)
	}
	if err != nil {
		return r.failStartup("Failed to load required spot taxonomy %s: %v", taxonomyPath, err)
	}
	spot.ConfigureTaxonomy(taxonomy)
	pathPolicies := make(map[string]pathreliability.ModePolicy)
	for mode, offsetMode := range spot.PathReliabilityModePolicies() {
		pathPolicies[mode] = pathreliability.ModePolicy{Ingest: true, OffsetMode: offsetMode}
	}
	pathreliability.ConfigureModePolicies(pathPolicies)
	if !r.validateTaxonomyReferences() {
		return false
	}
	iaruPath := filepath.Join(r.configSource, "iaru_regions.yaml")
	warnings, err = spot.LoadIARURegionsFileWithWarnings(iaruPath)
	for _, warning := range warnings {
		log.Printf("Config warning: %s", warning)
	}
	if err != nil {
		return r.failStartup("Failed to load required IARU region table %s: %v", iaruPath, err)
	}
	modePath := filepath.Join(r.configSource, "iaru_mode_inference.yaml")
	warnings, err = spot.LoadIARUModeInferenceFileWithWarnings(modePath)
	for _, warning := range warnings {
		log.Printf("Config warning: %s", warning)
	}
	if err != nil {
		return r.failStartup("Failed to load required IARU mode inference table %s: %v", modePath, err)
	}
	return true
}

func (r *clusterRuntime) validateTaxonomyReferences() bool {
	for _, mode := range r.cfg.Filter.DefaultModes {
		if !spot.IsSupportedFilterMode(mode) {
			return r.failStartup("Invalid filter.default_modes entry %q: mode is not declared as filter_visible in spot_taxonomy.yaml", mode)
		}
	}
	for _, seed := range r.cfg.ModeInference.DigitalSeeds {
		if !spot.IsModeInferenceSeedMode(seed.Mode) {
			return r.failStartup("Invalid mode_inference.digital_seeds mode %q at %d kHz: mode must be declared with mode_inference_seed: true in spot_taxonomy.yaml", seed.Mode, seed.FrequencyKHz)
		}
	}
	for mode := range r.cfg.PathReliability.ModeThresholds {
		if !spot.IsKnownMode(mode) {
			return r.failStartup("Invalid path_reliability.mode_thresholds mode %q: mode is not declared in spot_taxonomy.yaml", mode)
		}
	}
	return true
}

func (r *clusterRuntime) initializeULSAndCTY() {
	// Toggle FCC ULS lookups independently of the downloader so disabled configs
	// can keep the DB on disk without performing license checks.
	uls.SetLicenseChecksEnabled(r.cfg.FCCULS.Enabled)
	uls.SetLicenseCacheTTL(time.Duration(r.cfg.FCCULS.CacheTTLSeconds) * time.Second)
	uls.SetAllowlistPath(r.cfg.FCCULS.AllowlistPath)
	uls.StartBackground(r.ctx, r.cfg.FCCULS)

	r.ctyState = newCTYRefreshState()
	ctyPath := strings.TrimSpace(r.cfg.CTY.File)
	ctyURL := strings.TrimSpace(r.cfg.CTY.URL)
	if r.cfg.CTY.Enabled && ctyPath != "" {
		if _, err := os.Stat(ctyPath); err != nil && errors.Is(err, os.ErrNotExist) && ctyURL != "" {
			if fresh, updated, refreshErr := refreshCTYDatabase(r.ctx, r.cfg.CTY); refreshErr != nil {
				log.Printf("Warning: CTY download failed: %v", refreshErr)
				r.ctyState.recordFailure(time.Now().UTC(), refreshErr)
			} else if updated && fresh != nil {
				r.ctyDB.Store(fresh)
				r.ctyState.recordSuccess(time.Now().UTC())
				log.Printf("Downloaded CTY database from %s", ctyURL)
			} else {
				r.ctyState.recordSuccess(time.Now().UTC())
				log.Printf("CTY database already up to date (%s)", ctyPath)
			}
		}
	}
	if r.cfg.CTY.Enabled && r.ctyDB.Load() == nil && ctyPath != "" {
		if loaded, loadErr := cty.LoadCTYDatabase(ctyPath); loadErr != nil {
			log.Printf("Warning: failed to load CTY database: %v", loadErr)
			r.ctyState.recordFailure(time.Now().UTC(), loadErr)
		} else {
			r.ctyDB.Store(loaded)
			r.ctyState.recordSuccess(time.Now().UTC())
			log.Printf("Loaded CTY database from %s", ctyPath)
		}
	}
	if r.cfg.CTY.Enabled && ctyURL != "" && ctyPath != "" {
		startCTYScheduler(r.ctx, r.cfg.CTY, &r.ctyDB, r.metaCache, r.ctyState)
	} else if r.cfg.CTY.Enabled {
		log.Printf("Warning: CTY download enabled but url or file missing")
	}
}

func (r *clusterRuntime) initializeCorrectionModels() {
	spot.ConfigureMorseWeights(
		r.cfg.CallCorrection.MorseWeights.Insert,
		r.cfg.CallCorrection.MorseWeights.Delete,
		r.cfg.CallCorrection.MorseWeights.Sub,
		r.cfg.CallCorrection.MorseWeights.Scale,
	)
	spot.ConfigureBaudotWeights(
		r.cfg.CallCorrection.BaudotWeights.Insert,
		r.cfg.CallCorrection.BaudotWeights.Delete,
		r.cfg.CallCorrection.BaudotWeights.Sub,
		r.cfg.CallCorrection.BaudotWeights.Scale,
	)

	r.spotterReliability = loadSpotterReliabilityWithLog(r.cfg.CallCorrection.SpotterReliabilityFile, "")
	r.spotterReliabilityCW = loadSpotterReliabilityWithLog(r.cfg.CallCorrection.SpotterReliabilityFileCW, "CW ")
	r.spotterReliabilityRTTY = loadSpotterReliabilityWithLog(r.cfg.CallCorrection.SpotterReliabilityFileRTTY, "RTTY ")

	if r.cfg.CallCorrection.ConfusionModelEnabled {
		modelPath := strings.TrimSpace(r.cfg.CallCorrection.ConfusionModelFile)
		if modelPath == "" {
			log.Printf("Warning: call correction confusion model enabled but confusion_model_file is empty")
		} else if loaded, err := spot.LoadConfusionModel(modelPath); err != nil {
			log.Printf("Warning: failed to load confusion model from %s: %v", modelPath, err)
		} else {
			r.confusionModel = loaded
			log.Printf("Loaded call correction confusion model from %s", modelPath)
		}
	}

	r.adaptiveMinReports = spot.NewAdaptiveMinReports(r.cfg.CallCorrection)
	r.refresher = newAdaptiveRefresher(r.adaptiveMinReports, r.cfg.CallCorrection.AdaptiveRefreshByBand, noopRefresh)
	if r.refresher != nil {
		r.refresher.Start()
	}
	if r.cfg.FCCULS.Enabled && strings.TrimSpace(r.cfg.FCCULS.DBPath) != "" {
		uls.SetLicenseDBPath(r.cfg.FCCULS.DBPath)
	}
}

func loadSpotterReliabilityWithLog(path, label string) spot.SpotterReliability {
	relPath := strings.TrimSpace(path)
	if relPath == "" {
		return nil
	}
	rel, n, err := spot.LoadSpotterReliability(relPath)
	if err != nil {
		log.Printf("Warning: failed to load %sspotter reliability from %s: %v", label, relPath, err)
		return nil
	}
	log.Printf("Loaded %d %sspotter reliability weights from %s", n, label, relPath)
	return rel
}

func (r *clusterRuntime) initializeObservabilityState() {
	r.statsTracker = stats.NewTracker()
	dropDedupeWindow := time.Duration(r.cfg.Logging.DropDedupeWindowSeconds) * time.Second
	r.dropLogDeduper = newDropLogDeduper(dropDedupeWindow, defaultDropLogDedupeMaxKeys)
	r.dropReporter = makeDroppedReporter(r.surface, r.dropLogDeduper)
	r.unlicensedReporter = makeUnlicensedReporter(r.surface, r.statsTracker, r.dropLogDeduper, r.droppedCallLogger)

	if r.cfg.Reputation.Enabled {
		gate, err := reputation.NewGate(r.cfg.Reputation, r.ctyLookup)
		if err != nil {
			log.Printf("Warning: reputation gate disabled: %v", err)
		} else {
			r.repGate = gate
			r.repGate.Start(r.ctx)
			consoleReporter := makeReputationDropReporter(r.surface, r.dropReporter, r.statsTracker, r.cfg.Reputation)
			r.repDropReporter = func(ev reputation.DropEvent) {
				if consoleReporter != nil {
					consoleReporter(ev)
				}
				r.logReputationDropEvent(ev)
			}
		}
	}

	capacity := r.cfg.Buffer.Capacity
	if capacity <= 0 {
		capacity = 300000
	}
	r.spotBuffer = buffer.NewRingBuffer(capacity)
	log.Printf("Ring buffer created (capacity: %d)", capacity)
}

func (r *clusterRuntime) initializeSignalResolverAndRecentStore() bool {
	if r.cfg.CallCorrection.Enabled {
		r.signalResolver = spot.NewSignalResolver(spot.SignalResolverConfig{
			QueueSize:                 shadowResolverQueueSize,
			MaxActiveKeys:             shadowResolverMaxActiveKeys,
			MaxCandidatesPerKey:       shadowResolverMaxCandidatesPerKey,
			MaxReportersPerCand:       shadowResolverMaxReportersPerCand,
			InactiveTTL:               shadowResolverInactiveTTL,
			EvalMinInterval:           shadowResolverEvalMinInterval,
			SweepInterval:             shadowResolverSweepInterval,
			HysteresisWindows:         shadowResolverHysteresisWindows,
			FreqGuardMinSeparationKHz: r.cfg.CallCorrection.FreqGuardMinSeparationKHz,
			FreqGuardRunnerUpRatio:    r.cfg.CallCorrection.FreqGuardRunnerUpRatio,
			MaxEditDistance:           r.cfg.CallCorrection.MaxEditDistance,
			DistanceModelCW:           r.cfg.CallCorrection.DistanceModelCW,
			DistanceModelRTTY:         r.cfg.CallCorrection.DistanceModelRTTY,
			SpotterReliability:        r.spotterReliability,
			SpotterReliabilityCW:      r.spotterReliabilityCW,
			SpotterReliabilityRTTY:    r.spotterReliabilityRTTY,
			MinSpotterReliability:     r.cfg.CallCorrection.MinSpotterReliability,
			ConfusionModel:            r.confusionModel,
			ConfusionWeight:           r.cfg.CallCorrection.ConfusionModelWeight,
			FamilyPolicy: spot.CorrectionFamilyPolicy{
				Configured:                 true,
				TruncationEnabled:          r.cfg.CallCorrection.FamilyPolicy.Truncation.Enabled,
				TruncationMaxLengthDelta:   r.cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
				TruncationMinShorterLength: r.cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
				TruncationAllowPrefix:      r.cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
				TruncationAllowSuffix:      r.cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch,
			},
		})
		r.signalResolver.Start()
	}

	if !r.cfg.CallCorrection.Enabled {
		return true
	}
	if r.cfg.CallCorrection.CustomSCP.Enabled {
		customOpts := spot.CustomSCPOptions{
			Path:                   r.cfg.CallCorrection.CustomSCP.Path,
			HorizonDays:            r.cfg.CallCorrection.CustomSCP.HistoryHorizonDays,
			StaticHorizonDays:      r.cfg.CallCorrection.CustomSCP.StaticHorizonDays,
			MaxKeys:                r.cfg.CallCorrection.CustomSCP.MaxKeys,
			MaxSpottersPerKey:      r.cfg.CallCorrection.CustomSCP.MaxSpottersPerKey,
			CleanupInterval:        time.Duration(r.cfg.CallCorrection.CustomSCP.CleanupIntervalSeconds) * time.Second,
			CacheSizeBytes:         int64(r.cfg.CallCorrection.CustomSCP.BlockCacheMB) << 20,
			BloomFilterBitsPerKey:  r.cfg.CallCorrection.CustomSCP.BloomFilterBits,
			MemTableSizeBytes:      uint64(r.cfg.CallCorrection.CustomSCP.MemTableSizeMB) << 20,
			L0CompactionThreshold:  r.cfg.CallCorrection.CustomSCP.L0CompactionThreshold,
			L0StopWritesThreshold:  r.cfg.CallCorrection.CustomSCP.L0StopWritesThreshold,
			CoreMinScore:           maxInt(r.cfg.CallCorrection.CustomSCP.ResolverMinScore, r.cfg.CallCorrection.CustomSCP.StabilizerMinScore),
			CoreMinH3Cells:         maxInt(r.cfg.CallCorrection.CustomSCP.ResolverMinUniqueH3Cells, r.cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells),
			SFloorMinScore:         r.cfg.CallCorrection.CustomSCP.SFloorMinScore,
			SFloorExactMinH3Cells:  r.cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsExact,
			SFloorFamilyMinH3Cells: r.cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsFamily,
			MinSNRDBCW:             r.cfg.CallCorrection.CustomSCP.MinSNRDBCW,
			MinSNRDBRTTY:           r.cfg.CallCorrection.CustomSCP.MinSNRDBRTTY,
		}
		opened, err := openCustomSCPStoreWithRecovery(r.ctx, customOpts)
		if err != nil {
			log.Printf("Failed to open custom SCP database: %v", err)
			return false
		}
		r.customSCPStore = opened
		r.customSCPStore.StartCleanup()
		r.recentBandStore = r.customSCPStore
		startCustomSCPCheckpointScheduler(r.ctx, r.customSCPStore, customOpts.Path)
		startCustomSCPIntegrityScheduler(r.ctx, r.customSCPStore)
		return true
	}
	if r.cfg.CallCorrection.RecentBandBonusEnabled || r.cfg.CallCorrection.StabilizerEnabled {
		legacyStore := spot.NewRecentBandStore(time.Duration(r.cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
		legacyStore.StartCleanup()
		r.recentBandStore = legacyStore
	}
	return true
}

func (r *clusterRuntime) initializeWhoSpotsMeStore() {
	window := time.Duration(r.cfg.WhoSpotsMe.WindowMinutes) * time.Minute
	store := spot.NewWhoSpotsMeStore(window)
	store.StartCleanup()
	r.whoSpotsMeStore = store
}

func (r *clusterRuntime) initializeGridStore() bool {
	gridOpts := gridstore.Options{
		CacheSizeBytes:        int64(r.cfg.GridBlockCacheMB) << 20,
		BloomFilterBitsPerKey: r.cfg.GridBloomFilterBits,
		MemTableSizeBytes:     uint64(r.cfg.GridMemTableSizeMB) << 20,
		L0CompactionThreshold: r.cfg.GridL0Compaction,
		L0StopWritesThreshold: r.cfg.GridL0StopWrites,
		WriteQueueDepth:       r.cfg.GridWriteQueueDepth,
	}
	gridStore, err := gridstore.Open(r.cfg.GridDBPath, gridOpts)
	if err != nil {
		if pebble.IsCorruptionError(err) {
			log.Printf("Gridstore: corruption detected on open (%v); starting checkpoint restore", err)
			startGridStoreRecovery(r.ctx, r.gridStoreHandle, r.cfg.GridDBPath, gridOpts, r.metaCache)
		} else {
			return r.failStartup("Gridstore: failed to open grid database: %v", err)
		}
	} else {
		r.gridStoreHandle.Set(gridStore)
	}

	r.gridDBCheckOnMiss, r.gridDBCheckSource = gridDBCheckOnMissEnabled(r.cfg)
	log.Printf("Gridstore: db_check_on_miss=%v (source=%s)", r.gridDBCheckOnMiss, r.gridDBCheckSource)

	gridTTL := time.Duration(r.cfg.GridTTLDays) * 24 * time.Hour
	r.gridUpdater, r.ctyUpdater, r.gridUpdateState, r.stopGridWriter, r.gridLookup, r.gridLookupSync =
		startGridWriter(r.gridStoreHandle, time.Duration(r.cfg.GridFlushSec)*time.Second, r.metaCache, gridTTL, r.gridDBCheckOnMiss, r.ctyLookup)
	startGridCheckpointScheduler(r.ctx, r.gridStoreHandle, r.cfg.GridDBPath)
	startGridIntegrityScheduler(r.ctx, r.gridStoreHandle)
	return true
}

func (r *clusterRuntime) initializeSkewStore() {
	if !r.cfg.Skew.Enabled {
		return
	}
	r.skewStore = skew.NewStore()
	if table, err := skew.LoadFile(r.cfg.Skew.File); err == nil {
		r.skewStore.Set(table)
		log.Printf("Loaded %d RBN skew corrections from %s", table.Count(), r.cfg.Skew.File)
	} else {
		log.Printf("Warning: failed to load RBN skew table (%s): %v", r.cfg.Skew.File, err)
	}
	if r.skewStore.Count() == 0 {
		if count, err := refreshSkewTable(r.ctx, r.cfg.Skew, r.skewStore); err != nil {
			log.Printf("Warning: initial RBN skew download failed: %v", err)
		} else {
			log.Printf("Downloaded %d RBN skew corrections from %s", count, r.cfg.Skew.URL)
		}
	}
	if r.skewStore.Count() > 0 {
		startSkewScheduler(r.ctx, r.cfg.Skew, r.skewStore)
		return
	}
	log.Printf("Warning: RBN skew scheduler disabled (no initial data); ensure %s is reachable", r.cfg.Skew.URL)
	r.skewStore = nil
}

func (r *clusterRuntime) initializePipelineCore() {
	r.freqAverager = spot.NewFrequencyAverager()
	r.freqAverager.StartCleanup(time.Minute, frequencyAverageWindow(r.cfg.SpotPolicy))
	if r.cfg.Harmonics.Enabled {
		r.harmonicDetector = spot.NewHarmonicDetector(spot.HarmonicSettings{
			Enabled:              true,
			RecencyWindow:        time.Duration(r.cfg.Harmonics.RecencySeconds) * time.Second,
			MaxHarmonicMultiple:  r.cfg.Harmonics.MaxHarmonicMultiple,
			FrequencyToleranceHz: r.cfg.Harmonics.FrequencyToleranceHz,
			MinReportDelta:       r.cfg.Harmonics.MinReportDelta,
			MinReportDeltaStep:   r.cfg.Harmonics.MinReportDeltaStep,
		})
		r.harmonicDetector.StartCleanup(time.Minute)
	}

	dedupWindow := time.Duration(r.cfg.Dedup.ClusterWindowSeconds) * time.Second
	r.deduplicator = dedup.NewDeduplicator(dedupWindow, r.cfg.Dedup.PreferStrongerSNR, r.cfg.Dedup.OutputBufferSize)
	r.deduplicator.Start()
	if dedupWindow > 0 {
		log.Printf("Deduplication active with %v window", dedupWindow)
	} else {
		log.Println("Deduplication disabled (cluster window=0); spots pass through unfiltered")
	}

	dedupInput := r.deduplicator.GetInputChannel()
	if r.cfg.FloodControl.Enabled {
		r.floodController = floodcontrol.New(r.cfg.FloodControl, dedupInput, r.statsTracker, r.dropReporter)
		r.floodController.Start()
		dedupInput = r.floodController.Input()
		log.Printf("Flood control active with partition=%s log_interval=%ds", r.cfg.FloodControl.PartitionMode, r.cfg.FloodControl.LogIntervalSeconds)
	} else {
		log.Println("Flood control disabled")
	}
	r.ingestValidator = newIngestValidator(r.ctyLookup, r.metaCache, r.ctyUpdater, r.gridUpdater, dedupInput, r.unlicensedReporter, r.dropReporter, r.cfg.CTY.Enabled)
	r.ingestValidator.SetBadCallReporter(r.reportBadCallDrop)
	r.ingestValidator.Start()
	r.ingestInput = r.ingestValidator.Input()

	r.initializeSecondaryDedupers()
	r.modeAssigner = newModeAssigner(r.cfg.ModeInference)
}

func (r *clusterRuntime) initializeSecondaryDedupers() {
	secondaryFastWindow := time.Duration(r.cfg.Dedup.SecondaryFastWindowSeconds) * time.Second
	secondaryMedWindow := time.Duration(r.cfg.Dedup.SecondaryMedWindowSeconds) * time.Second
	secondarySlowWindow := time.Duration(r.cfg.Dedup.SecondarySlowWindowSeconds) * time.Second

	if secondaryFastWindow > 0 {
		r.secondaryFast = dedup.NewSecondaryDeduper(secondaryFastWindow, r.cfg.Dedup.SecondaryFastPreferStrong)
		r.secondaryFast.Start()
		log.Printf("Secondary dedupe (fast) active with %v window", secondaryFastWindow)
	} else {
		log.Println("Secondary dedupe (fast) disabled")
	}
	if secondaryMedWindow > 0 {
		r.secondaryMed = dedup.NewSecondaryDeduper(secondaryMedWindow, r.cfg.Dedup.SecondaryMedPreferStrong)
		r.secondaryMed.Start()
		log.Printf("Secondary dedupe (med) active with %v window", secondaryMedWindow)
	} else {
		log.Println("Secondary dedupe (med) disabled")
	}
	if secondarySlowWindow > 0 {
		r.secondarySlow = dedup.NewSecondaryDeduperWithKey(secondarySlowWindow, r.cfg.Dedup.SecondarySlowPreferStrong, dedup.SecondaryKeyCQZone)
		r.secondarySlow.Start()
		log.Printf("Secondary dedupe (slow) active with %v window", secondarySlowWindow)
	} else {
		log.Println("Secondary dedupe (slow) disabled")
	}

	if secondaryFastWindow <= 0 && secondaryMedWindow <= 0 && secondarySlowWindow <= 0 {
		log.Println("Warning: secondary dedupe disabled (fast+med+slow=0); spots broadcast without secondary suppression")
	}
}

func modeSeedsFromConfig(cfg config.ModeInferenceConfig) []spot.ModeSeed {
	modeSeeds := make([]spot.ModeSeed, 0, len(cfg.DigitalSeeds))
	for _, seed := range cfg.DigitalSeeds {
		modeSeeds = append(modeSeeds, spot.ModeSeed{
			FrequencyKHz: seed.FrequencyKHz,
			Mode:         seed.Mode,
		})
	}
	return modeSeeds
}

func newModeAssigner(cfg config.ModeInferenceConfig) *spot.ModeAssigner {
	modeSeeds := modeSeedsFromConfig(cfg)
	assigner := spot.NewModeAssigner(spot.ModeInferenceSettings{
		DXFreqCacheTTL:        time.Duration(cfg.DXFreqCacheTTLSeconds) * time.Second,
		DXFreqCacheSize:       cfg.DXFreqCacheSize,
		DigitalWindow:         time.Duration(cfg.DigitalWindowSeconds) * time.Second,
		DigitalMinCorroborate: cfg.DigitalMinCorroborators,
		DigitalSeedTTL:        time.Duration(cfg.DigitalSeedTTLSeconds) * time.Second,
		DigitalCacheSize:      cfg.DigitalCacheSize,
		DigitalSeeds:          modeSeeds,
	})
	log.Printf("Mode inference: dx_cache=%d ttl=%ds digital_window=%ds min_corrob=%d seeds=%d seed_ttl=%ds",
		cfg.DXFreqCacheSize,
		cfg.DXFreqCacheTTLSeconds,
		cfg.DigitalWindowSeconds,
		cfg.DigitalMinCorroborators,
		len(modeSeeds),
		cfg.DigitalSeedTTLSeconds)
	return assigner
}

func (r *clusterRuntime) initializeServices() bool {
	if !r.initializePeerManager() {
		return false
	}
	r.initializeArchiveWriter()
	r.processor = commands.NewProcessor(
		r.spotBuffer,
		r.archiveWriter,
		r.ingestInput,
		r.ctyLookup,
		r.repGate,
		r.repDropReporter,
		commands.WithPathGlyphHelp(commands.PathGlyphHelpConfig{
			Enabled:      r.pathCfg.Enabled && r.pathCfg.DisplayEnabled,
			High:         r.pathCfg.GlyphSymbols.High,
			Medium:       r.pathCfg.GlyphSymbols.Medium,
			Low:          r.pathCfg.GlyphSymbols.Low,
			Unlikely:     r.pathCfg.GlyphSymbols.Unlikely,
			Insufficient: r.pathCfg.GlyphSymbols.Insufficient,
			Closed:       r.pathCfg.GlyphSymbols.Closed,
		}),
		commands.WithDedupeHelp(commands.DedupeHelpConfig{
			Configured:        true,
			FastWindowSeconds: r.cfg.Dedup.SecondaryFastWindowSeconds,
			MedWindowSeconds:  r.cfg.Dedup.SecondaryMedWindowSeconds,
			SlowWindowSeconds: r.cfg.Dedup.SecondarySlowWindowSeconds,
		}),
		commands.WithWhoSpotsMeHelp(commands.WhoSpotsMeHelpConfig{
			Configured:    true,
			WindowMinutes: r.cfg.WhoSpotsMe.WindowMinutes,
		}),
		commands.WithBuildInfo(commands.BuildInfo{
			Version:     r.versionInfo.Version,
			Commit:      r.versionInfo.Commit,
			BuildTime:   r.versionInfo.BuildTime,
			VCSModified: r.versionInfo.VCSModified,
			GoVersion:   r.versionInfo.GoVersion,
		}),
		commands.WithWhoSpotsMe(r.whoSpotsMeStore),
		commands.WithBadCallReporter(r.reportBadCallDrop),
	)
	if !r.initializeTelnetServer() {
		return false
	}
	if !r.initializeToxicityClassifier() {
		return false
	}
	r.startOutputPipeline()
	return true
}

func (r *clusterRuntime) initializeToxicityClassifier() bool {
	cfgPath := filepath.Join(r.configSource, "toxicity.yaml")
	toxicityCfg, present, err := toxicity.LoadConfig(cfgPath)
	if err != nil {
		log.Printf("Failed to load toxicity classifier config: %v", err)
		return false
	}
	if !present || !toxicityCfg.Enabled {
		log.Printf("Toxicity classifier disabled")
		return true
	}
	safeGatePath := toxicityCfg.SafeGateFile
	if !filepath.IsAbs(safeGatePath) {
		safeGatePath = filepath.Join(r.configSource, safeGatePath)
	}
	gate, err := toxicity.LoadSafeGate(safeGatePath)
	if err != nil {
		log.Printf("Failed to load toxicity safe gate: %v", err)
		return false
	}
	classifier := toxicity.NewClassifier(toxicityCfg, gate, toxicity.NewClient(toxicityCfg, nil))
	if classifier == nil {
		log.Printf("Toxicity classifier disabled by zero workers or queue")
		return true
	}
	classifier.Start()
	r.toxicityClassifier = classifier
	log.Printf("Toxicity classifier: enabled workers=%d queue=%d cache=%d ttl=%ds max_comment_bytes=%d",
		toxicityCfg.Workers,
		toxicityCfg.QueueSize,
		toxicityCfg.CacheMaxEntries,
		toxicityCfg.CacheTTLSeconds,
		toxicityCfg.MaxCommentBytes)
	return true
}

func (r *clusterRuntime) initializePeerManager() bool {
	if !r.cfg.Peering.Enabled {
		return true
	}
	pm, err := peer.NewManager(r.cfg.Peering, r.cfg.Peering.LocalCallsign, r.ingestInput, r.cfg.SpotPolicy.MaxAgeSeconds, r.dropReporter)
	if err != nil {
		log.Printf("Failed to init peering manager: %v", err)
		return false
	}
	pm.SetBadCallReporter(r.reportBadCallDrop)
	pm.SetConnectionReporter(func(ev peer.ConnectionEvent) {
		r.logPeerConnectionEvent(ev.Direction, ev.Action, ev.Peer, ev.Endpoint, ev.Reason)
	})
	if err := pm.Start(r.ctx); err != nil {
		log.Printf("Failed to start peering manager: %v", err)
		return false
	}
	r.peerManager = pm
	log.Printf("Peering: listen_port=%d peers=%d hop=%d keepalive=%ds forward_spots=%t", r.cfg.Peering.ListenPort, len(r.cfg.Peering.Peers), r.cfg.Peering.HopCount, r.cfg.Peering.KeepaliveSeconds, r.cfg.Peering.ForwardSpots)
	return true
}

func (r *clusterRuntime) initializeArchiveWriter() {
	if !r.cfg.Archive.Enabled {
		return
	}
	writer, err := archive.NewWriter(r.cfg.Archive)
	if err != nil {
		log.Printf("Warning: archive disabled due to init error: %v", err)
		return
	}
	r.archiveWriter = writer
	r.archiveWriter.Start()
	log.Printf("Archive: writing to %s (batch=%d/%dms queue=%d cleanup=%ds retention=%ds)",
		r.cfg.Archive.DBPath,
		r.cfg.Archive.BatchSize,
		r.cfg.Archive.BatchIntervalMS,
		r.cfg.Archive.QueueSize,
		r.cfg.Archive.CleanupIntervalSeconds,
		r.cfg.Archive.RetentionSeconds)
}

func (r *clusterRuntime) initializeTelnetServer() bool {
	r.telnetServer = telnet.NewServer(r.buildTelnetServerOptions(), r.processor)
	if err := r.telnetServer.Start(); err != nil {
		log.Printf("Failed to start telnet server: %v", err)
		return false
	}
	r.telnetServer.SetClientListListener(func() {
		if r.surface == nil {
			return
		}
		lines := formatNetworkLines(r.telnetServer, r.telnetServer.ListClientCallsigns())
		if len(lines) == 0 {
			return
		}
		r.surface.UpdateNetworkStatus(lines[0], lines[1:])
	})
	if r.peerManager != nil {
		r.peerManager.SetRawBroadcast(r.telnetServer.BroadcastRaw)
		r.peerManager.SetWWVBroadcast(r.telnetServer.BroadcastWWV)
		r.peerManager.SetAnnouncementBroadcast(r.telnetServer.BroadcastAnnouncement)
		r.peerManager.SetDirectMessage(r.telnetServer.SendDirectMessage)
		r.peerManager.SetUserCountProvider(r.telnetServer.GetClientCount)
	}
	return true
}

func (r *clusterRuntime) buildTelnetServerOptions() telnet.ServerOptions {
	return telnet.ServerOptions{
		Port:                      r.cfg.Telnet.Port,
		WelcomeMessage:            r.cfg.Telnet.WelcomeMessage,
		DuplicateLoginMsg:         r.cfg.Telnet.DuplicateLoginMsg,
		LoginGreeting:             r.cfg.Telnet.LoginGreeting,
		LoginPrompt:               r.cfg.Telnet.LoginPrompt,
		LoginEmptyMessage:         r.cfg.Telnet.LoginEmptyMessage,
		LoginInvalidMessage:       r.cfg.Telnet.LoginInvalidMessage,
		InputTooLongMessage:       r.cfg.Telnet.InputTooLongMessage,
		InputInvalidCharMessage:   r.cfg.Telnet.InputInvalidCharMessage,
		DialectWelcomeMessage:     r.cfg.Telnet.DialectWelcomeMessage,
		DialectSourceDefault:      r.cfg.Telnet.DialectSourceDefaultLabel,
		DialectSourcePersisted:    r.cfg.Telnet.DialectSourcePersistedLabel,
		PathStatusMessage:         r.cfg.Telnet.PathStatusMessage,
		ClusterCall:               r.cfg.Server.NodeID,
		MaxConnections:            r.cfg.Telnet.MaxConnections,
		BroadcastWorkers:          r.cfg.Telnet.BroadcastWorkers,
		BroadcastQueue:            r.cfg.Telnet.BroadcastQueue,
		WorkerQueue:               r.cfg.Telnet.WorkerQueue,
		ClientBuffer:              r.cfg.Telnet.ClientBuffer,
		ControlQueue:              r.cfg.Telnet.ControlQueueSize,
		BulletinDedupeWindow:      time.Duration(r.cfg.Telnet.BulletinDedupeWindowSeconds) * time.Second,
		BulletinDedupeMaxEntries:  r.cfg.Telnet.BulletinDedupeMaxEntries,
		BroadcastBatchInterval:    time.Duration(r.cfg.Telnet.BroadcastBatchIntervalMS) * time.Millisecond,
		BroadcastBatchIntervalSet: true,
		WriterBatchMaxBytes:       r.cfg.Telnet.WriterBatchMaxBytes,
		WriterBatchWait:           time.Duration(r.cfg.Telnet.WriterBatchWaitMS) * time.Millisecond,
		RejectWorkers:             r.cfg.Telnet.RejectWorkers,
		RejectQueueSize:           r.cfg.Telnet.RejectQueueSize,
		RejectWriteDeadline:       time.Duration(r.cfg.Telnet.RejectWriteDeadlineMS) * time.Millisecond,
		Transport:                 r.cfg.Telnet.Transport,
		EchoMode:                  r.cfg.Telnet.EchoMode,
		HandshakeMode:             string(r.cfg.Telnet.SkipHandshake),
		ReadIdleTimeout:           time.Duration(r.cfg.Telnet.ReadIdleTimeoutSeconds) * time.Second,
		LoginTimeout:              time.Duration(r.cfg.Telnet.LoginTimeoutSeconds) * time.Second,
		MaxPreloginSessions:       r.cfg.Telnet.MaxPreloginSessions,
		PreloginTimeout:           time.Duration(r.cfg.Telnet.PreloginTimeoutSeconds) * time.Second,
		AcceptRatePerIP:           r.cfg.Telnet.AcceptRatePerIP,
		AcceptBurstPerIP:          r.cfg.Telnet.AcceptBurstPerIP,
		AcceptRatePerSubnet:       r.cfg.Telnet.AcceptRatePerSubnet,
		AcceptBurstPerSubnet:      r.cfg.Telnet.AcceptBurstPerSubnet,
		AcceptRateGlobal:          r.cfg.Telnet.AcceptRateGlobal,
		AcceptBurstGlobal:         r.cfg.Telnet.AcceptBurstGlobal,
		AcceptRatePerASN:          r.cfg.Telnet.AcceptRatePerASN,
		AcceptBurstPerASN:         r.cfg.Telnet.AcceptBurstPerASN,
		AcceptRatePerCountry:      r.cfg.Telnet.AcceptRatePerCountry,
		AcceptBurstPerCountry:     r.cfg.Telnet.AcceptBurstPerCountry,
		PreloginConcurrencyPerIP:  r.cfg.Telnet.PreloginConcurrencyPerIP,
		AdmissionLogInterval:      time.Duration(r.cfg.Telnet.AdmissionLogIntervalSeconds) * time.Second,
		AdmissionLogSampleRate:    r.cfg.Telnet.AdmissionLogSampleRate,
		AdmissionLogMaxLines:      r.cfg.Telnet.AdmissionLogMaxReasonLinesPerInterval,
		LoginLineLimit:            r.cfg.Telnet.LoginLineLimit,
		CommandLineLimit:          r.cfg.Telnet.CommandLineLimit,
		DropExtremeRate:           r.cfg.Telnet.DropExtremeRate,
		DropExtremeWindow:         time.Duration(r.cfg.Telnet.DropExtremeWindowSeconds) * time.Second,
		DropExtremeMinAttempts:    r.cfg.Telnet.DropExtremeMinAttempts,
		ReputationGate:            r.repGate,
		PathPredictor:             r.pathPredictor,
		PathClosedFallback:        r.voacapFallback,
		PathDisplayEnabled:        r.pathCfg.DisplayEnabled,
		NoiseModel:                r.pathCfg.NoiseModel(),
		GridLookup:                r.gridLookup,
		CTYLookup:                 r.ctyLookup,
		DedupeFastEnabled:         r.secondaryFast != nil,
		DedupeMedEnabled:          r.secondaryMed != nil,
		DedupeSlowEnabled:         r.secondarySlow != nil,
		DefaultDedupePolicy:       r.cfg.Dedup.DefaultPolicy,
		NearbyLoginWarning:        r.cfg.Telnet.NearbyLoginWarning,
		LoginAttemptReporter:      r.logLoginAttemptEvent,
		ConnectionReporter:        r.logTelnetConnectionEvent,
		SolarWeather:              r.solarMgr,
	}
}

func (r *clusterRuntime) startOutputPipeline() {
	// The output pipeline remains a single goroutine so sequencing of correction,
	// suppression, archive fan-out, and peer publish stays identical.
	go processOutputSpots(
		r.deduplicator,
		r.secondaryFast,
		r.secondaryMed,
		r.secondarySlow,
		&r.secondaryStageCount,
		r.modeAssigner,
		r.spotBuffer,
		r.telnetServer,
		r.peerManager,
		r.statsTracker,
		r.signalResolver,
		r.cfg.CallCorrection,
		r.ctyLookup,
		r.metaCache,
		r.harmonicDetector,
		r.cfg.Harmonics,
		r.freqAverager,
		r.cfg.SpotPolicy,
		r.surface,
		r.gridUpdater,
		r.gridLookup,
		r.gridLookupSync,
		r.unlicensedReporter,
		r.droppedCallLogger,
		r.adaptiveMinReports,
		r.refresher,
		r.spotterReliability,
		r.spotterReliabilityCW,
		r.spotterReliabilityRTTY,
		r.confusionModel,
		r.recentBandStore,
		r.customSCPStore,
		r.whoSpotsMeStore,
		r.cfg.RBN.KeepSSIDSuffix,
		r.archiveWriter,
		&r.lastOutput,
		r.pathPredictor,
		r.pathReport,
		r.allowedBandSet,
		r.toxicityClassifier,
	)
	startPipelineHealthMonitor(r.ctx, r.deduplicator, &r.lastOutput, r.peerManager)
}

func (r *clusterRuntime) connectFeeds() {
	r.connectRBNFeed()
	r.connectRBNDigitalFeed()
	r.connectHumanTelnetFeed()
	r.connectDXSummitFeed()
	r.connectPSKReporterFeed()
}

func (r *clusterRuntime) connectRBNFeed() {
	if !r.cfg.RBN.Enabled {
		return
	}
	r.rbnClient = rbn.NewClient(r.cfg.RBN.Host, r.cfg.RBN.Port, r.cfg.RBN.Callsign, r.cfg.RBN.Name, r.skewStore, r.cfg.RBN.KeepSSIDSuffix, r.cfg.RBN.SlotBuffer)
	r.rbnClient.SetBadCallReporter(r.reportBadCallDrop)
	r.rbnClient.SetTelnetTransport(r.cfg.RBN.TelnetTransport)
	if r.cfg.RBN.KeepaliveSec > 0 {
		r.rbnClient.EnableKeepalive(time.Duration(r.cfg.RBN.KeepaliveSec) * time.Second)
	}
	if err := r.rbnClient.ConnectWithInitialRetry(); err != nil {
		log.Printf("Warning: Failed to connect to RBN CW/RTTY: %v", err)
		r.logIngestConnectionEvent(ingestSourceName(r.cfg.RBN.Name, "RBN"), "failed", eventLogEndpoint(r.cfg.RBN.Host, r.cfg.RBN.Port), err.Error(), "disconnected")
	}
	go forwardSpots(r.rbnClient.GetSpotChannel(), r.ingestInput, "RBN-CW", r.cfg.SpotPolicy, nil)
	log.Println("RBN CW/RTTY client ready to forward spots into unified dedup engine")
}

func (r *clusterRuntime) connectRBNDigitalFeed() {
	if !r.cfg.RBNDigital.Enabled {
		return
	}
	r.rbnDigitalClient = rbn.NewClient(r.cfg.RBNDigital.Host, r.cfg.RBNDigital.Port, r.cfg.RBNDigital.Callsign, r.cfg.RBNDigital.Name, r.skewStore, r.cfg.RBNDigital.KeepSSIDSuffix, r.cfg.RBNDigital.SlotBuffer)
	r.rbnDigitalClient.SetBadCallReporter(r.reportBadCallDrop)
	r.rbnDigitalClient.SetTelnetTransport(r.cfg.RBNDigital.TelnetTransport)
	if r.cfg.RBNDigital.KeepaliveSec > 0 {
		r.rbnDigitalClient.EnableKeepalive(time.Duration(r.cfg.RBNDigital.KeepaliveSec) * time.Second)
	}
	if err := r.rbnDigitalClient.ConnectWithInitialRetry(); err != nil {
		log.Printf("Warning: Failed to connect to RBN Digital: %v", err)
		r.logIngestConnectionEvent(ingestSourceName(r.cfg.RBNDigital.Name, "RBN Digital"), "failed", eventLogEndpoint(r.cfg.RBNDigital.Host, r.cfg.RBNDigital.Port), err.Error(), "disconnected")
	}
	go forwardSpots(r.rbnDigitalClient.GetSpotChannel(), r.ingestInput, "RBN-FT", r.cfg.SpotPolicy, nil)
	log.Println("RBN Digital (FT4/FT8) client ready to forward spots into unified dedup engine")
}

func (r *clusterRuntime) connectHumanTelnetFeed() {
	if !r.cfg.HumanTelnet.Enabled {
		return
	}
	rawPassthrough := make(chan string, 256)
	go r.forwardHumanPassthrough(rawPassthrough)

	r.humanTelnetClient = rbn.NewClient(r.cfg.HumanTelnet.Host, r.cfg.HumanTelnet.Port, r.cfg.HumanTelnet.Callsign, r.cfg.HumanTelnet.Name, r.skewStore, r.cfg.HumanTelnet.KeepSSIDSuffix, r.cfg.HumanTelnet.SlotBuffer)
	r.humanTelnetClient.SetBadCallReporter(r.reportBadCallDrop)
	r.humanTelnetClient.SetTelnetTransport(r.cfg.HumanTelnet.TelnetTransport)
	r.humanTelnetClient.UseMinimalParser()
	r.humanTelnetClient.SetRawPassthrough(rawPassthrough)
	if r.cfg.HumanTelnet.KeepaliveSec > 0 {
		// Prevent idle disconnects on upstream telnet feeds by sending periodic CRLF.
		r.humanTelnetClient.EnableKeepalive(time.Duration(r.cfg.HumanTelnet.KeepaliveSec) * time.Second)
	}
	if err := r.humanTelnetClient.Connect(); err != nil {
		log.Printf("Warning: Failed to connect to human/relay telnet feed: %v", err)
		r.logIngestConnectionEvent(ingestSourceName(r.cfg.HumanTelnet.Name, "Human Telnet"), "failed", eventLogEndpoint(r.cfg.HumanTelnet.Host, r.cfg.HumanTelnet.Port), err.Error(), "disconnected")
		return
	}
	go forwardSpots(r.humanTelnetClient.GetSpotChannel(), r.ingestInput, "HUMAN-TELNET", r.cfg.SpotPolicy, func(sp *spot.Spot) {
		sp.IsHuman = true
		sp.SourceType = spot.SourceUpstream
		if strings.TrimSpace(sp.SourceNode) == "" {
			sp.SourceNode = "HUMAN-TELNET"
		}
		if strings.TrimSpace(sp.Mode) == "" {
			sp.Mode = "RTTY" // temporary default until mode parser is added
			sp.EnsureNormalized()
		}
	})
	log.Println("Human/relay telnet client feeding spots into unified dedup engine")
}

func (r *clusterRuntime) forwardHumanPassthrough(rawPassthrough <-chan string) {
	for line := range rawPassthrough {
		if r.telnetServer == nil {
			continue
		}
		if kind := wwvKindFromLine(line); kind != "" {
			r.telnetServer.BroadcastWWV(kind, line)
			continue
		}
		if announcement := announcementFromLine(line); announcement != "" {
			r.telnetServer.BroadcastAnnouncement(announcement)
		}
	}
}

func (r *clusterRuntime) connectDXSummitFeed() {
	if !r.cfg.DXSummit.Enabled {
		return
	}
	r.dxsummitClient = dxsummit.NewClient(r.cfg.DXSummit)
	r.dxsummitClient.SetBadCallReporter(r.reportBadCallDrop)
	if err := r.dxsummitClient.Connect(); err != nil {
		log.Printf("Warning: Failed to start DXSummit: %v", err)
		r.logIngestConnectionEvent(ingestSourceName(r.cfg.DXSummit.Name, dxsummit.SourceNode), "failed", r.dxsummitClient.Endpoint(), err.Error(), "disconnected")
		r.dxsummitClient = nil
		return
	}
	go forwardSpots(r.dxsummitClient.GetSpotChannel(), r.ingestInput, "DXSummit", r.cfg.SpotPolicy, nil)
	log.Println("DXSummit client feeding spots into unified dedup engine")
}

func (r *clusterRuntime) connectPSKReporterFeed() {
	if !r.cfg.PSKReporter.Enabled {
		return
	}
	r.pskrTopics = r.cfg.PSKReporter.SubscriptionTopics()
	mqttQoS12Timeout := time.Duration(r.cfg.PSKReporter.MQTTQoS12EnqueueTimeoutMS) * time.Millisecond
	ftDialRegistry := spot.NewFTDialRegistry(modeSeedsFromConfig(r.cfg.ModeInference))
	r.pskrClient = pskreporter.NewClient(
		r.cfg.PSKReporter.Broker,
		r.cfg.PSKReporter.Port,
		r.pskrTopics,
		r.cfg.PSKReporter.Name,
		r.cfg.PSKReporter.Workers,
		r.cfg.PSKReporter.MQTTInboundWorkers,
		r.cfg.PSKReporter.MQTTInboundQueueDepth,
		mqttQoS12Timeout,
		r.skewStore,
		ftDialRegistry,
		r.cfg.PSKReporter.AppendSpotterSSID,
		r.cfg.PSKReporter.SpotChannelSize,
		r.cfg.PSKReporter.MaxPayloadBytes,
	)
	r.pskrClient.SetBadCallReporter(r.reportBadCallDrop)
	if err := r.pskrClient.Connect(); err != nil {
		log.Printf("Warning: Failed to connect to PSKReporter: %v", err)
		r.logIngestConnectionEvent(ingestSourceName(r.cfg.PSKReporter.Name, "PSKReporter"), "failed", r.pskrClient.Endpoint(), err.Error(), "disconnected")
		return
	}
	go forwardSpots(r.pskrClient.GetSpotChannel(), r.ingestInput, "PSKReporter", r.cfg.SpotPolicy, nil)
	go processPSKRPathOnlySpots(r.pskrClient, r.pathPredictor, r.pathReport, r.pskrPathOnlyStats, r.cfg.SpotPolicy, r.allowedBandSet)
	log.Println("PSKReporter client feeding spots into unified dedup engine")
}

func (r *clusterRuntime) startMonitors() {
	ingestSources := make([]ingestHealthSource, 0, 5)
	if r.rbnClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(r.cfg.RBN.Name, "RBN"), r.rbnClient))
	}
	if r.rbnDigitalClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(r.cfg.RBNDigital.Name, "RBN Digital"), r.rbnDigitalClient))
	}
	if r.humanTelnetClient != nil {
		ingestSources = append(ingestSources, rbnHealthSource(ingestSourceName(r.cfg.HumanTelnet.Name, "Human Telnet"), r.humanTelnetClient))
	}
	if r.dxsummitClient != nil {
		ingestSources = append(ingestSources, dxsummitHealthSource(ingestSourceName(r.cfg.DXSummit.Name, dxsummit.SourceNode), r.dxsummitClient))
	}
	if r.pskrClient != nil {
		ingestSources = append(ingestSources, pskReporterHealthSource(ingestSourceName(r.cfg.PSKReporter.Name, "PSKReporter"), r.pskrClient))
	}
	startIngestHealthMonitorWithReporter(r.ctx, ingestSources, r.logIngestConnectionEvent)

	statsInterval := time.Duration(r.cfg.Stats.DisplayIntervalSeconds) * time.Second
	go displayStatsWithFCC(
		statsInterval,
		r.statsTracker,
		r.ingestValidator,
		r.deduplicator,
		r.secondaryFast,
		r.secondaryMed,
		r.secondarySlow,
		&r.secondaryStageCount,
		r.spotBuffer,
		r.ctyLookup,
		r.metaCache,
		r.ctyState,
		r.recentBandStore,
		r.signalResolver,
		r.telnetServer,
		r.surface,
		r.gridUpdateState,
		r.gridStoreHandle,
		r.cfg.FCCULS.DBPath,
		r.pathPredictor,
		r.modeAssigner,
		r.toxicityClassifier,
		r.rbnClient,
		r.rbnDigitalClient,
		r.pskrClient,
		r.dxsummitClient,
		r.pskrPathOnlyStats,
		r.peerManager,
		r.cfg.Server.NodeID,
		r.cfg.Skew.File,
		dashboardIngestSourceConfigFromConfig(r.cfg),
	)
	if r.pathCfg.Enabled {
		go startPathPredictionLogger(r.ctx, r.propagationLog, r.telnetServer, r.pathPredictor, r.pathReport)
	}
}

func (r *clusterRuntime) logStartup() {
	log.Println("Cluster is running. Press Ctrl+C to stop.")
	log.Printf("Connect via: telnet localhost %d", r.cfg.Telnet.Port)
	if r.cfg.RBN.Enabled {
		log.Println("Receiving CW/RTTY spots from RBN (port 7000)...")
	}
	if r.cfg.RBNDigital.Enabled {
		log.Println("Receiving FT4/FT8 spots from RBN Digital (port 7001)...")
	}
	if r.cfg.PSKReporter.Enabled {
		topicList := strings.Join(r.pskrTopics, ", ")
		if topicList == "" {
			topicList = "<none>"
		}
		log.Printf("Receiving digital mode spots from PSKReporter (topics: %s)...", topicList)
	}
	if r.cfg.HumanTelnet.Enabled {
		log.Printf("Receiving human/relay spots from %s:%d...", r.cfg.HumanTelnet.Host, r.cfg.HumanTelnet.Port)
	}
	if r.cfg.DXSummit.Enabled {
		log.Printf("Receiving DXSummit HTTP spots every %d seconds...", r.cfg.DXSummit.PollIntervalSeconds)
	}
	if r.cfg.Dedup.ClusterWindowSeconds > 0 {
		log.Printf("Unified deduplication active: %d second window", r.cfg.Dedup.ClusterWindowSeconds)
	} else {
		log.Println("Unified deduplication bypassed (window=0); duplicates are not filtered")
	}
	logGoRuntimeTuning(r.cfg.GoRuntime)
	log.Println("Architecture: ALL sources -> Dedup Engine -> Ring Buffer -> Clients")
	log.Printf("Statistics will be displayed every %d seconds...", r.cfg.Stats.DisplayIntervalSeconds)
	log.Println("---")
	maybeStartContentionProfiling()
	maybeStartHeapLogger()
	maybeStartMapLogger(r.statsTracker, r.pathPredictor, r.deduplicator, r.secondaryFast, r.secondaryMed, r.secondarySlow, r.customSCPStore)
	maybeStartDiagServer()
}

func (r *clusterRuntime) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal: %v", sig)
	log.Println("Shutting down gracefully...")
	r.shutdown()
	log.Println("Cluster stopped")
}

func (r *clusterRuntime) shutdown() {
	if r.freqAverager != nil {
		r.freqAverager.StopCleanup()
	}
	if r.harmonicDetector != nil {
		r.harmonicDetector.StopCleanup()
	}
	if r.recentBandStore != nil {
		r.recentBandStore.StopCleanup()
	}
	if r.whoSpotsMeStore != nil {
		r.whoSpotsMeStore.StopCleanup()
	}
	if r.signalResolver != nil {
		r.signalResolver.Stop()
	}
	if r.floodController != nil {
		r.floodController.Stop()
	}
	if r.deduplicator != nil {
		r.deduplicator.Stop()
	}
	if r.peerManager != nil {
		r.peerManager.Stop()
	}
	if r.secondaryFast != nil {
		r.secondaryFast.Stop()
	}
	if r.secondaryMed != nil {
		r.secondaryMed.Stop()
	}
	if r.secondarySlow != nil {
		r.secondarySlow.Stop()
	}
	if r.rbnClient != nil {
		r.rbnClient.Stop()
	}
	if r.rbnDigitalClient != nil {
		r.rbnDigitalClient.Stop()
	}
	if r.pskrClient != nil {
		r.pskrClient.Stop()
	}
	if r.dxsummitClient != nil {
		r.dxsummitClient.Stop()
	}
	if r.telnetServer != nil {
		r.telnetServer.Stop()
	}
}

func (r *clusterRuntime) close() {
	// Preserve the previous defer-chain order now that startup is factored into
	// helpers: archive first, then grid writer/store, then recent-support store,
	// refresher, context cancellation/wait, UI, and finally logging sinks.
	if r.archiveWriter != nil {
		r.archiveWriter.Stop()
	}
	if r.stopGridWriter != nil {
		r.stopGridWriter()
	}
	if r.gridStoreHandle != nil {
		r.gridStoreHandle.Close()
	}
	if r.customSCPStore != nil {
		_ = r.customSCPStore.Close()
	} else if r.recentBandStore != nil {
		r.recentBandStore.StopCleanup()
	}
	if r.whoSpotsMeStore != nil {
		r.whoSpotsMeStore.StopCleanup()
	}
	if r.refresher != nil {
		r.refresher.Stop()
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.voacapFallback != nil {
		r.voacapFallback.Wait()
	}
	if r.voacapSSN != nil {
		r.voacapSSN.Wait()
	}
	if r.propScheduler != nil {
		r.propScheduler.Wait()
	}
	if r.surface != nil {
		r.surface.Stop()
	}
	if r.droppedCallLogger != nil {
		_ = r.droppedCallLogger.Close()
	}
	if r.eventFileLogger != nil {
		_ = r.eventFileLogger.Close()
	}
	if r.propagationLog != nil {
		_ = r.propagationLog.Close()
	}
	if r.logMux != nil {
		r.logMux.Close()
	}
}

func (r *clusterRuntime) reportBadCallDrop(source, role, reason, call, deCall, dxCall, mode, detail string) {
	if r == nil || r.droppedCallLogger == nil {
		return
	}
	r.droppedCallLogger.LogBadCall(source, role, reason, call, deCall, dxCall, mode, detail)
}

func (r *clusterRuntime) ctyLookup() *cty.CTYDatabase {
	return r.ctyDB.Load()
}
