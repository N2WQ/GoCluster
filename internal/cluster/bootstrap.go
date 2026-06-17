// Program gocluster wires together all ingest clients (RBN, PSKReporter),
// protections (deduplication, call correction, harmonics), persistence layers
// (ring buffer, grid store), and the telnet server UI.
package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	pprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/download"
	"dxcluster/dxsummit"
	"dxcluster/gridstore"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/pebbleresilience"
	"dxcluster/internal/ratelimit"
	"dxcluster/internal/schedule"
	"dxcluster/internal/toxicity"
	"dxcluster/pathreliability"
	"dxcluster/peer"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/reputation"
	"dxcluster/skew"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/strutil"
	"dxcluster/telnet"
	"dxcluster/ui"
	"dxcluster/uls"

	"github.com/cockroachdb/pebble"
	"github.com/dustin/go-humanize"
	"golang.org/x/term"
)

const (
	dedupeEntryBytes    = 32
	callMetaEntryBytes  = 96
	sourceModeDelimiter = "|"
	defaultConfigPath   = "data/config"
	envConfigPath       = "DXC_CONFIG_PATH"

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

	shadowResolverQueueSize           = 8192
	shadowResolverMaxActiveKeys       = 6000
	shadowResolverMaxCandidatesPerKey = 16
	shadowResolverMaxReportersPerCand = 64
	shadowResolverInactiveTTL         = 10 * time.Minute
	shadowResolverEvalMinInterval     = 500 * time.Millisecond
	shadowResolverSweepInterval       = 1 * time.Second
	shadowResolverHysteresisWindows   = 2
)

var ingestForwardDropLogInterval = 30 * time.Second

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type BuildInfo struct {
	Version     string
	Commit      string
	BuildTime   string
	VCSModified string
	GoVersion   string
}

var currentBuildInfo = BuildInfo{
	Version:   "dev",
	Commit:    "unknown",
	BuildTime: "unknown",
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
func loadClusterConfig() (*config.Config, string, config.LoadDiagnostics, error) {
	candidates := make([]string, 0, 2)
	if envPath := strings.TrimSpace(os.Getenv(envConfigPath)); envPath != "" {
		candidates = append(candidates, envPath)
	}
	candidates = append(candidates, defaultConfigPath)

	var lastErr error
	var lastDiagnostics config.LoadDiagnostics
	for _, path := range candidates {
		if path == "" {
			continue
		}
		cfg, diagnostics, err := config.LoadWithDiagnostics(path)
		if err != nil {
			lastDiagnostics = diagnostics
			if os.IsNotExist(err) {
				lastErr = err
				continue
			}
			return nil, path, diagnostics, err
		}
		return cfg, cfg.LoadedFrom, diagnostics, nil
	}
	return nil, "", lastDiagnostics, fmt.Errorf("unable to load config; tried %s (last error: %w)", strings.Join(candidates, ", "), lastErr)
}

func logStartupConfigDiagnosticsFallback(diagnostics config.LoadDiagnostics) {
	if len(diagnostics.Warnings) == 0 && len(diagnostics.Errors) == 0 {
		return
	}
	log.Printf("Config diagnostics: configured system log is unavailable before config load completes; writing startup diagnostics to stderr")
	for _, warning := range diagnostics.Warnings {
		log.Printf("Config warning: %s", warning)
	}
	for _, errText := range diagnostics.Errors {
		log.Printf("Config error: %s", errText)
	}
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

// Run is the live-binary entrypoint behind the root package main wrapper.
// It preserves the existing configuration load, startup, and shutdown flow.
func Run(versionInfo BuildInfo) error {
	currentBuildInfo = versionInfo
	cfg, configSource, diagnostics, err := loadClusterConfig()
	if err != nil {
		logStartupConfigDiagnosticsFallback(diagnostics)
		return err
	}
	applyGoRuntimeTuning(cfg.GoRuntime)
	runtime := newClusterRuntime(versionInfo, cfg, configSource, diagnostics)
	defer runtime.close()
	if !runtime.initialize() {
		if runtime.startupErr != nil {
			return runtime.startupErr
		}
		return fmt.Errorf("cluster startup aborted")
	}
	runtime.waitForShutdown()
	return nil
}

// Purpose: Build a reporter callback for unlicensed drops.
// Key aspects: Returns a closure that increments stats and formats output.
// Upstream: main wiring for applyLicenseGate reporting.
// Downstream: tracker.IncrementUnlicensedDrops and dash.AppendUnlicensed/log.Println.
func makeUnlicensedReporter(dash ui.Surface, tracker *stats.Tracker, deduper *dropLogDeduper, droppedCalls *droppedCallLogger) func(source, role, call, deCall, dxCall, mode string, freq float64) {
	// Purpose: Emit an unlicensed drop event with consistent formatting.
	// Key aspects: Normalizes fields and routes to UI or log.
	// Upstream: applyLicenseGate.
	// Downstream: tracker.IncrementUnlicensedDrops, dash.AppendUnlicensed, log.Println.
	return func(source, role, call, deCall, dxCall, mode string, freq float64) {
		if tracker != nil {
			tracker.IncrementUnlicensedDrops()
		}
		source = strutil.NormalizeUpper(source)
		role = strutil.NormalizeUpper(role)
		mode = strutil.NormalizeUpper(mode)
		call = strings.TrimSpace(strings.ToUpper(call))
		if droppedCalls != nil {
			droppedCalls.LogNoLicense(source, role, call, deCall, dxCall, mode, "fcc_uls")
		}

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
	reasons := formatTopCounterSummary(tracker.CorrectionDecisionReasons(), 2)
	appliedReasons := formatTopCounterSummary(tracker.CorrectionDecisionAppliedReasons(), 2)
	paths := formatTopCounterSummary(tracker.CorrectionDecisionPaths(), 2)
	return fmt.Sprintf("CorrGate: %s (T) / %s (A) / %s (R) / %s (FB) [rej:%s ap:%s] [%s]",
		humanize.Comma(int64(total)),
		humanize.Comma(int64(applied)),
		humanize.Comma(int64(rejected)),
		humanize.Comma(int64(fallback)),
		reasons,
		appliedReasons,
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

func formatFloodSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "Flood: n/a"
	}
	return fmt.Sprintf(
		"Flood: %s (O) / %s (S) / %s (D) / %s (X)",
		humanize.Comma(int64(tracker.FloodObserved())),
		humanize.Comma(int64(tracker.FloodSuppressed())),
		humanize.Comma(int64(tracker.FloodDropped())),
		humanize.Comma(int64(tracker.FloodOverflow())),
	)
}

func formatStabilizerGlyphSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "Stabilizer Glyph: n/a"
	}
	glyphStats := tracker.StabilizerGlyphTurnStats()
	if len(glyphStats) == 0 {
		return "Stabilizer Glyph: n/a"
	}
	order := []string{"?", "S", "P", "V", "C"}
	parts := make([]string, 0, len(glyphStats))
	seen := make(map[string]struct{}, len(order))
	for _, glyph := range order {
		stat, ok := glyphStats[glyph]
		if !ok || stat.Samples == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %.2f", glyph, stat.AverageTurns))
		seen[glyph] = struct{}{}
	}
	extras := make([]string, 0, len(glyphStats))
	for glyph, stat := range glyphStats {
		if _, ok := seen[glyph]; ok {
			continue
		}
		if stat.Samples == 0 {
			continue
		}
		extras = append(extras, glyph)
	}
	sort.Strings(extras)
	for _, glyph := range extras {
		parts = append(parts, fmt.Sprintf("%s %.2f", glyph, glyphStats[glyph].AverageTurns))
	}
	if len(parts) == 0 {
		return "Stabilizer Glyph: n/a"
	}
	return "Stabilizer Glyph: avg turns " + strings.Join(parts, " | ")
}

func formatResolverSummary(resolver *spot.SignalResolver) string {
	if resolver == nil {
		return "Resolver: n/a"
	}
	metrics := resolver.MetricsSnapshot()
	return fmt.Sprintf(
		"Resolver: %s (C) / %s (P) / %s (U) / %s (S) | q=%d drop %s (Q) / %s (K) / %s (C) / %s (R)",
		humanize.Comma(int64(metrics.StateConfident)),
		humanize.Comma(int64(metrics.StateProbable)),
		humanize.Comma(int64(metrics.StateUncertain)),
		humanize.Comma(int64(metrics.StateSplit)),
		metrics.QueueDepth,
		humanize.Comma(int64(metrics.DropQueueFull)),
		humanize.Comma(int64(metrics.DropMaxKeys)),
		humanize.Comma(int64(metrics.DropMaxCandidates)),
		humanize.Comma(int64(metrics.DropMaxReporters)),
	)
}

func formatResolverPressureSummary(resolver *spot.SignalResolver) string {
	if resolver == nil {
		return "Resolver Pressure: n/a"
	}
	metrics := resolver.MetricsSnapshot()
	return fmt.Sprintf(
		"Resolver Pressure: %s (C) / %s (R) evict %s (C) / %s (R) hw %s (C) / %s (R)",
		humanize.Comma(int64(metrics.CapPressureCandidates)),
		humanize.Comma(int64(metrics.CapPressureReporters)),
		humanize.Comma(int64(metrics.EvictedCandidates)),
		humanize.Comma(int64(metrics.EvictedReporters)),
		humanize.Comma(int64(metrics.HighWaterCandidates)),
		humanize.Comma(int64(metrics.HighWaterReporters)),
	)
}

func formatTemporalSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "Temporal: n/a"
	}
	return fmt.Sprintf(
		"Temporal: pending %s | committed %s | fallback %s | abstain %s | bypass %s",
		humanize.Comma(tracker.TemporalPending()),
		humanize.Comma(int64(tracker.TemporalCommitted())),
		humanize.Comma(int64(tracker.TemporalFallbackResolver())),
		humanize.Comma(int64(tracker.TemporalAbstainLowMargin())),
		humanize.Comma(int64(tracker.TemporalOverflowBypass())),
	)
}

func formatToxicitySummary(classifier *toxicity.Classifier) string {
	if classifier == nil {
		return "Toxicity: disabled"
	}
	stats := classifier.Snapshot()
	return fmt.Sprintf(
		"Toxicity: %s (AI) / %s (L) / %s (H) / %s (T) / %s (U) | q=%d rb=%d cache=%d evict=%s err %s (TO) / %s (M) / %s (Q)",
		humanize.Comma(int64(stats.AICalls)),
		humanize.Comma(int64(stats.SafeBypass)),
		humanize.Comma(int64(stats.CacheHits)),
		humanize.Comma(int64(stats.Toxic)),
		humanize.Comma(int64(stats.Unavailable)),
		stats.Pending,
		stats.ResultBacklog,
		stats.CacheEntries,
		humanize.Comma(int64(stats.Evictions)),
		humanize.Comma(int64(stats.Timeouts)),
		humanize.Comma(int64(stats.Malformed)),
		humanize.Comma(int64(stats.QueueFull)),
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
func displayStatsWithFCC(interval time.Duration, tracker *stats.Tracker, ingestStats *ingestValidator, dedup *dedup.Deduplicator, secondaryFast *dedup.SecondaryDeduper, secondaryMed *dedup.SecondaryDeduper, secondarySlow *dedup.SecondaryDeduper, secondaryStage *atomic.Uint64, buf *buffer.RingBuffer, ctyLookup func() *cty.CTYDatabase, metaCache *callMetaCache, ctyState *ctyRefreshState, recentBandStore spot.RecentSupportStore, signalResolver *spot.SignalResolver, telnetSrv *telnet.Server, dash ui.Surface, gridStats *gridMetrics, gridDB *gridStoreHandle, fccDBPath string, pathPredictor *pathreliability.Predictor, voacapSSN pathreliability.VOACAPSSNProvider, voacapFallback voacapFallbackSnapshotProvider, modeAssigner *spot.ModeAssigner, toxicityClassifier *toxicity.Classifier, rbnClient *rbn.Client, rbnDigitalClient *rbn.Client, pskrClient *pskreporter.Client, dxsummitClient *dxsummit.Client, pskrPathOnly *pathOnlyStats, peerManager *peer.Manager, clusterCall string, skewPath string, ingestSourceCfg dashboardIngestSourceConfig) {
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

		rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4, rbnFT2 :=
			rbnIngestDeltas(sourceTotals, prevSourceCounts, sourceModeTotals, prevSourceModeCounts)

		// PSKReporter includes a per-mode breakdown in the stats ticker.
		pskTotal := diffCounter(sourceTotals, prevSourceCounts, "PSKREPORTER")
		pskCW := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "CW")
		pskRTTY := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "RTTY")
		pskFT8 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT8")
		pskFT4 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT4")
		pskFT2 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "FT2")
		pskMSK144 := diffSourceMode(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "MSK144")
		// PSK31/63 is tracked separately from canonical PSK to match Overview semantics.
		psk31_63 := diffSourceModes(sourceModeTotals, prevSourceModeCounts, "PSKREPORTER", "PSK31", "PSK63")
		// Peer ingest is summarized under the synthetic P92 label (stats key is PEER).
		p92Total := diffCounter(sourceTotals, prevSourceCounts, "PEER")

		totalCorrections := tracker.CallCorrections()
		totalUnlicensed := tracker.UnlicensedDrops()
		totalFreqCorrections := tracker.FrequencyCorrections()
		totalHarmonics := tracker.HarmonicSuppressions()
		reputationTotal := tracker.ReputationDrops()
		corrDecisionLine := formatCorrectionDecisionSummary(tracker)
		stabilizerLine := formatStabilizerSummary(tracker)
		stabilizerGlyphLine := formatStabilizerGlyphSummary(tracker)
		resolverLine := formatResolverSummary(signalResolver)
		resolverPressureLine := formatResolverPressureSummary(signalResolver)
		temporalLine := formatTemporalSummary(tracker)
		toxicityLine := formatToxicitySummary(toxicityClassifier)

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
		var preloginActive int64
		var preloginRejectGlobal, preloginRejectRate, preloginRejectConcurrency, preloginTimeouts uint64
		var bulletinDedupe telnet.BulletinDedupeSnapshot
		var clientCount int
		if telnetSrv != nil {
			queueDrops, clientDrops, senderFailures = telnetSrv.BroadcastMetricSnapshot()
			preloginActive, preloginRejectGlobal, preloginRejectRate, preloginRejectConcurrency, preloginTimeouts, _, _ = telnetSrv.PreloginMetricSnapshot()
			bulletinDedupe = telnetSrv.BulletinDedupeSnapshot()
			clientCount = telnetSrv.GetClientCount()
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

		rbnCWLive := rbnClient != nil && rbnClient.HealthSnapshot().Connected
		rbnFTLive := rbnDigitalClient != nil && rbnDigitalClient.HealthSnapshot().Connected
		rbnLive := rbnFeedFamilyLive(rbnCWLive, rbnFTLive)
		pskLive := pskReporterLive(pskSnap, now)
		dxsummitSnap := dxsummit.HealthSnapshot{}
		if dxsummitClient != nil {
			dxsummitSnap = dxsummitClient.HealthSnapshot()
		}
		dxsummitLive := dxsummitIsLive(dxsummitSnap, ingestSourceCfg.DXSummitPollIntervalSeconds, now)
		if pathPredictor != nil {
			pathPredictor.RefreshStatsSnapshot(now)
		}
		peerSessions := 0
		var peerSSIDs []string
		if peerManager != nil {
			peerSessions = peerManager.ActiveSessionCount()
			peerSSIDs = peerManager.ActiveSessionSSIDs()
		}
		p92Live := peerSessions > 0
		ingestSources := dashboardIngestSources(ingestSourceCfg, rbnCWLive, rbnFTLive, pskLive, dxsummitLive, p92Live, peerSessions, peerSSIDs)

		lines := make([]string, 0, 11)
		lines = append(lines,
			fmt.Sprintf("%s   %s", formatUptimeLine(tracker.GetUptime()), formatMemoryLine(buf, dedup, secondaryFast, secondaryMed, secondarySlow, metaCache)), // 1
			formatGridLineOrPlaceholder(gridStats, gridDB, pathPredictor),                                                                                      // 2
			formatDataLineOrPlaceholder(ctyLookup, ctyState, fccSnap),                                                                                          // 3
			fmt.Sprintf("%s: %d TOTAL / %d CW / %d RTTY / %d FT8 / %d FT4 / %d FT2",
				withIngestStatusLabel("RBN", rbnLive), combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4, rbnFT2), // 4
			fmt.Sprintf("%s: %s TOTAL / %s CW / %s RTTY / %s FT8 / %s FT4 / %s FT2 / %s MSK144",
				withIngestStatusLabel("PSKReporter", pskLive),
				humanize.Comma(int64(pskTotal)),
				humanize.Comma(int64(pskCW)),
				humanize.Comma(int64(pskRTTY)),
				humanize.Comma(int64(pskFT8)),
				humanize.Comma(int64(pskFT4)),
				humanize.Comma(int64(pskFT2)),
				humanize.Comma(int64(pskMSK144)),
			), // 5
			fmt.Sprintf("%s: %s TOTAL", withIngestStatusLabel("P92", p92Live), humanize.Comma(int64(p92Total))), // 6
		)
		lines[4] += fmt.Sprintf(" / %s PSK", humanize.Comma(int64(psk31_63)))
		lines = append(lines, pathOnlyLine)
		lines = append(lines,
			fmt.Sprintf("Calls: %d (C) / %d (U) / %d (F) / %d (H) / %d (R)", totalCorrections, totalUnlicensed, totalFreqCorrections, totalHarmonics, reputationTotal), // 6
			corrDecisionLine,
			"",
			resolverLine,
			resolverPressureLine,
			"",
			stabilizerLine,
			stabilizerGlyphLine,
			"",
			temporalLine,
			toxicityLine,
			pipelineLine, // 7
			fmt.Sprintf("Telnet: %d clients. Drops: %d (Q) / %d (C) / %d (W). Bulletins: %d accepted / %d suppressed / %d evicted / %d tracked. Prelogin: %d active / rejects %d (G) %d (R) %d (C) / %d (T)", clientCount, queueDrops, clientDrops, senderFailures, bulletinDedupe.Accepted, bulletinDedupe.Suppressed, bulletinDedupe.Evicted, bulletinDedupe.Tracked, preloginActive, preloginRejectGlobal, preloginRejectRate, preloginRejectConcurrency, preloginTimeouts), // 8
		)

		prevSourceCounts = sourceTotals
		prevSourceModeCounts = sourceModeTotals

		if dash != nil {
			dash.SetStats(lines)
			overviewLines := buildOverviewLines(tracker, dedup, secondaryFast, secondaryMed, secondarySlow, metaCache, recentBandStore, ctyState, fccSnap, gridStats, gridDB, pathPredictor, voacapSSN, voacapFallback, modeAssigner, telnetSrv, clusterCall,
				rbnLive, pskLive, p92Live,
				ingestSources,
				combinedRBN, rbnCW, rbnRTTY, rbnFT8, rbnFT4, rbnFT2,
				pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskFT2, pskMSK144, psk31_63,
				p92Total,
				totalCorrections, totalUnlicensed, totalHarmonics, reputationTotal,
				pathOnlyLine,
				skewPath,
				&mem,
				gcP99Label,
			)
			snapshot := ui.Snapshot{
				GeneratedAt:   time.Now().UTC(),
				OverviewLines: overviewLines,
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

// forwardSpots pushes source spots into ingest with optional per-spot transform.
// It drops nil and stale spots, performs non-blocking enqueue, and rate-limits drop logs.
func forwardSpots(
	spotChan <-chan *spot.Spot,
	ingest chan<- *spot.Spot,
	label string,
	spotPolicy config.SpotPolicy,
	transform func(*spot.Spot),
) {
	drops := ratelimit.NewCounter(ingestForwardDropLogInterval)
	for s := range spotChan {
		if s == nil {
			continue
		}
		if transform != nil {
			transform(s)
		}
		if isStale(s, spotPolicy) {
			continue
		}
		select {
		case ingest <- s:
		default:
			if count, ok := drops.Inc(); ok {
				log.Printf("%s: Ingest input full, dropping spot (total drops=%d)", label, count)
			}
		}
	}
	log.Printf("%s: Spot processing stopped", label)
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
		cells := hydrateSpotPathCells(s)
		if (cells.dxFine == pathreliability.InvalidCell || cells.deFine == pathreliability.InvalidCell) &&
			(cells.dxCoarse == pathreliability.InvalidCell || cells.deCoarse == pathreliability.InvalidCell) {
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
			pathReport.ObserveWithCells(s, spotTime, cells.deCoarse, cells.dxCoarse)
		}
		// Spot SNR reflects DX -> DE (spotter is the receiver).
		predictor.UpdateWithReceiverHash(bucket, cells.deFine, cells.dxFine, cells.deCoarse, cells.dxCoarse, band, ft8, 1.0, spotTime, s.IsBeacon, pathReceiverHash(s))
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
	secondaryStage *atomic.Uint64,
	modeAssigner *spot.ModeAssigner,
	buf *buffer.RingBuffer,
	telnet *telnet.Server,
	peerManager *peer.Manager,
	tracker *stats.Tracker,
	signalResolver *spot.SignalResolver,
	correctionCfg config.CallCorrectionConfig,
	ctyLookup func() *cty.CTYDatabase,
	metaCache *callMetaCache,
	harmonicDetector *spot.HarmonicDetector,
	harmonicCfg config.HarmonicConfig,
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
	dash ui.Surface,
	gridUpdate func(call, grid string),
	gridLookup func(call string) (string, bool, bool),
	gridLookupSync func(call string) (string, bool, bool),
	unlicensedReporter func(source, role, call, deCall, dxCall, mode string, freq float64),
	droppedCallLogger *droppedCallLogger,
	adaptiveMinReports *spot.AdaptiveMinReports,
	refresher *adaptiveRefresher,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore spot.RecentSupportStore,
	customSCPStore *spot.CustomSCPStore,
	whoSpotsMeStore *spot.WhoSpotsMeStore,
	broadcastKeepSSID bool,
	archiveWriter *archive.Writer,
	lastOutput *atomic.Int64,
	pathPredictor *pathreliability.Predictor,
	pathReport *pathReportMetrics,
	allowedBands map[string]struct{},
	toxicityClassifier *toxicity.Classifier,
) {
	newOutputPipeline(
		deduplicator,
		secondaryFast,
		secondaryMed,
		secondarySlow,
		secondaryStage,
		modeAssigner,
		buf,
		telnet,
		peerManager,
		tracker,
		signalResolver,
		correctionCfg,
		ctyLookup,
		metaCache,
		harmonicDetector,
		harmonicCfg,
		freqAvg,
		spotPolicy,
		dash,
		gridUpdate,
		gridLookup,
		gridLookupSync,
		unlicensedReporter,
		droppedCallLogger,
		adaptiveMinReports,
		refresher,
		spotterReliability,
		spotterReliabilityCW,
		spotterReliabilityRTTY,
		confusionModel,
		recentBandStore,
		customSCPStore,
		whoSpotsMeStore,
		broadcastKeepSSID,
		archiveWriter,
		lastOutput,
		pathPredictor,
		pathReport,
		allowedBands,
		toxicityClassifier,
	).run()
}

func cloneSpotForTelnetStabilizer(s *spot.Spot) *spot.Spot {
	if s == nil {
		return nil
	}
	return s.Clone()
}

type stabilizerDelayDecision = correctionflow.StabilizerDelayDecision

const (
	stabilizerDelayReasonNone               = correctionflow.StabilizerDelayReasonNone
	stabilizerDelayReasonUnknownOrNonRecent = correctionflow.StabilizerDelayReasonUnknownOrNonRecent
	stabilizerDelayReasonAmbiguous          = correctionflow.StabilizerDelayReasonAmbiguous
	stabilizerDelayReasonPLowConfidence     = correctionflow.StabilizerDelayReasonPLowConfidence
	stabilizerDelayReasonEditNeighbor       = correctionflow.StabilizerDelayReasonEditNeighbor
)

// shouldDelayTelnetByStabilizer is a compatibility wrapper used by tests and
// non-resolver call sites to evaluate baseline stabilizer delay eligibility.
func shouldDelayTelnetByStabilizer(s *spot.Spot, store spot.RecentSupportStore, cfg config.CallCorrectionConfig, now time.Time) bool {
	if spot.IsLocalSelfSpot(s) {
		return false
	}
	return correctionflow.ShouldDelayTelnetByStabilizer(s, store, cfg, now)
}

func evaluateTelnetStabilizerDelay(
	s *spot.Spot,
	store spot.RecentSupportStore,
	cfg config.CallCorrectionConfig,
	now time.Time,
	resolverSnapshot spot.ResolverSnapshot,
	resolverSnapshotOK bool,
) stabilizerDelayDecision {
	if spot.IsLocalSelfSpot(s) {
		return stabilizerDelayDecision{Reason: stabilizerDelayReasonNone}
	}
	return correctionflow.EvaluateStabilizerDelay(s, store, cfg, now, resolverSnapshot, resolverSnapshotOK)
}

func stabilizerReleaseReason(decision stabilizerDelayDecision, prior string) string {
	return correctionflow.StabilizerReleaseReason(decision, prior)
}

// shouldRetryTelnetByStabilizer reports whether a delayed spot should be held
// for another stabilizer cycle based on the current policy decision.
func shouldRetryTelnetByStabilizer(decision stabilizerDelayDecision, checksCompleted int) bool {
	return correctionflow.ShouldRetryStabilizerDelay(decision, checksCompleted)
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

func startPathPredictionLogger(ctx context.Context, propLog lineSink, srv *telnet.Server, predictor *pathreliability.Predictor, pathReport *pathReportMetrics) {
	// Purpose: Periodically report path prediction outcomes, bucket counts, and weight histograms.
	// Key aspects: Writes only to the propagation file sink, uses atomic snapshot/reset, and exits on context cancellation.
	// Upstream: main startup.
	// Downstream: telnet.Server.PathPredictionStatsSnapshot, pathreliability.Predictor stats/histograms.
	if propLog == nil || (srv == nil && predictor == nil) {
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
				propLog.WriteLine(line, now)
			}
			if srv != nil {
				stats := srv.PathPredictionStatsSnapshot()
				if stats.Total > 0 {
					fileOnly(fmt.Sprintf(
						"Path predictions (5m): total=%s derived=%s combined=%s voacap_closed=%s voacap_aligned=%s voacap_sparse_upgrade=%s voacap_open=%s insufficient=%s no_sample=%s low_count=%s low_receiver=%s low_weight=%s stale=%s cap_limited=%s cap_would_block=%s beacon_rx=%s beacon_rx_insufficient=%s beacon_rx_no_sample=%s beacon_rx_low_count=%s beacon_rx_low_receiver=%s beacon_rx_low_weight=%s beacon_rx_stale=%s beacon_rx_voacap_closed=%s beacon_rx_voacap_aligned=%s beacon_rx_voacap_sparse_upgrade=%s beacon_rx_voacap_open=%s override_r=%s override_g=%s",
						humanize.Comma(int64(stats.Total)),
						humanize.Comma(int64(stats.Derived)),
						humanize.Comma(int64(stats.Combined)),
						humanize.Comma(int64(stats.VOACAPClosed)),
						humanize.Comma(int64(stats.VOACAPAligned)),
						humanize.Comma(int64(stats.VOACAPSparseUpgrade)),
						humanize.Comma(int64(stats.VOACAPOpen)),
						humanize.Comma(int64(stats.Insufficient)),
						humanize.Comma(int64(stats.NoSample)),
						humanize.Comma(int64(stats.LowCount)),
						humanize.Comma(int64(stats.LowReceiver)),
						humanize.Comma(int64(stats.LowWeight)),
						humanize.Comma(int64(stats.Stale)),
						humanize.Comma(int64(stats.CapLimited)),
						humanize.Comma(int64(stats.CapWouldBlock)),
						humanize.Comma(int64(stats.BeaconRX)),
						humanize.Comma(int64(stats.BeaconRXInsufficient)),
						humanize.Comma(int64(stats.BeaconRXNoSample)),
						humanize.Comma(int64(stats.BeaconRXLowCount)),
						humanize.Comma(int64(stats.BeaconRXLowReceiver)),
						humanize.Comma(int64(stats.BeaconRXLowWeight)),
						humanize.Comma(int64(stats.BeaconRXStale)),
						humanize.Comma(int64(stats.BeaconRXVOACAPClosed)),
						humanize.Comma(int64(stats.BeaconRXVOACAPAligned)),
						humanize.Comma(int64(stats.BeaconRXVOACAPSparseUpgrade)),
						humanize.Comma(int64(stats.BeaconRXVOACAPOpen)),
						humanize.Comma(int64(stats.OverrideR)),
						humanize.Comma(int64(stats.OverrideG)),
					))
				}
				fallback := stats.VOACAPFallback
				if fallback.HasActivity() ||
					stats.VOACAPFallbackClosedCandidate != 0 ||
					stats.VOACAPFallbackClosedNoP50 != 0 ||
					stats.VOACAPFallbackClosedSparseP50 != 0 ||
					stats.VOACAPFallbackClosedSparseHigh != 0 ||
					stats.VOACAPFallbackClosedSparseMed != 0 ||
					stats.VOACAPFallbackClosedSparseLow != 0 ||
					stats.VOACAPFallbackClosedSparseUnlk != 0 ||
					stats.VOACAPFallbackAlignedCandidate != 0 ||
					stats.VOACAPFallbackOpenNoP50 != 0 ||
					stats.VOACAPFallbackClassMismatch != 0 ||
					stats.VOACAPFallbackSparseUpgrade != 0 ||
					stats.VOACAPFallbackOpenNoP50REL != 0 ||
					stats.VOACAPFallbackReliabilityMissing != 0 ||
					stats.VOACAPFallbackReliabilityBelow != 0 ||
					stats.VOACAPFallbackReliabilityMultiTier != 0 {
					fileOnly(fmt.Sprintf(
						"VOACAP fallback (5m): queued=%s success=%s failure=%s cache_hit=%s no_current_hour=%s delay_wait=%s inflight=%s queue_full=%s not_running=%s ssn_unavailable=%s invalid_request=%s invalid_unsupported_band=%s invalid_empty_unknown_band=%s invalid_user_grid=%s invalid_dx_grid=%s invalid_user_cell=%s invalid_dx_cell=%s closed=%s closed_no_p50=%s closed_with_sparse_p50=%s closed_with_sparse_p50_class_high=%s closed_with_sparse_p50_class_medium=%s closed_with_sparse_p50_class_low=%s closed_with_sparse_p50_class_unlikely=%s aligned=%s open_no_p50=%s class_mismatch=%s sparse_upgrade=%s open_no_p50_rel=%s rel_missing=%s rel_below_floor=%s rel_multi_tier=%s",
						humanize.Comma(fallback.Queued),
						humanize.Comma(fallback.RunSuccess),
						humanize.Comma(fallback.RunFailure),
						humanize.Comma(fallback.CacheHit),
						humanize.Comma(fallback.NoCurrentHour),
						humanize.Comma(fallback.DelayWait),
						humanize.Comma(fallback.Inflight),
						humanize.Comma(fallback.QueueFull),
						humanize.Comma(fallback.NotRunning),
						humanize.Comma(fallback.SSNUnavailable),
						humanize.Comma(fallback.InvalidRequest),
						humanize.Comma(fallback.InvalidUnsupportedBand),
						humanize.Comma(fallback.InvalidEmptyUnknownBand),
						humanize.Comma(fallback.InvalidUserGrid),
						humanize.Comma(fallback.InvalidDXGrid),
						humanize.Comma(fallback.InvalidUserCell),
						humanize.Comma(fallback.InvalidDXCell),
						humanize.Comma(stats.VOACAPFallbackClosedCandidate),
						humanize.Comma(stats.VOACAPFallbackClosedNoP50),
						humanize.Comma(stats.VOACAPFallbackClosedSparseP50),
						humanize.Comma(stats.VOACAPFallbackClosedSparseHigh),
						humanize.Comma(stats.VOACAPFallbackClosedSparseMed),
						humanize.Comma(stats.VOACAPFallbackClosedSparseLow),
						humanize.Comma(stats.VOACAPFallbackClosedSparseUnlk),
						humanize.Comma(stats.VOACAPFallbackAlignedCandidate),
						humanize.Comma(stats.VOACAPFallbackOpenNoP50),
						humanize.Comma(stats.VOACAPFallbackClassMismatch),
						humanize.Comma(stats.VOACAPFallbackSparseUpgrade),
						humanize.Comma(stats.VOACAPFallbackOpenNoP50REL),
						humanize.Comma(stats.VOACAPFallbackReliabilityMissing),
						humanize.Comma(stats.VOACAPFallbackReliabilityBelow),
						humanize.Comma(stats.VOACAPFallbackReliabilityMultiTier),
					))
				}
				sparse := stats.SparseP50VOACAP
				if sparse.HasActivity() {
					fileOnly(fmt.Sprintf(
						"Sparse p50 VOACAP (5m): total=%s no_p50=%s very_low_count=%s beacon_rx=%s non_beacon=%s cache_miss_total=%s cache_hit=%s queued=%s delayed=%s inflight=%s invalid_request=%s invalid_unsupported_band=%s invalid_empty_unknown_band=%s invalid_user_grid=%s invalid_dx_grid=%s invalid_user_cell=%s invalid_dx_cell=%s ssn_unavailable=%s no_current_hour=%s queue_full=%s not_running=%s disabled=%s unavailable=%s closed=%s aligned=%s sparse_upgrade=%s open_rel_pass=%s open_rel_fail=%s not_closed=%s rel_missing=%s rel_below_floor=%s rel_multi_tier=%s",
						humanize.Comma(sparse.Total),
						humanize.Comma(sparse.NoP50),
						humanize.Comma(sparse.VeryLowCount),
						humanize.Comma(sparse.BeaconRX),
						humanize.Comma(sparse.NonBeacon),
						humanize.Comma(sparse.CacheMissTotal),
						humanize.Comma(sparse.CacheHit),
						humanize.Comma(sparse.Queued),
						humanize.Comma(sparse.Delayed),
						humanize.Comma(sparse.Inflight),
						humanize.Comma(sparse.InvalidRequest),
						humanize.Comma(sparse.InvalidUnsupportedBand),
						humanize.Comma(sparse.InvalidEmptyUnknownBand),
						humanize.Comma(sparse.InvalidUserGrid),
						humanize.Comma(sparse.InvalidDXGrid),
						humanize.Comma(sparse.InvalidUserCell),
						humanize.Comma(sparse.InvalidDXCell),
						humanize.Comma(sparse.SSNUnavailable),
						humanize.Comma(sparse.NoCurrentHour),
						humanize.Comma(sparse.QueueFull),
						humanize.Comma(sparse.NotRunning),
						humanize.Comma(sparse.Disabled),
						humanize.Comma(sparse.Unavailable),
						humanize.Comma(sparse.Closed),
						humanize.Comma(sparse.Aligned),
						humanize.Comma(sparse.SparseUpgrade),
						humanize.Comma(sparse.OpenRELPass),
						humanize.Comma(sparse.OpenRELFail),
						humanize.Comma(sparse.NotClosed),
						humanize.Comma(sparse.RELMissing),
						humanize.Comma(sparse.RELBelowFloor),
						humanize.Comma(sparse.RELMultiTier),
					))
				}
				if stats.VOACAPP50CompareChecked != 0 {
					fileOnly(fmt.Sprintf(
						"VOACAP p50 compare (5m): checked=%s cache_hit=%s cache_miss=%s same_class=%s p50_stronger=%s voacap_stronger=%s equal_snr=%s voacap_closed_p50_high=%s voacap_closed_p50_medium=%s voacap_closed_p50_low=%s voacap_closed_p50_unlikely=%s delta_abs_0_3=%s delta_abs_4_9=%s delta_abs_10_19=%s delta_abs_20_plus=%s",
						humanize.Comma(stats.VOACAPP50CompareChecked),
						humanize.Comma(stats.VOACAPP50CompareCacheHit),
						humanize.Comma(stats.VOACAPP50CompareCacheMiss),
						humanize.Comma(stats.VOACAPP50CompareSameClass),
						humanize.Comma(stats.VOACAPP50CompareP50Stronger),
						humanize.Comma(stats.VOACAPP50CompareVOACAPStronger),
						humanize.Comma(stats.VOACAPP50CompareEqualSNR),
						humanize.Comma(stats.VOACAPP50CompareClosedP50High),
						humanize.Comma(stats.VOACAPP50CompareClosedP50Med),
						humanize.Comma(stats.VOACAPP50CompareClosedP50Low),
						humanize.Comma(stats.VOACAPP50CompareClosedP50Unlk),
						humanize.Comma(stats.VOACAPP50CompareDeltaAbs0To3),
						humanize.Comma(stats.VOACAPP50CompareDeltaAbs4To9),
						humanize.Comma(stats.VOACAPP50CompareDeltaAbs10To19),
						humanize.Comma(stats.VOACAPP50CompareDeltaAbs20Plus),
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
// source identity (e.g., N2WQ-1-# -> N2WQ-#, N2WQ-1 -> N2WQ).
// It preserves non-numeric suffixes.
// Purpose: Normalize spotter SSIDs before telnet broadcast.
// Key aspects: Collapses numeric suffixes while preserving known source markers.
// Upstream: processOutputSpots.
// Downstream: stripNumericSSID.
func collapseSSIDForBroadcast(call string) string {
	call = strings.TrimSpace(call)
	if call == "" {
		return call
	}
	if marker := knownSourceMarkerSuffix(call); marker != "" {
		trimmed := strings.TrimSuffix(call, marker)
		return stripNumericSSID(trimmed) + marker
	}
	return stripNumericSSID(call)
}

func knownSourceMarkerSuffix(call string) string {
	switch {
	case strings.HasSuffix(call, "-#"):
		return "-#"
	case strings.HasSuffix(call, "-@"):
		return "-@"
	default:
		return ""
	}
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
	call = strutil.NormalizeUpper(call)
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

// peerPublishComment ensures manual spots carry an inferred mode to peers even
// when the user omitted a comment. Peers only see the comment field in PC61/PC11
// frames, so we fall back to the inferred mode when the comment is blank.
func peerPublishComment(src *spot.Spot) string {
	if src == nil {
		return ""
	}
	comment := strings.TrimSpace(src.Comment)
	if src.SourceType != spot.SourceManual {
		return comment
	}
	if comment != "" {
		return comment
	}
	mode := strings.TrimSpace(src.Mode)
	if mode == "" {
		return comment
	}
	return mode
}

// applyLicenseGate runs the FCC license check after all corrections and returns true when the spot should be dropped.
// Purpose: Enforce FCC ULS licensing gates for US base calls (DX only; DE checked at ingest).
// Key aspects: Jurisdiction is derived from the normalized base call; reporter callback on drops.
// Upstream: processOutputSpots before broadcast.
// Downstream: uls.IsLicensedUS, reporter.
func applyLicenseGate(s *spot.Spot, ctyDB *cty.CTYDatabase, metaCache *callMetaCache, reporter func(source, role, call, deCall, dxCall, mode string, freq float64)) bool {
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
				reporter(droppedCallSourceFromSpot(s), "DX", callKey, deCall, dxCall, droppedCallModeFromSpot(s), s.Frequency)
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
		Continent:  info.Continent,
		Country:    info.Country,
		CQZone:     info.CQZone,
		IARURegion: spot.ResolveIARURegion(info.ADIF, info.Continent),
		ITUZone:    info.ITUZone,
		ADIF:       info.ADIF,
	}
}

func mergeMetadataPreserveGrid(current spot.CallMetadata, info *cty.PrefixInfo) spot.CallMetadata {
	next := metadataFromPrefix(info)
	if grid := strings.TrimSpace(current.Grid); grid != "" {
		next.Grid = grid
		next.GridDerived = current.GridDerived
	}
	return next
}

func hydrateDXMetadataForModeInference(s *spot.Spot, ctyDB *cty.CTYDatabase, metaCache *callMetaCache) {
	if s == nil || ctyDB == nil {
		return
	}
	if s.DXMetadata.ADIF > 0 && s.DXMetadata.IARURegion != spot.IARURegionUnknown {
		return
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	call = normalizeCallForMetadata(call)
	if call == "" {
		return
	}
	info := effectivePrefixInfo(ctyDB, metaCache, call)
	if info == nil {
		return
	}
	s.DXMetadata = mergeMetadataPreserveGrid(s.DXMetadata, info)
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
}

const (
	resolverDecisionPathPrimary                   = "resolver_primary"
	resolverDecisionApplied                       = "resolver_applied"
	resolverDecisionAppliedNeighbor               = "resolver_applied_neighbor_override"
	resolverDecisionAppliedRecentPlus1            = "resolver_applied_recent_plus1"
	resolverDecisionAppliedNeighborRecentPlus1    = "resolver_applied_neighbor_recent_plus1"
	resolverDecisionAppliedBayesReport            = "resolver_applied_bayes_report"
	resolverDecisionAppliedNeighborBayesReport    = "resolver_applied_neighbor_bayes_report"
	resolverDecisionAppliedBayesAdvantage         = "resolver_applied_bayes_advantage"
	resolverDecisionAppliedNeighborBayesAdvantage = "resolver_applied_neighbor_bayes_advantage"
	resolverDecisionNoSnapshot                    = "resolver_no_snapshot"
	resolverDecisionNeighborConflict              = "resolver_neighbor_conflict"
	resolverDecisionStateSplit                    = "resolver_state_split"
	resolverDecisionStateUncertain                = "resolver_state_uncertain"
	resolverDecisionStateUnknown                  = "resolver_state_unknown"
	resolverDecisionPrecallMissing                = "resolver_precall_missing"
	resolverDecisionWinnerMissing                 = "resolver_winner_missing"
	resolverDecisionSameCall                      = "resolver_same_call"
	resolverDecisionInvalidBaseCall               = "resolver_invalid_base"
	resolverDecisionCTYMiss                       = "resolver_cty_miss"
	resolverDecisionGatePrefix                    = "resolver_gate_"
	resolverDecisionRecentPlus1RejectPrefix       = "resolver_recent_plus1_reject_"
	resolverDecisionBayesReportRejectPrefix       = "resolver_bayes_report_reject_"
	resolverDecisionBayesAdvantageRejectPrefix    = "resolver_bayes_advantage_reject_"
	resolverRecentPlus1DisallowEditNeighborGate   = "edit_neighbor_contested"
)

func observeResolverPrimaryDecision(tracker *stats.Tracker, decision, reason string, candidateRank int) {
	if tracker == nil {
		return
	}
	decision = strings.ToLower(strings.TrimSpace(decision))
	if decision == "" {
		decision = "rejected"
	}
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = "unknown"
	}
	if candidateRank < 0 {
		candidateRank = 0
	}
	tracker.ObserveCallCorrectionDecision(resolverDecisionPathPrimary, decision, reason, candidateRank)
}

func resolverGateDecisionReason(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = "unknown"
	}
	return resolverDecisionGatePrefix + reason
}

func resolverRecentPlus1DecisionReason(gate spot.ResolverPrimaryGateResult) (string, bool) {
	if !gate.RecentPlus1Considered || gate.RecentPlus1Applied {
		return "", false
	}
	reject := strings.ToLower(strings.TrimSpace(gate.RecentPlus1Reject))
	if reject == "" {
		return "", false
	}
	return resolverDecisionRecentPlus1RejectPrefix + reject, true
}

func resolverBayesDecisionReason(gate spot.ResolverPrimaryGateResult) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(gate.Reason)) {
	case "min_reports":
		if !gate.BayesReportBonusConsidered || gate.BayesReportBonusApplied {
			return "", false
		}
		reject := strings.ToLower(strings.TrimSpace(gate.BayesReportBonusReject))
		if reject == "" {
			return "", false
		}
		return resolverDecisionBayesReportRejectPrefix + reject, true
	case "advantage":
		if !gate.BayesAdvantageConsidered || gate.BayesAdvantageApplied {
			return "", false
		}
		reject := strings.ToLower(strings.TrimSpace(gate.BayesAdvantageReject))
		if reject == "" {
			return "", false
		}
		return resolverDecisionBayesAdvantageRejectPrefix + reject, true
	default:
		return "", false
	}
}

func resolverAppliedDecisionReason(gate spot.ResolverPrimaryGateResult, selection correctionflow.ResolverPrimarySelection) string {
	switch {
	case gate.BayesAdvantageApplied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborBayesAdvantage
	case gate.BayesAdvantageApplied:
		return resolverDecisionAppliedBayesAdvantage
	case gate.BayesReportBonusApplied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborBayesReport
	case gate.BayesReportBonusApplied:
		return resolverDecisionAppliedBayesReport
	case gate.RecentPlus1Applied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborRecentPlus1
	case gate.RecentPlus1Applied:
		return resolverDecisionAppliedRecentPlus1
	case selection.WinnerOverride:
		return resolverDecisionAppliedNeighbor
	default:
		return resolverDecisionApplied
	}
}

// Purpose: Apply resolver-primary correction behavior from the latest resolver snapshot.
// Key aspects: Uses split/uncertain as no-correction, maps confidence to emitted-call
// semantics, and applies C/B/suppress rails consistent with existing correction behavior.
// Upstream: processOutputSpots (primary mode) and stabilizer release path.
// Downstream: SignalResolver lookup, CTY validation, tracker counters, UI/log messages.
func maybeApplyResolverCorrection(
	spotEntry *spot.Spot,
	resolver *spot.SignalResolver,
	evidence spot.ResolverEvidence,
	hasEvidence bool,
	cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	metaCache *callMetaCache,
	tracker *stats.Tracker,
	dash ui.Surface,
	recentBandStore spot.RecentSupportStore,
	adaptive *spot.AdaptiveMinReports,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
) bool {
	return maybeApplyResolverCorrectionWithSelectionOverride(
		spotEntry,
		resolver,
		evidence,
		hasEvidence,
		cfg,
		ctyDB,
		metaCache,
		tracker,
		dash,
		recentBandStore,
		adaptive,
		spotterReliability,
		spotterReliabilityCW,
		spotterReliabilityRTTY,
		confusionModel,
		nil,
	)
}

//nolint:unparam // Reliability/model parameters preserve the runtime correction-call signature while temporal selection overrides reuse the same path.
func maybeApplyResolverCorrectionWithSelectionOverride(
	spotEntry *spot.Spot,
	resolver *spot.SignalResolver,
	evidence spot.ResolverEvidence,
	hasEvidence bool,
	cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	metaCache *callMetaCache,
	tracker *stats.Tracker,
	dash ui.Surface,
	recentBandStore spot.RecentSupportStore,
	adaptive *spot.AdaptiveMinReports,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	selectionOverride *correctionflow.ResolverPrimarySelection,
) bool {
	if spotEntry == nil {
		return false
	}
	if spot.IsLocalSelfSpot(spotEntry) {
		spotEntry.Confidence = "V"
		return false
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_non_candidate_mode", 0)
		return false
	}
	if !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_disabled", 0)
		return false
	}

	preCorrectionCall := normalizedDXCall(spotEntry)
	if preCorrectionCall == "" {
		spotEntry.Confidence = "?"
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionPrecallMissing, 0)
		return false
	}

	selection := correctionflow.ResolverPrimarySelection{}
	snapshot := spot.ResolverSnapshot{}
	snapshotOK := false
	if selectionOverride != nil {
		selection = *selectionOverride
		snapshot = selection.Snapshot
		snapshotOK = selection.SnapshotOK
	} else if hasEvidence && resolver != nil {
		selection = correctionflow.SelectResolverPrimarySnapshotForCall(resolver, evidence.Key, cfg, preCorrectionCall)
		snapshot = selection.Snapshot
		snapshotOK = selection.SnapshotOK
	}
	setResolverConfidence(spotEntry, snapshot, snapshotOK, preCorrectionCall)
	if !snapshotOK {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNoSnapshot, 0)
		return false
	}
	if selection.NeighborhoodSplit {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNeighborConflict, 0)
		return false
	}

	switch snapshot.State {
	case spot.ResolverStateConfident, spot.ResolverStateProbable:
	case spot.ResolverStateSplit:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateSplit, 0)
		return false
	case spot.ResolverStateUncertain:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUncertain, 0)
		return false
	default:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUnknown, 0)
		return false
	}

	winnerCall := spot.NormalizeSpotDXCallsign(snapshot.Winner)
	if winnerCall == "" {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionWinnerMissing, 1)
		return false
	}
	if strings.EqualFold(winnerCall, preCorrectionCall) {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionSameCall, 1)
		return false
	}

	subjectSupport := correctionflow.ResolverSupportForCall(snapshot, preCorrectionCall)
	winnerSupport := correctionflow.ResolverSupportForCall(snapshot, winnerCall)
	subjectWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snapshot, preCorrectionCall)
	winnerWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snapshot, winnerCall)
	winnerConfidence := correctionflow.ResolverWinnerConfidence(snapshot)
	subjectMode := spotEntry.ModeNorm
	if subjectMode == "" {
		subjectMode = spotEntry.Mode
	}
	subjectBand := spotEntry.BandNorm
	if subjectBand == "" || subjectBand == "???" {
		subjectBand = spot.FreqToBand(spotEntry.Frequency)
	}

	now := time.Now().UTC()
	runtime := correctionflow.ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, true)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:             cfg,
		MinReports:      runtime.MinReports,
		Window:          runtime.Window,
		FreqToleranceHz: runtime.FreqToleranceHz,
		RecentBandStore: recentBandStore,
	})
	gateOptions := spot.ResolverPrimaryGateOptions{}
	if cfg.ResolverRecentPlus1Enabled || cfg.BayesBonus.Enabled {
		if spot.ResolverSnapshotHasComparableEditNeighbor(snapshot, winnerCall, subjectMode, cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
			gateOptions.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
		}
	}
	gate := spot.EvaluateResolverPrimaryGates(
		preCorrectionCall,
		winnerCall,
		subjectBand,
		subjectMode,
		subjectSupport,
		winnerSupport,
		winnerConfidence,
		subjectWeightedSupport,
		winnerWeightedSupport,
		settings,
		now,
		gateOptions,
	)
	if !gate.Allow {
		reason := resolverGateDecisionReason(gate.Reason)
		if bayesReason, ok := resolverBayesDecisionReason(gate); ok {
			reason = bayesReason
		} else if plusReason, ok := resolverRecentPlus1DecisionReason(gate); ok {
			reason = plusReason
		}
		observeResolverPrimaryDecision(tracker, "rejected", reason, 1)
		return false
	}

	correctionMsg := formatCallCorrectedMessage(preCorrectionCall, winnerCall, spotEntry.Frequency, winnerSupport, winnerConfidence)
	if shouldRejectCTYCall(winnerCall) {
		log.Printf("Resolver correction rejected (invalid base call): suggested %s at %.1f kHz", winnerCall, spotEntry.Frequency)
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionInvalidBaseCall, 1)
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			log.Printf("Resolver correction suppression engaged: dropping spot from %s at %.1f kHz", preCorrectionCall, spotEntry.Frequency)
			return true
		}
		spotEntry.Confidence = "B"
		spotEntry.ConfidencePercent = winnerConfidence
		spotEntry.ConfidencePercentOK = true
		return false
	}

	if ctyDB != nil {
		if info := effectivePrefixInfo(ctyDB, metaCache, winnerCall); info != nil {
			if dash != nil {
				dash.AppendCall(correctionMsg)
			} else {
				log.Println(correctionMsg)
			}
			spotEntry.DXCall = winnerCall
			spotEntry.DXCallNorm = winnerCall
			spotEntry.Confidence = "C"
			spotEntry.ConfidencePercent = winnerConfidence
			spotEntry.ConfidencePercentOK = true
			if tracker != nil {
				tracker.IncrementCallCorrections()
			}
			appliedReason := resolverAppliedDecisionReason(gate, selection)
			observeResolverPrimaryDecision(tracker, "applied", appliedReason, 1)
		} else {
			log.Printf("Resolver correction rejected (CTY miss): suggested %s at %.1f kHz", winnerCall, spotEntry.Frequency)
			observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionCTYMiss, 1)
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				log.Printf("Resolver correction suppression engaged: dropping spot from %s at %.1f kHz", preCorrectionCall, spotEntry.Frequency)
				return true
			}
			spotEntry.Confidence = "B"
			spotEntry.ConfidencePercent = winnerConfidence
			spotEntry.ConfidencePercentOK = true
		}
		return false
	}

	if dash != nil {
		dash.AppendCall(correctionMsg)
	} else {
		log.Println(correctionMsg)
	}
	spotEntry.DXCall = winnerCall
	spotEntry.DXCallNorm = winnerCall
	spotEntry.Confidence = "C"
	spotEntry.ConfidencePercent = winnerConfidence
	spotEntry.ConfidencePercentOK = true
	if tracker != nil {
		tracker.IncrementCallCorrections()
	}
	appliedReason := resolverAppliedDecisionReason(gate, selection)
	observeResolverPrimaryDecision(tracker, "applied", appliedReason, 1)
	return false
}

func resolverConfidenceGlyph(snapshot spot.ResolverSnapshot, snapshotOK bool, emittedCall string) string {
	return correctionflow.ResolverConfidenceGlyphForCall(snapshot, snapshotOK, emittedCall)
}

func setResolverConfidence(s *spot.Spot, snapshot spot.ResolverSnapshot, snapshotOK bool, emittedCall string) {
	if s == nil {
		return
	}
	s.Confidence = resolverConfidenceGlyph(snapshot, snapshotOK, emittedCall)
	if percent, ok := correctionflow.ResolverCallConfidencePercent(snapshot, emittedCall); snapshotOK && ok {
		s.ConfidencePercent = percent
		s.ConfidencePercentOK = true
	} else {
		s.ConfidencePercent = 0
		s.ConfidencePercentOK = false
	}
}

// Purpose: Compute the time window for call correction recency.
// Key aspects: Uses config recency defaults and overrides.
// Upstream: main correctionIndex cleanup scheduling.
// Downstream: time.Duration math.
func buildResolverEvidenceSnapshot(
	spotEntry *spot.Spot,
	cfg config.CallCorrectionConfig,
	adaptive *spot.AdaptiveMinReports,
	now time.Time,
) (spot.ResolverEvidence, bool) {
	return correctionflow.BuildResolverEvidenceSnapshot(spotEntry, cfg, adaptive, now)
}

func normalizedDXCall(s *spot.Spot) string {
	return correctionflow.NormalizedDXCall(s)
}

func buildCorrectionSettings(
	cfg config.CallCorrectionConfig,
	minReports int,
	window time.Duration,
	freqToleranceHz float64,
	recentBandStore spot.RecentSupportStore,
) spot.CorrectionSettings {
	return correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:             cfg,
		MinReports:      minReports,
		Window:          window,
		FreqToleranceHz: freqToleranceHz,
		RecentBandStore: recentBandStore,
	})
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

// Purpose: Decide whether a mode participates in resolver-based confidence rails.
// Key aspects: Treats USB/LSB as voice modes; FT modes stay outside resolver mutation.
// Upstream: processOutputSpots confidence seeding and fallback.
// Downstream: strings.ToUpper/TrimSpace.
func modeSupportsResolverConfidenceGlyph(mode string) bool {
	switch strutil.NormalizeUpper(mode) {
	case "CW", "RTTY", "USB", "LSB":
		return true
	default:
		return false
	}
}

// Purpose: Report whether a mode uses the FT corroboration rail.
// Key aspects: Keeps FT confidence tagging separate from resolver call mutation.
// Upstream: FT hold/floor assignment.
// Downstream: strings.ToUpper/TrimSpace.
func modeSupportsFTConfidenceGlyph(mode string) bool {
	switch strutil.NormalizeUpper(mode) {
	case "FT2", "FT4", "FT8":
		return true
	default:
		return false
	}
}

// Purpose: Decide whether a mode can carry any confidence glyph.
// Key aspects: Resolver-capable modes and FT corroboration modes both qualify.
// Upstream: confidence floor helpers and filter contracts.
// Downstream: modeSupportsResolverConfidenceGlyph, modeSupportsFTConfidenceGlyph.
func modeSupportsConfidenceGlyph(mode string) bool {
	return modeSupportsResolverConfidenceGlyph(mode) || modeSupportsFTConfidenceGlyph(mode)
}

func stabilizerImmediateCountEligible(s *spot.Spot) bool {
	if s == nil {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	return spot.IsCallCorrectionCandidate(mode)
}

// recordRecentBandObservation records accepted spots for recent-on-band
// corroboration. This is intentionally post-gate (after correction/harmonic/
// license filters) so the cache favors calls that already survived core checks.
// Stabilizer-delayed spots are recorded only after delay resolution.
func recordRecentBandObservation(s *spot.Spot, store spot.RecentSupportStore, customSCP *spot.CustomSCPStore, cfg config.CallCorrectionConfig) {
	if s == nil || s.IsBeacon {
		return
	}
	if customSCP != nil && cfg.CustomSCP.Enabled {
		customSCP.RecordSpot(s)
		return
	}
	legacyStore, ok := store.(*spot.RecentBandStore)
	if !ok || legacyStore == nil {
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
	key, baseKey, ok := spot.CorrectionFamilyKeyPair(call)
	if !ok {
		key = call
	}
	if key != "" {
		legacyStore.Record(key, band, mode, spotter, seenAt)
	}
	if baseKey != "" {
		legacyStore.Record(baseKey, band, mode, spotter, seenAt)
	}
}

// recordWhoSpotsMeObservation records accepted observations for WHOSPOTSME
// using cluster acceptance time rather than source timestamps so the rolling
// window reflects what this node has recently admitted.
func recordWhoSpotsMeObservation(s *spot.Spot, store *spot.WhoSpotsMeStore, seenAt time.Time) {
	if s == nil || store == nil || s.IsBeacon || s.IsTestSpotter {
		return
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if call == "" {
		return
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	if band == "" || band == "???" {
		return
	}
	continent := s.DEContinentNorm
	if continent == "" {
		continent = s.DEMetadata.Continent
	}
	store.Record(call, band, s.DEMetadata.ADIF, continent, seenAt)
}

// Purpose: Apply confidence floor only when confidence is still unknown.
// Key aspects: If confidence is '?', upgrade to 'S' when DX call is in
// custom-SCP static membership or admitted by recent-on-band evidence.
// Upstream: processOutputSpots after confidence assignment.
// Downstream: custom SCP and RecentBandStore support checks plus modeSupportsConfidenceGlyph.
func applySupportFloor(
	s *spot.Spot,
	recentBandStore spot.RecentSupportStore,
	customSCPStore *spot.CustomSCPStore,
	ftRecentBandStore spot.RecentSupportStore,
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

	staticHit := customSCPStore != nil && corrCfg.CustomSCP.Enabled && customSCPStore.StaticContains(call)

	recentHit := false
	if modeSupportsFTConfidenceGlyph(mode) {
		if ftRecentBandStore != nil && corrCfg.RecentBandBonusEnabled {
			band := s.BandNorm
			if band == "" || band == "???" {
				band = spot.FreqToBand(s.Frequency)
			}
			recentHit = hasRecentSupportForCallFamily(ftRecentBandStore, call, band, mode, corrCfg.RecentBandRecordMinUniqueSpotters, time.Now().UTC())
		}
	} else if customSCPStore != nil && corrCfg.CustomSCP.Enabled {
		band := s.BandNorm
		if band == "" || band == "???" {
			band = spot.FreqToBand(s.Frequency)
		}
		now := time.Now().UTC()
		recentHit = customSCPStore.HasSFloorSupportExact(
			call,
			band,
			mode,
			corrCfg.CustomSCP.SFloorMinUniqueSpottersExact,
			now,
		)
		if !recentHit {
			key, baseKey, ok := spot.CorrectionFamilyKeyPair(call)
			if ok {
				recentHit = customSCPStore.HasSFloorSupportFamilyKeyPair(
					key,
					baseKey,
					band,
					mode,
					corrCfg.CustomSCP.SFloorMinUniqueSpottersFamily,
					now,
				)
			}
		}
	} else if recentBandStore != nil && corrCfg.RecentBandBonusEnabled {
		band := s.BandNorm
		if band == "" || band == "???" {
			band = spot.FreqToBand(s.Frequency)
		}
		recentHit = hasRecentSupportForCallFamily(recentBandStore, call, band, mode, corrCfg.RecentBandRecordMinUniqueSpotters, time.Now().UTC())
	}

	if staticHit || recentHit {
		s.Confidence = "S"
		return true
	}
	return false
}

// hasRecentSupportForCallFamily checks recent support across family identities.
// Key aspects: Uses canonical vote/base keys to keep slash variants coherent.
func hasRecentSupportForCallFamily(store spot.RecentSupportStore, call, band, mode string, minUnique int, now time.Time) bool {
	return correctionflow.HasRecentSupportForCallFamily(store, call, band, mode, minUnique, now)
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
		if strutil.NormalizeUpper(s.SourceNode) == "DXSUMMIT" {
			return "DXSUMMIT"
		}
		return "UPSTREAM"
	}

	node := strutil.NormalizeUpper(s.SourceNode)
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
	case "DXSUMMIT":
		return "DXSUMMIT"
	}
	return "OTHER"
}

func rbnStatsLabel(sourceNode string) string {
	trimmed := strutil.NormalizeUpper(sourceNode)
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
// Purpose: Format the confidence string for corrected calls.
// Key aspects: Uses '?' only for single-reporter (unique/no-support) cases, and
// maps all multi-reporter cases into P/V buckets; SCP floor applied later.
// Upstream: maybeApplyCallCorrectionWithLogger.
// Downstream: None (pure mapping).
func formatConfidence(percent int, totalReporters int) string {
	return correctionflow.FormatConfidence(percent, totalReporters)
}

// Purpose: Decide whether a spot is eligible for frequency averaging.
// Key aspects: Skips digital modes and requires a valid call/frequency.
// Upstream: processOutputSpots frequency averaging path.
// Downstream: string checks on mode and call.
func shouldAverageFrequency(s *spot.Spot) bool {
	mode := strutil.NormalizeUpper(s.Mode)
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
	filtered := skew.FilterEntries(entries, cfg.MinAbsSkew)
	if len(filtered) == 0 {
		return 0, fmt.Errorf("skew: no entries after filtering (min_abs_skew=%g)", cfg.MinAbsSkew)
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
// Downstream: internal/schedule helpers.
func nextSkewRefreshDelay(cfg config.SkewConfig, now time.Time) time.Duration {
	return schedule.NextDailyUTC(cfg.RefreshUTC, now, 0, 30, schedule.ParseOptions{})
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
func startPropReportDailyScheduler(ctx context.Context, cfg config.PropReportConfig, logCfg config.PropagationLoggingConfig, scheduler *propReportScheduler) {
	if scheduler == nil || !cfg.Enabled {
		return
	}
	if !logCfg.Enabled {
		return
	}
	logDir := strings.TrimSpace(logCfg.Dir)
	if logDir == "" {
		log.Printf("Prop report daily scheduler disabled (logging.propagation.dir empty)")
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
// Downstream: internal/schedule helpers.
func nextCTYRefreshDelay(cfg config.CTYConfig, now time.Time) time.Duration {
	return schedule.NextDailyUTC(cfg.RefreshUTC, now, 0, 45, schedule.ParseOptions{})
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
		rawCall := strutil.NormalizeUpper(call)
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
			rawCall := strutil.NormalizeUpper(call)
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

func startGridStoreRecovery(ctx context.Context, storeHandle *gridStoreHandle, dbPath string, opts gridstore.Options, metaCache *callMetaCache) {
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
		if metaCache != nil {
			metaCache.Clear()
		}
	}()
}

func gridCheckpointRoot(dbPath string) string {
	return pebbleresilience.CheckpointRoot(dbPath, gridCheckpointDirName)
}

func listGridCheckpoints(root string) ([]pebbleresilience.CheckpointEntry, error) {
	checkpoints, err := pebbleresilience.ListCheckpoints(root, gridCheckpointNameLayoutUTC)
	if err != nil {
		return nil, err
	}
	return checkpoints, nil
}

func cleanupGridCheckpoints(root string, now time.Time) (int, error) {
	return pebbleresilience.CleanupCheckpoints(root, gridCheckpointNameLayoutUTC, gridCheckpointRetention, now)
}

func nextGridCheckpointDelay(now time.Time) time.Duration {
	target := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC).Add(gridCheckpointInterval)
	if !target.After(now) {
		target = target.Add(gridCheckpointInterval)
	}
	return target.Sub(now)
}

func nextDailyUTC(refreshUTC string, now time.Time, fallbackHour int, fallbackMinute int) time.Duration {
	return schedule.NextDailyUTC(refreshUTC, now, fallbackHour, fallbackMinute, schedule.ParseOptions{})
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
		stats, err := gridstore.VerifyCheckpoint(ctx, checkpoint.Path, gridCheckpointVerifyTimeout)
		if err != nil {
			log.Printf("Gridstore: checkpoint verify failed (%s): %v", checkpoint.Path, err)
			continue
		}
		if stats.CountMetaErr != nil && !stats.CountMetaValid {
			log.Printf("Gridstore: checkpoint %s count metadata warning: %v", checkpoint.Path, stats.CountMetaErr)
		}
		if err := restoreGridStoreFromPath(ctx, dbPath, checkpoint.Path); err != nil {
			log.Printf("Gridstore: checkpoint restore failed (%s): %v", checkpoint.Path, err)
			continue
		}
		store, err := gridstore.Open(dbPath, opts)
		if err != nil {
			log.Printf("Gridstore: open after restore failed (%s): %v", checkpoint.Path, err)
			continue
		}
		return store, checkpoint.Path, nil
	}
	return nil, "", errors.New("gridstore: no valid checkpoints")
}

func restoreGridStoreFromPath(ctx context.Context, dbPath string, checkpointPath string) error {
	return pebbleresilience.RestoreFromCheckpointDir(
		ctx,
		dbPath,
		checkpointPath,
		gridCheckpointNameLayoutUTC,
		gridCheckpointDirName,
	)
}

func openCustomSCPStoreWithRecovery(ctx context.Context, opts spot.CustomSCPOptions) (*spot.CustomSCPStore, error) {
	store, err := spot.OpenCustomSCPStore(opts)
	if err == nil {
		return store, nil
	}
	if !pebble.IsCorruptionError(err) {
		return nil, err
	}
	log.Printf("CustomSCP: corruption detected on open (%v); restoring from checkpoint", err)
	if restoreErr := restoreCustomSCPStoreFromCheckpoint(ctx, opts.Path); restoreErr != nil {
		return nil, fmt.Errorf("custom_scp: checkpoint restore failed: %w", restoreErr)
	}
	return spot.OpenCustomSCPStore(opts)
}

func startCustomSCPCheckpointScheduler(ctx context.Context, store *spot.CustomSCPStore, dbPath string) {
	if store == nil || strings.TrimSpace(dbPath) == "" {
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
			root := customSCPCheckpointRoot(dbPath)
			if err := os.MkdirAll(root, 0o755); err != nil {
				log.Printf("CustomSCP: checkpoint mkdir failed: %v", err)
				continue
			}
			ts := time.Now().UTC().Format(gridCheckpointNameLayoutUTC)
			tmp := filepath.Join(root, ".tmp-"+ts)
			dest := filepath.Join(root, ts)
			if err := store.Checkpoint(tmp); err != nil {
				log.Printf("CustomSCP: checkpoint failed: %v", err)
				_ = os.RemoveAll(tmp)
				continue
			}
			if err := os.Rename(tmp, dest); err != nil {
				log.Printf("CustomSCP: checkpoint rename failed: %v", err)
				_ = os.RemoveAll(tmp)
				continue
			}
			if removed, err := cleanupCustomSCPCheckpoints(root, time.Now().UTC()); err != nil {
				log.Printf("CustomSCP: checkpoint cleanup failed: %v", err)
			} else if removed > 0 {
				log.Printf("CustomSCP: checkpoint cleanup removed %d old checkpoint(s)", removed)
			}
		}
	}()
}

func startCustomSCPIntegrityScheduler(ctx context.Context, store *spot.CustomSCPStore) {
	if store == nil {
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
			scanCtx, cancel := context.WithTimeout(ctx, gridIntegrityScanTimeout)
			records, err := store.Verify(scanCtx, gridIntegrityScanTimeout)
			cancel()
			if err != nil {
				log.Printf("CustomSCP: integrity scan failed: %v", err)
				continue
			}
			log.Printf("CustomSCP: integrity scan ok (records=%d)", records)
		}
	}()
}

func customSCPCheckpointRoot(dbPath string) string {
	return pebbleresilience.CheckpointRoot(dbPath, gridCheckpointDirName)
}

func listCustomSCPCheckpoints(root string) ([]pebbleresilience.CheckpointEntry, error) {
	return pebbleresilience.ListCheckpoints(root, gridCheckpointNameLayoutUTC)
}

func cleanupCustomSCPCheckpoints(root string, now time.Time) (int, error) {
	return pebbleresilience.CleanupCheckpoints(root, gridCheckpointNameLayoutUTC, gridCheckpointRetention, now)
}

func restoreCustomSCPStoreFromCheckpoint(ctx context.Context, dbPath string) error {
	root := customSCPCheckpointRoot(dbPath)
	checkpoints, err := listCustomSCPCheckpoints(root)
	if err != nil {
		return fmt.Errorf("custom_scp: list checkpoints: %w", err)
	}
	if len(checkpoints) == 0 {
		return errors.New("custom_scp: no checkpoints available")
	}
	for _, checkpoint := range checkpoints {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := spot.VerifyCustomSCPCheckpoint(ctx, checkpoint.Path, gridCheckpointVerifyTimeout); err != nil {
			log.Printf("CustomSCP: checkpoint verify failed (%s): %v", checkpoint.Path, err)
			continue
		}
		if err := pebbleresilience.RestoreFromCheckpointDir(ctx, dbPath, checkpoint.Path, gridCheckpointNameLayoutUTC, gridCheckpointDirName); err != nil {
			log.Printf("CustomSCP: checkpoint restore failed (%s): %v", checkpoint.Path, err)
			continue
		}
		return nil
	}
	return errors.New("custom_scp: no valid checkpoints")
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
// exec alloc / ring buffer / primary dedup (dup%) / secondary dedup (fast+med+slow) / call meta cache (hit%).
// Purpose: Format the memory/status line for the stats pane.
// Key aspects: Reports ring buffer occupancy and cache hit stats.
// Upstream: displayStatsWithFCC.
// Downstream: buffer.RingBuffer stats and cache lookups.
func formatMemoryLine(buf *buffer.RingBuffer, dedup *dedup.Deduplicator, secondaryFast *dedup.SecondaryDeduper, secondaryMed *dedup.SecondaryDeduper, secondarySlow *dedup.SecondaryDeduper, metaCache *callMetaCache) string {
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

	return fmt.Sprintf("Memory MB: %.1f / %.1f / %.1f (%.1f%%) / %.1f / %.1f (%.1f%%)",
		execMB, ringMB, dedupeMB, dedupeRatio, secondaryMB, metaMB, metaRatio)
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

func formatModeCacheLine(assigner *spot.ModeAssigner) string {
	if assigner == nil {
		return "[yellow]Mode cache[-]: [yellow]DX hit[-] n/a | [yellow]Digital[-] n/a | [yellow]Mix[-] E0 I0 RC0 RV0 RM0 RU0"
	}
	stats := assigner.Stats()
	dxHitPct := percentValue(stats.DXHits, stats.DXLookups)
	return fmt.Sprintf("[yellow]Mode cache[-]: [yellow]DX hit[-] %s | [yellow]Digital[-] %s/%s | [yellow]Mix[-] E%s I%s RC%s RV%s RM%s RU%s",
		formatPercentString(dxHitPct),
		humanize.Comma(int64(stats.DigitalBuckets)),
		humanize.Comma(int64(stats.DigitalMax)),
		humanize.Comma(int64(stats.Explicit)),
		humanize.Comma(int64(stats.Inferred)),
		humanize.Comma(int64(stats.RegionalCW)),
		humanize.Comma(int64(stats.RegionalVoice)),
		humanize.Comma(int64(stats.RegionalMixed)),
		humanize.Comma(int64(stats.RegionalUnknown)),
	)
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
	recentBandStore spot.RecentSupportStore,
	ctyState *ctyRefreshState,
	fccSnap *fccSnapshot,
	gridStats *gridMetrics,
	gridDB *gridStoreHandle,
	pathPredictor *pathreliability.Predictor,
	voacapSSN pathreliability.VOACAPSSNProvider,
	voacapFallback voacapFallbackSnapshotProvider,
	modeAssigner *spot.ModeAssigner,
	telnetSrv *telnet.Server,
	clusterCall string,
	rbnLive, pskLive, p92Live bool,
	ingestSources []dashboardIngestSource,
	rbnTotal, rbnCW, rbnRTTY, rbnFT8, rbnFT4, rbnFT2 uint64,
	pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskFT2, pskMSK144, psk31_63 uint64,
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
	gridSizeLabel := humanize.Comma(gridCount)
	metaSizeLabel := humanize.Comma(int64(metaCount))
	recentSupportLines := formatRecentSupportByBandLines(recentBandStore, now, 0)
	cacheBars := make([]string, 0, len(recentSupportLines)+4)
	cacheBars = append(cacheBars,
		fmt.Sprintf("[yellow]Grid cache[-]:  %s %s  |  [yellow]Meta[-]: %s %s",
			formatPercentBarWithLabel(gridHitPct, 20, gridSizeLabel), formatPercentString(gridHitPct),
			formatPercentBarWithLabel(metaHitPct, 20, metaSizeLabel), formatPercentString(metaHitPct)),
		formatModeCacheLine(modeAssigner),
	)
	cacheBars = append(cacheBars, "")
	cacheBars = append(cacheBars, recentSupportLines...)
	cacheBars = append(cacheBars, "")

	ctyTime := "n/a"
	if ctyState != nil {
		if ts, ok := ctyState.lastSuccessTime(); ok {
			ctyTime = formatDateShortZ(ts)
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
	voacapSSNLabel := formatVOACAPSSNLabel(voacapSSN, now)

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
	pskLine := formatIngestLine(withIngestStatusLabel("PSK", pskLive), pskTotal, pskCW, pskRTTY, pskFT8, pskFT4, pskFT2, pskMSK144, true)
	pskLine += " | " + formatIngestField("[yellow]PSK[-]", psk31_63, ingestWidthPSK)
	lines = append(lines,
		fmt.Sprintf("[yellow]Cluster[-]: %s  [yellow]Version[-]: %s  [yellow]Uptime[-]: %s", clusterCall, currentBuildInfo.Version, formatUptimeShort(uptime)),
		"MEMORY / GC",
		fmt.Sprintf("[yellow]Heap[-]: %s  [yellow]Sys[-]: %s  [yellow]GC p99 (interval)[-]: %s  [yellow]Last GC[-]: %s ago  [yellow]Goroutines[-]: %d", heap, sys, gcP99Label, formatDurationShort(lastGC), runtime.NumGoroutine()),
		"INGEST RATES (per min)",
		formatIngestLine(withIngestStatusLabel("RBN", rbnLive), rbnTotal, rbnCW, rbnRTTY, rbnFT8, rbnFT4, rbnFT2, 0, false),
		pskLine,
		fmt.Sprintf("%s: %s", withIngestStatusLabel("P92", p92Live), humanize.Comma(int64(p92Total))),
		pathOnlyLine,
		"PIPELINE QUALITY",
		fmt.Sprintf("[yellow]Primary Dedupe[-]: %s | [yellow]Secondary[-]: %s", primaryDupPct, secondarySummary),
		fmt.Sprintf("[yellow]Corrections[-]: %s | [yellow]Unlicensed[-]: %s | [yellow]Harmonics[-]: %s | [yellow]Reputation[-]: %s",
			humanize.Comma(int64(totalCorrections)),
			humanize.Comma(int64(totalUnlicensed)),
			humanize.Comma(int64(totalHarmonics)),
			humanize.Comma(int64(reputationTotal)),
		),
		fmt.Sprintf("[yellow]Flood[-]: %s", strings.TrimPrefix(formatFloodSummary(tracker), "Flood: ")),
		"",
		fmt.Sprintf("[yellow]Stabilizer Glyph[-]: %s", strings.TrimPrefix(formatStabilizerGlyphSummary(tracker), "Stabilizer Glyph: ")),
		"CACHES & DATA FRESHNESS",
	)
	lines = append(lines, cacheBars...)
	lines = append(lines,
		"",
		fmt.Sprintf("[yellow]CTY[-]: %s  [yellow]FCC[-]: %s  [yellow]Skew[-]: %s  [yellow]VOACAP SSN[-]: %s", ctyTime, fccTime, skewTime, voacapSSNLabel),
		"PATH PREDICTIONS",
	)
	lines = append(lines, formatPathLines(pathPredictor, voacapFallback, now)...)
	lines = append(lines, "INGEST SOURCES")
	lines = append(lines, formatIngestSourceLines(ingestSources)...)
	lines = append(lines, "NETWORK")
	lines = append(lines, formatNetworkLines(telnetSrv, clientList)...)
	return lines
}

func formatVOACAPSSNLabel(provider pathreliability.VOACAPSSNProvider, now time.Time) string {
	if provider == nil {
		return "n/a"
	}
	ssn, ok := provider.CurrentSSN(now)
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%d", ssn)
}

type dashboardIngestSourceConfig struct {
	RBNEnabled                  bool
	RBNDigitalEnabled           bool
	PSKReporterEnabled          bool
	DXSummitEnabled             bool
	PeeringEnabled              bool
	DXSummitPollIntervalSeconds int
}

type dashboardIngestSource struct {
	Label     string
	Enabled   bool
	Connected bool
	Details   []string
}

func dashboardIngestSourceConfigFromConfig(cfg *config.Config) dashboardIngestSourceConfig {
	if cfg == nil {
		return dashboardIngestSourceConfig{}
	}
	return dashboardIngestSourceConfig{
		RBNEnabled:                  cfg.RBN.Enabled,
		RBNDigitalEnabled:           cfg.RBNDigital.Enabled,
		PSKReporterEnabled:          cfg.PSKReporter.Enabled,
		DXSummitEnabled:             cfg.DXSummit.Enabled,
		PeeringEnabled:              cfg.Peering.Enabled,
		DXSummitPollIntervalSeconds: cfg.DXSummit.PollIntervalSeconds,
	}
}

func dashboardIngestSources(
	cfg dashboardIngestSourceConfig,
	rbnCWLive, rbnFTLive, pskLive, dxsummitLive, p92Live bool,
	peerSessions int,
	peerSSIDs []string,
) []dashboardIngestSource {
	peerDetails := []string(nil)
	if p92Live {
		if len(peerSSIDs) > 0 {
			peerDetails = append(peerDetails, peerSSIDs...)
		} else if peerSessions > 0 {
			peerDetails = append(peerDetails, fmt.Sprintf("Peers (%d)", peerSessions))
		}
	}
	return []dashboardIngestSource{
		{Label: "RBN", Enabled: cfg.RBNEnabled, Connected: rbnCWLive},
		{Label: "RBN-FT", Enabled: cfg.RBNDigitalEnabled, Connected: rbnFTLive},
		{Label: "PSKReporter", Enabled: cfg.PSKReporterEnabled, Connected: pskLive},
		{Label: dxsummit.SourceNode, Enabled: cfg.DXSummitEnabled, Connected: dxsummitLive},
		{Label: "Peers", Enabled: cfg.PeeringEnabled, Connected: p92Live, Details: peerDetails},
	}
}

func formatIngestSourceLines(sources []dashboardIngestSource) []string {
	connected := 0
	enabled := 0
	entries := make([]string, 0, len(sources))
	for _, source := range sources {
		if !source.Enabled {
			continue
		}
		enabled++
		if source.Connected {
			connected++
			if len(source.Details) > 0 {
				for _, detail := range source.Details {
					entries = append(entries, withIngestStatusLabel(detail, true))
				}
				continue
			}
		}
		entries = append(entries, withIngestStatusLabel(source.Label, source.Connected))
	}
	lines := []string{fmt.Sprintf("[yellow]Ingest[-]: %d / %d connected", connected, enabled)}
	if enabled == 0 {
		lines = append(lines, "", "(none enabled)")
		return lines
	}
	if len(entries) == 0 {
		lines = append(lines, "")
		return lines
	}
	lines = append(lines, formatClientListLines(entries)...)
	return lines
}

func formatNetworkSummaryLine(telnetSrv *telnet.Server) string {
	if telnetSrv == nil {
		return "[yellow]Telnet[-]: 0 clients   [yellow]Drops[-]: Q0 C0 W0   [yellow]Prelogin[-]: A0 G0 R0 C0 T0"
	}
	queueDrops, clientDrops, senderFailures := telnetSrv.BroadcastMetricSnapshot()
	preloginActive, preloginRejectGlobal, preloginRejectRate, preloginRejectConcurrency, preloginTimeouts, _, _ := telnetSrv.PreloginMetricSnapshot()
	clientCount := telnetSrv.GetClientCount()
	return fmt.Sprintf("[yellow]Telnet[-]: %d clients   [yellow]Drops[-]: Q%d C%d W%d   [yellow]Prelogin[-]: A%d G%d R%d C%d T%d",
		clientCount, queueDrops, clientDrops, senderFailures, preloginActive, preloginRejectGlobal, preloginRejectRate, preloginRejectConcurrency, preloginTimeouts,
	)
}

func formatNetworkLines(telnetSrv *telnet.Server, clientList []string) []string {
	clientLines := formatClientListLines(clientList)
	lines := make([]string, 0, 1+len(clientLines))
	lines = append(lines, formatNetworkSummaryLine(telnetSrv))
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

const (
	ingestWidthTotal  = 6
	ingestWidthNarrow = 4
	ingestWidthMid    = 5
	ingestWidthFT     = 6
	ingestWidthPSK    = ingestWidthNarrow
)

func formatIngestLine(label string, total, cw, rtty, ft8, ft4, ft2, msk uint64, includeMSK bool) string {
	totalStr := padRight(humanize.Comma(int64(total)), ingestWidthTotal)
	fields := []string{
		fmt.Sprintf("%s: %s", label, totalStr),
		formatIngestField("[yellow]CW[-]", cw, ingestWidthMid),
		formatIngestField("[yellow]RTTY[-]", rtty, ingestWidthMid),
		formatIngestField("[yellow]FT8[-]", ft8, ingestWidthFT),
		formatIngestField("[yellow]FT4[-]", ft4, ingestWidthFT),
		formatIngestField("[yellow]FT2[-]", ft2, ingestWidthFT),
	}
	if includeMSK {
		fields = append(fields, formatIngestField("[yellow]MSK[-]", msk, ingestWidthNarrow))
	}
	return strings.Join(fields, " | ")
}

func formatIngestField(label string, value uint64, width int) string {
	val := padRight(humanize.Comma(int64(value)), width)
	return fmt.Sprintf("%s %s", label, val)
}

func withIngestStatusLabel(label string, live bool) string {
	if live {
		return fmt.Sprintf("[green]%s[-]", label)
	}
	return fmt.Sprintf("[red]%s[-]", label)
}

type voacapFallbackSnapshotProvider interface {
	Snapshot() pathreliability.VOACAPClosedFallbackSnapshot
}

func formatPathLines(predictor *pathreliability.Predictor, voacapFallback voacapFallbackSnapshotProvider, now time.Time) []string {
	const (
		colsPerRow = 4
	)
	lines := make([]string, 0, 2)
	lines = append(lines, formatVOACAPCacheLine(voacapFallback))
	if predictor == nil || !predictor.Config().Enabled {
		lines = append(lines, "[yellow]H3 path pairs[-]: n/a")
		return lines
	}
	stats := predictor.Stats(now)
	lines = append(lines, fmt.Sprintf("[yellow]H3 path pairs[-]: %s (L2) / %s (L1)",
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

func formatVOACAPCacheLine(provider voacapFallbackSnapshotProvider) string {
	if provider == nil {
		return "[yellow]VOACAP cache[-]: n/a"
	}
	snap := provider.Snapshot()
	return fmt.Sprintf("[yellow]VOACAP cache[-]: %s (C) / %s (D) / %s (I) / %s (Q)",
		humanize.Comma(int64(snap.CacheEntries)),
		humanize.Comma(int64(snap.DelayEntries)),
		humanize.Comma(int64(snap.InflightEntries)),
		humanize.Comma(int64(snap.QueueDepth)),
	)
}

func formatRecentSupportByBandLines(store spot.RecentSupportStore, now time.Time, maxBands int) []string {
	lines := []string{"[yellow]Recent support[-]: n/a"}
	if store == nil {
		return lines
	}
	total := store.ActiveCallCount(now)
	if customSCP, ok := store.(*spot.CustomSCPStore); ok && customSCP != nil {
		staticTotal := customSCP.StaticCallCount(now)
		lines[len(lines)-1] = fmt.Sprintf("[yellow]Custom SCP[-]: %s (R) / %s (S)",
			humanize.Comma(int64(total)),
			humanize.Comma(int64(staticTotal)),
		)
	} else {
		lines[len(lines)-1] = fmt.Sprintf("[yellow]Recent support[-]: %s", humanize.Comma(int64(total)))
	}

	counts := store.ActiveCallCountsByBand(now)
	if len(counts) == 0 {
		return lines
	}
	bandOrder := spot.SupportedBandNames()
	type bandCount struct {
		band  string
		count int
	}
	entries := make([]bandCount, 0, len(counts))
	seen := make(map[string]struct{}, len(bandOrder))
	for _, band := range bandOrder {
		count := counts[band]
		if count <= 0 {
			continue
		}
		entries = append(entries, bandCount{band: band, count: count})
		seen[band] = struct{}{}
	}
	extras := make([]string, 0, len(counts))
	for band, count := range counts {
		if count <= 0 {
			continue
		}
		if _, ok := seen[band]; ok {
			continue
		}
		extras = append(extras, band)
	}
	sort.Strings(extras)
	for _, band := range extras {
		entries = append(entries, bandCount{band: band, count: counts[band]})
	}
	if len(entries) == 0 {
		return lines
	}
	if maxBands > 0 && len(entries) > maxBands {
		entries = entries[:maxBands]
	}
	lines = append(lines, "")

	const colsPerRow = 5
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
	key = strutil.NormalizeUpper(key)
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

func diffSourceModes(current, previous map[string]uint64, source string, modes ...string) uint64 {
	var total uint64
	for _, mode := range modes {
		total += diffSourceMode(current, previous, source, mode)
	}
	return total
}

// Purpose: Report whether the RBN source family has any live feed.
// Key aspects: CW/RTTY and FT feeds remain individually visible in source rows.
// Upstream: stats ticker liveness.
// Downstream: withIngestStatusLabel.
func rbnFeedFamilyLive(rbnCWLive, rbnFTLive bool) bool {
	return rbnCWLive || rbnFTLive
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

// Purpose: Report whether DXSummit has completed a recent successful poll.
// Key aspects: Seed-only startup can be live without emitted spots, so liveness
// is based on LastPollAt rather than LastSpotAt.
// Upstream: stats ticker liveness.
// Downstream: dxsummit.HealthSnapshot timestamps.
func dxsummitIsLive(snap dxsummit.HealthSnapshot, pollIntervalSeconds int, now time.Time) bool {
	if !snap.Connected || snap.LastPollAt.IsZero() || pollIntervalSeconds <= 0 {
		return false
	}
	maxAge := 2*time.Duration(pollIntervalSeconds)*time.Second + time.Second
	return now.Sub(snap.LastPollAt) <= maxAge
}

// Purpose: Compute per-interval RBN ingest deltas (CW/RTTY + FT8/FT4/FT2).
// Key aspects: Reads FT counts from the "RBN-FT" source label.
// Upstream: displayStatsWithFCC stats ticker.
// Downstream: diffCounter and diffSourceMode.
func rbnIngestDeltas(
	sourceTotals, prevSourceCounts map[string]uint64,
	sourceModeTotals, prevSourceModeCounts map[string]uint64,
) (rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4, rbnFT2 uint64) {
	rbnTotal = diffCounter(sourceTotals, prevSourceCounts, "RBN")
	rbnCW = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "CW")
	rbnRTTY = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN", "RTTY")

	rbnFTTotal = diffCounter(sourceTotals, prevSourceCounts, "RBN-FT")
	rbnFT8 = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-FT", "FT8")
	rbnFT4 = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-FT", "FT4")
	rbnFT2 = diffSourceMode(sourceModeTotals, prevSourceModeCounts, "RBN-FT", "FT2")
	return
}

// Purpose: Build a stable key for source+mode counters.
// Key aspects: Uppercases and concatenates with a delimiter.
// Upstream: diffSourceMode.
// Downstream: strings.ToUpper.
func sourceModeKey(source, mode string) string {
	source = strutil.NormalizeUpper(source)
	mode = strutil.NormalizeUpper(mode)
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
func maybeStartMapLogger(tracker *stats.Tracker, predictor *pathreliability.Predictor, dedup *dedup.Deduplicator, secondaryFast, secondaryMed, secondarySlow *dedup.SecondaryDeduper, customSCPStore *spot.CustomSCPStore) {
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

			customSCP := spot.CustomSCPStatsSnapshot{}
			if customSCPStore != nil {
				customSCP = customSCPStore.StatsSnapshot()
			}

			log.Printf("Map sizes: stats sources=%d source-modes=%d; dedup primary=%d secondary fast=%d med=%d slow=%d; path buckets=%d; custom_scp static=%d keys=%d spotters=%d interned=%d intern_refs=%d intern_release_misses=%d entry_expiry=%d static_expiry=%d load_oversized=%d overflow_pruned=%d stale_obs_pruned=%d stale_static_pruned=%d",
				sourceCount,
				sourceModeCount,
				primarySize,
				fastSize,
				medSize,
				slowSize,
				pathBuckets,
				customSCP.StaticCalls,
				customSCP.ObservationKeys,
				customSCP.ObservationSpotters,
				customSCP.InternedStrings,
				customSCP.InternedRefs,
				customSCP.InternReleaseMisses,
				customSCP.EntryExpiryItems,
				customSCP.StaticExpiryItems,
				customSCP.OversizedKeysSeenOnLoad,
				customSCP.OverflowObservationsPruned,
				customSCP.StaleObservationsPruned,
				customSCP.StaleStaticPruned)
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
