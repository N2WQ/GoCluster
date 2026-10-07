// File role: Owns one deterministic archived RBN replay run.
// Crawler notes: Start here for run phases, archive/download paths, replay
// output files, manifest/runbook lifecycle, and cleanup ownership when replay
// fails after partial initialization.
// Related docs: docs/rbn_replay.md, docs/dev-runbook.md.
// Related tests: cmd/rbn_replay/runner_test.go, cmd/rbn_replay/rbn_history_test.go.
package main

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/download"
	"dxcluster/internal/correctionflow"
	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/uls"
)

type replayRequest struct {
	dateValue        string
	replayConfigPath string
	configDir        string
	archiveDir       string
	forceDownload    bool
}

// replayRunner owns one deterministic day replay. The fields mirror the
// execution phases so support/debug work can tell whether a failure came from
// input discovery, dependency wiring, replay runtime, or output finalization.
type replayRunner struct {
	request   replayRequest
	startedAt time.Time

	replayCfg  replayConfig
	cfg        *config.Config
	dayStart   time.Time
	dayEnd     time.Time
	dayCompact string

	configDir  string
	archiveDir string
	outDir     string
	zipPath    string
	zipResult  download.Result

	ctyDB              *cty.CTYDatabase
	adaptiveMinReports *spot.AdaptiveMinReports
	recentBandStore    spot.RecentSupportStore
	cleanupRecentBand  func()
	temporalDecoder    *correctionflow.TemporalDecoder
	tracker            *stats.Tracker
	resolver           *spot.SignalResolver
	driver             *spot.SignalResolverDriver

	manifest           replayManifest
	runbookSamplesFile *os.File
	runbookSamplesPath string
	csvParser          *rbnHistoryCSV
	stabilityCollector *replayStabilityCollector
	samples            []resolverSample
	nextSampleAt       time.Time
	lastRecordTime     time.Time
	recordsTotal       int64
	recordsProcessed   int64
	recordsSkippedMode int64
	recordsSkippedBad  int64
	abMetrics          replayABMetrics
	temporalSeq        uint64
	temporalNextID     uint64
	temporalQueue      replayTemporalHeap
	temporalPending    map[uint64]replayTemporalPending
}

// runReplay is the public orchestration entry point used by main and tests.
// Cleanup is deferred here so partially initialized resources are still closed
// when an early phase returns an error.
func runReplay(req replayRequest) error {
	runner := newReplayRunner(req)
	defer runner.close()
	return runner.run()
}

func newReplayRunner(req replayRequest) *replayRunner {
	return &replayRunner{
		request:         req,
		startedAt:       time.Now().UTC(),
		temporalNextID:  1,
		temporalPending: make(map[uint64]replayTemporalPending),
	}
}

func (r *replayRunner) run() error {
	if err := r.loadReplayConfig(); err != nil {
		return err
	}
	if err := r.resolveDay(); err != nil {
		return err
	}
	if err := r.loadClusterConfig(); err != nil {
		return err
	}
	if err := r.prepareArchiveDirs(); err != nil {
		return err
	}
	if err := r.downloadInputZip(); err != nil {
		return err
	}
	if err := r.configureExternalDependencies(); err != nil {
		return err
	}
	if err := r.buildReplayRuntime(); err != nil {
		return err
	}
	if err := r.openReplayOutputs(); err != nil {
		return err
	}
	if err := r.replayDay(); err != nil {
		return err
	}
	return r.finalize()
}

// close releases replay-only files and stores. It is intentionally best-effort
// because the primary error should remain the replay phase that failed.
func (r *replayRunner) close() {
	if r.csvParser != nil {
		_ = r.csvParser.Close()
	}
	if r.runbookSamplesFile != nil {
		_ = r.runbookSamplesFile.Close()
	}
	if r.cleanupRecentBand != nil {
		r.cleanupRecentBand()
		r.cleanupRecentBand = nil
	}
}

func (r *replayRunner) loadReplayConfig() error {
	loaded, err := loadReplayConfig(r.request.replayConfigPath)
	if err != nil {
		return err
	}
	r.replayCfg = loaded
	return nil
}

func (r *replayRunner) resolveDay() error {
	if strings.TrimSpace(r.request.dateValue) == "" {
		return errors.New("missing required -date (YYYY-MM-DD or YYYYMMDD)")
	}
	dayStart, dayCompact, err := parseUTCDate(r.request.dateValue)
	if err != nil {
		return err
	}
	r.dayStart = dayStart
	r.dayCompact = dayCompact
	r.dayEnd = dayStart.Add(24 * time.Hour)
	return nil
}

func (r *replayRunner) loadClusterConfig() error {
	r.configDir = resolveReplayConfigDir(r.request.configDir, r.replayCfg, os.Getenv(envConfigPath))

	cfg, err := config.Load(r.configDir)
	if err != nil {
		return err
	}
	if cfg == nil {
		return errors.New("config loaded as nil")
	}
	if !cfg.CallCorrection.Enabled {
		return fmt.Errorf("call_correction.enabled must be true for replay (config=%s)", cfg.LoadedFrom)
	}
	r.cfg = cfg
	return nil
}

// prepareArchiveDirs keeps downloaded inputs and generated outputs under the
// replay archive root so operators can collect a complete evidence bundle.
func (r *replayRunner) prepareArchiveDirs() error {
	r.archiveDir = resolveReplayArchiveDir(r.request.archiveDir, r.replayCfg)
	if err := ensureDir(r.archiveDir); err != nil {
		return err
	}

	r.outDir = buildReplayOutputDir(r.archiveDir, r.dayStart)
	return ensureDir(r.outDir)
}

// downloadInputZip records the download result in the manifest; "not modified"
// is still useful evidence when comparing replay runs.
func (r *replayRunner) downloadInputZip() error {
	forceRBNDownload := r.replayCfg.ForceDownload || r.request.forceDownload
	r.zipPath = buildReplayZipPath(r.archiveDir, r.dayCompact)

	zipResult, err := download.Download(context.Background(), download.Request{
		URL:         buildReplayZipURL(r.dayCompact),
		Destination: r.zipPath,
		Timeout:     10 * time.Minute,
		Force:       forceRBNDownload,
		UserAgent:   "dxcluster-rbn-replay",
	})
	if err != nil {
		return err
	}
	r.zipResult = zipResult
	return nil
}

// configureExternalDependencies loads the same CTY/ULS surfaces the runtime
// uses so replay failures match production support conditions.
func (r *replayRunner) configureExternalDependencies() error {
	cfg := r.cfg

	uls.SetLicenseChecksEnabled(cfg.FCCULS.Enabled)
	uls.SetCanadianLicenseChecksEnabled(cfg.ISED.Enabled)
	uls.SetLicenseCacheTTL(time.Duration(cfg.FCCULS.CacheTTLSeconds) * time.Second)

	allowlistPath := strings.TrimSpace(cfg.FCCULS.AllowlistPath)
	if allowlistPath != "" {
		if _, err := os.Stat(allowlistPath); err != nil {
			return fmt.Errorf("fcc_uls.allowlist_path missing/unreadable %s: %w", allowlistPath, err)
		}
		uls.SetAllowlistPath(allowlistPath)
	} else {
		uls.SetAllowlistPath("")
	}

	// Replay registers local snapshots without downloading or enriching spots.
	// A missing optional DB clears prior globals even when enforcement is disabled.
	dbPath := strings.TrimSpace(cfg.FCCULS.DBPath)
	if cfg.FCCULS.Enabled {
		if dbPath == "" {
			return fmt.Errorf("fcc_uls.enabled=true but fcc_uls.db_path is empty (config=%s)", cfg.LoadedFrom)
		}
		if _, err := os.Stat(dbPath); err != nil {
			return fmt.Errorf("fcc_uls.db_path missing/unreadable %s: %w", dbPath, err)
		}
	}
	uls.SetLicenseDBPath(dbPath)
	canadianPath := strings.TrimSpace(cfg.ISED.DBPath)
	if cfg.ISED.Enabled {
		if canadianPath == "" {
			return fmt.Errorf("ised.enabled=true but ised.db_path is empty (config=%s)", cfg.LoadedFrom)
		}
		if _, err := os.Stat(canadianPath); err != nil {
			return fmt.Errorf("ised.db_path missing/unreadable %s: %w", canadianPath, err)
		}
	}
	uls.SetCanadianLicenseDBPath(canadianPath)

	if !cfg.CTY.Enabled {
		return nil
	}

	ctyPath := strings.TrimSpace(cfg.CTY.File)
	if ctyPath == "" {
		return fmt.Errorf("cty.enabled=true but cty.file is empty (config=%s)", cfg.LoadedFrom)
	}
	if _, err := os.Stat(ctyPath); err != nil {
		return fmt.Errorf("cty.file missing/unreadable %s: %w", ctyPath, err)
	}
	loaded, err := cty.LoadCTYDatabase(ctyPath)
	if err != nil {
		return err
	}
	r.ctyDB = loaded
	return nil
}

// buildReplayRuntime constructs the correction pipeline in shadow form. It uses
// bounded resolver settings so replay can expose cap pressure instead of hiding
// it behind unbounded analysis-only state.
func (r *replayRunner) buildReplayRuntime() error {
	cfg := r.cfg

	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	spot.ConfigureBaudotWeights(cfg.CallCorrection.BaudotWeights.Insert, cfg.CallCorrection.BaudotWeights.Delete, cfg.CallCorrection.BaudotWeights.Sub, cfg.CallCorrection.BaudotWeights.Scale)

	spotterReliability, spotterReliabilityCW, spotterReliabilityRTTY, err := loadSpotterReliability(cfg.CallCorrection)
	if err != nil {
		return err
	}
	confusionModel, err := loadConfusionModel(cfg.CallCorrection)
	if err != nil {
		return err
	}

	r.adaptiveMinReports = spot.NewAdaptiveMinReports(cfg.CallCorrection)
	if err := r.openRecentBandStore(); err != nil {
		return err
	}

	if cfg.CallCorrection.Enabled && cfg.CallCorrection.TemporalDecoder.Enabled {
		r.temporalDecoder = correctionflow.NewTemporalDecoder(cfg.CallCorrection)
	}

	r.tracker = stats.NewTracker()
	r.resolver = spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:                 shadowResolverQueueSize,
		MaxActiveKeys:             shadowResolverMaxActiveKeys,
		MaxCandidatesPerKey:       shadowResolverMaxCandidatesPerKey,
		MaxReportersPerCand:       shadowResolverMaxReportersPerCand,
		InactiveTTL:               shadowResolverInactiveTTL,
		EvalMinInterval:           shadowResolverEvalMinInterval,
		SweepInterval:             shadowResolverSweepInterval,
		HysteresisWindows:         shadowResolverHysteresisWindows,
		FreqGuardMinSeparationKHz: cfg.CallCorrection.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:    cfg.CallCorrection.FreqGuardRunnerUpRatio,
		MaxEditDistance:           cfg.CallCorrection.MaxEditDistance,
		DistanceModelCW:           cfg.CallCorrection.DistanceModelCW,
		DistanceModelRTTY:         cfg.CallCorrection.DistanceModelRTTY,
		SpotterReliability:        spotterReliability,
		SpotterReliabilityCW:      spotterReliabilityCW,
		SpotterReliabilityRTTY:    spotterReliabilityRTTY,
		MinSpotterReliability:     cfg.CallCorrection.MinSpotterReliability,
		ConfusionModel:            confusionModel,
		ConfusionWeight:           cfg.CallCorrection.ConfusionModelWeight,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.CallCorrection.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
	})
	driver, err := spot.NewSignalResolverDriver(r.resolver)
	if err != nil {
		return err
	}
	r.driver = driver
	return nil
}

// openRecentBandStore selects the same support-memory backend as runtime, but
// writes custom_scp data under the replay output directory to keep experiments
// isolated from production state.
func (r *replayRunner) openRecentBandStore() error {
	cfg := r.cfg
	if !cfg.CallCorrection.Enabled {
		return nil
	}
	if cfg.CallCorrection.CustomSCP.Enabled {
		replayCustomSCPPath := filepath.Join(r.outDir, "custom_scp_runtime")
		if err := os.RemoveAll(replayCustomSCPPath); err != nil {
			return fmt.Errorf("remove replay custom SCP path %s: %w", replayCustomSCPPath, err)
		}
		coreMinScore := cfg.CallCorrection.CustomSCP.ResolverMinScore
		if cfg.CallCorrection.CustomSCP.StabilizerMinScore > coreMinScore {
			coreMinScore = cfg.CallCorrection.CustomSCP.StabilizerMinScore
		}
		coreMinH3Cells := cfg.CallCorrection.CustomSCP.ResolverMinUniqueH3Cells
		if cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells > coreMinH3Cells {
			coreMinH3Cells = cfg.CallCorrection.CustomSCP.StabilizerMinUniqueH3Cells
		}
		customOpts := spot.CustomSCPOptions{
			Path:                   replayCustomSCPPath,
			HorizonDays:            cfg.CallCorrection.CustomSCP.HistoryHorizonDays,
			MaxKeys:                cfg.CallCorrection.CustomSCP.MaxKeys,
			MaxSpottersPerKey:      cfg.CallCorrection.CustomSCP.MaxSpottersPerKey,
			CleanupInterval:        time.Duration(cfg.CallCorrection.CustomSCP.CleanupIntervalSeconds) * time.Second,
			CacheSizeBytes:         int64(cfg.CallCorrection.CustomSCP.BlockCacheMB) << 20,
			BloomFilterBitsPerKey:  cfg.CallCorrection.CustomSCP.BloomFilterBits,
			MemTableSizeBytes:      uint64(cfg.CallCorrection.CustomSCP.MemTableSizeMB) << 20,
			L0CompactionThreshold:  cfg.CallCorrection.CustomSCP.L0CompactionThreshold,
			L0StopWritesThreshold:  cfg.CallCorrection.CustomSCP.L0StopWritesThreshold,
			CoreMinScore:           coreMinScore,
			CoreMinH3Cells:         coreMinH3Cells,
			SFloorMinScore:         cfg.CallCorrection.CustomSCP.SFloorMinScore,
			SFloorExactMinH3Cells:  cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsExact,
			SFloorFamilyMinH3Cells: cfg.CallCorrection.CustomSCP.SFloorMinUniqueH3CellsFamily,
			MinSNRDBCW:             cfg.CallCorrection.CustomSCP.MinSNRDBCW,
			MinSNRDBRTTY:           cfg.CallCorrection.CustomSCP.MinSNRDBRTTY,
		}
		customSCPStore, err := spot.OpenCustomSCPStore(customOpts)
		if err != nil {
			return err
		}
		customSCPStore.StartCleanup()
		r.recentBandStore = customSCPStore
		r.cleanupRecentBand = func() {
			customSCPStore.Close()
		}
		return nil
	}
	if cfg.CallCorrection.RecentBandBonusEnabled || cfg.CallCorrection.StabilizerEnabled {
		legacyStore := spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
		legacyStore.StartCleanup()
		r.recentBandStore = legacyStore
		r.cleanupRecentBand = legacyStore.StopCleanup
	}
	return nil
}

// openReplayOutputs creates all evidence artifacts before replay starts so a
// failed run still leaves the input/header context needed for troubleshooting.
func (r *replayRunner) openReplayOutputs() error {
	r.runbookSamplesPath = filepath.Join(r.outDir, "runbook_samples.log")
	runbookSamplesFile, err := os.OpenFile(r.runbookSamplesPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	r.runbookSamplesFile = runbookSamplesFile
	r.manifest = buildReplayManifest(r.startedAt, r.dayStart, r.cfg, r.archiveDir, r.outDir, r.zipPath, r.zipResult.Meta, r.runbookSamplesPath)

	csvParser, err := openRBNHistoryCSV(r.zipPath, r.dayCompact+".csv")
	if err != nil {
		return err
	}
	r.csvParser = csvParser
	r.manifest.CSV.Header = csvParser.Header()
	r.stabilityCollector = newReplayStabilityCollector(r.replayCfg.Stability)
	return nil
}

// replayDay streams the CSV once, emits fixed-interval samples, and force-drains
// temporal holds after the day so every accepted observation has a final
// decision in the artifacts.
func (r *replayRunner) replayDay() error {
	heap.Init(&r.temporalQueue)
	r.nextSampleAt = r.dayStart

	if err := r.emitSample(r.nextSampleAt); err != nil {
		return err
	}
	r.nextSampleAt = r.nextSampleAt.Add(sampleInterval)

	for {
		row, ok, err := r.csvParser.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("csv read: %w", err)
		}
		if err := r.processCSVRow(row, ok); err != nil {
			return err
		}
	}

	r.drainTemporal(r.dayEnd.Add(24*time.Hour), true)
	for !r.nextSampleAt.After(r.dayEnd) {
		if err := r.emitSample(r.nextSampleAt); err != nil {
			return err
		}
		r.nextSampleAt = r.nextSampleAt.Add(sampleInterval)
	}
	return nil
}

// processCSVRow enforces chronological replay. Timestamp regressions are hard
// errors because resolver state is time-ordered and would otherwise produce
// misleading A/B metrics.
func (r *replayRunner) processCSVRow(row rbnHistoryRow, ok bool) error {
	r.recordsTotal++
	if !ok {
		r.recordsSkippedBad++
		return nil
	}

	if row.Time.Before(r.dayStart) || !row.Time.Before(r.dayEnd) {
		return fmt.Errorf("csv record out of day bounds at #%d: ts=%s", r.recordsTotal, row.Time.UTC().Format(time.RFC3339))
	}
	if !r.lastRecordTime.IsZero() && row.Time.Before(r.lastRecordTime) {
		return fmt.Errorf("csv timestamp regression at #%d: prev=%s curr=%s", r.recordsTotal, r.lastRecordTime.UTC().Format(time.RFC3339), row.Time.UTC().Format(time.RFC3339))
	}
	r.lastRecordTime = row.Time

	for !r.nextSampleAt.After(row.Time) {
		if err := r.emitSample(r.nextSampleAt); err != nil {
			return err
		}
		r.nextSampleAt = r.nextSampleAt.Add(sampleInterval)
	}

	if !row.SpotClass.Accepted() {
		r.recordsSkippedMode++
		return nil
	}
	if !spot.IsCallCorrectionCandidate(row.Mode) {
		r.recordsSkippedMode++
		return nil
	}
	r.stabilityCollector.ObserveRaw(row)

	r.recordsProcessed++
	now := row.Time.UTC()
	r.drainTemporal(now, false)

	// Keep resolver state current before enqueuing evidence, but do not drain the
	// evidence for this spot until after observation (observe-before-drain).
	r.driver.Step(now)

	spotEntry := newReplaySpot(row)
	if spotEntry.IsBeacon {
		return nil
	}

	resolverEvidence, hasResolverEvidence := buildResolverEvidenceSnapshot(spotEntry, r.cfg.CallCorrection, r.adaptiveMinReports, now)
	if hasResolverEvidence {
		if accepted := r.resolver.Enqueue(resolverEvidence); !accepted {
			return fmt.Errorf("resolver enqueue failed at %s for key=%s", now.Format(time.RFC3339), resolverEvidence.Key.String())
		}
	}
	if r.maybeHoldTemporal(spotEntry, resolverEvidence, hasResolverEvidence, now) {
		return nil
	}

	outcome := maybeApplyResolverCorrectionReplay(
		spotEntry,
		r.resolver,
		resolverEvidence,
		hasResolverEvidence,
		r.cfg.CallCorrection,
		r.ctyDB,
		r.tracker,
		r.adaptiveMinReports,
		r.recentBandStore,
		now,
	)
	r.processOutcome(spotEntry, outcome, now)

	// Drain the evidence we enqueued for this spot after observation.
	r.driver.Step(now)
	return nil
}

// maybeHoldTemporal mirrors the runtime temporal decoder but records A/B
// outcomes instead of delivering telnet output.
func (r *replayRunner) maybeHoldTemporal(
	spotEntry *spot.Spot,
	resolverEvidence spot.ResolverEvidence,
	hasResolverEvidence bool,
	now time.Time,
) bool {
	if r.temporalDecoder == nil || !r.temporalDecoder.Enabled() || !hasResolverEvidence {
		return false
	}

	subject := normalizedDXCall(spotEntry)
	selection := correctionflow.ResolverPrimarySelection{}
	if subject != "" {
		selection = correctionflow.SelectResolverPrimarySnapshotForCall(r.resolver, resolverEvidence.Key, r.cfg.CallCorrection, subject)
	}
	if !selection.SnapshotOK || !r.temporalDecoder.ShouldHoldSelection(selection) {
		return false
	}

	id := r.temporalNextID
	r.temporalNextID++
	accepted, reason := r.temporalDecoder.Observe(correctionflow.TemporalObservation{
		ID:          id,
		ObservedAt:  now,
		Key:         resolverEvidence.Key,
		SubjectCall: subject,
		Selection:   selection,
	})
	if accepted {
		r.temporalPending[id] = replayTemporalPending{
			id:          id,
			spot:        spotEntry,
			evidence:    resolverEvidence,
			hasEvidence: hasResolverEvidence,
			maxAt:       now.Add(r.temporalDecoder.MaxWaitDuration()),
			selection:   selection,
		}
		heap.Push(&r.temporalQueue, &replayTemporalItem{
			id:  id,
			due: now.Add(r.temporalDecoder.LagDuration()),
			seq: r.temporalSeq,
		})
		r.temporalSeq++
		r.abMetrics.Temporal.ObservePending()

		// Enqueue happened before temporal hold; advance resolver state for
		// this observation now, then decide at lag release.
		r.driver.Step(now)
		return true
	}
	overflowDecision := correctionflow.TemporalDecision{
		ID:            id,
		Reason:        reason,
		CommitLatency: 0,
	}
	switch r.cfg.CallCorrection.TemporalDecoder.OverflowAction {
	case "abstain":
		overflowDecision.Action = correctionflow.TemporalDecisionActionAbstain
		r.abMetrics.Temporal.ObserveDecision(overflowDecision)
		spotEntry.Confidence = correctionflow.ResolverConfidenceGlyphForCall(selection.Snapshot, selection.SnapshotOK, subject)
		r.processOutcome(spotEntry, replayResolverApplyOutcome{
			Selection:  selection,
			Confidence: replayConfidenceOutcome{Final: normalizeConfidenceGlyph(spotEntry.Confidence)},
		}, now)
		r.driver.Step(now)
		return true
	case "bypass":
		overflowDecision.Action = correctionflow.TemporalDecisionActionBypass
	default:
		overflowDecision.Action = correctionflow.TemporalDecisionActionFallbackResolver
	}
	r.abMetrics.Temporal.ObserveDecision(overflowDecision)
	return false
}

// emitSample captures resolver pressure at fixed wall-clock replay intervals.
// Any resolver drop is a failure because this tool is used to validate bounded
// settings, not to tolerate hidden analysis loss.
func (r *replayRunner) emitSample(ts time.Time) error {
	ts = ts.UTC()
	r.driver.Sweep(ts)

	metrics := r.resolver.MetricsSnapshot()
	if metrics.DropQueueFull > 0 || metrics.DropMaxKeys > 0 || metrics.DropMaxCandidates > 0 || metrics.DropMaxReporters > 0 {
		return fmt.Errorf("resolver drops observed at %s: Q=%d K=%d C=%d R=%d",
			ts.Format(time.RFC3339),
			metrics.DropQueueFull,
			metrics.DropMaxKeys,
			metrics.DropMaxCandidates,
			metrics.DropMaxReporters,
		)
	}

	prefix := ts.Format("2006/01/02 15:04:05")
	fmt.Fprintf(r.runbookSamplesFile, "%s %s\n", prefix, formatCorrectionDecisionSummary(r.tracker))
	fmt.Fprintf(r.runbookSamplesFile, "%s %s\n", prefix, formatResolverSummaryFromMetrics(metrics))

	r.samples = append(r.samples, resolverSample{TS: ts, Metrics: metrics})
	return nil
}

// processOutcome feeds replay metrics and support-memory state from the same
// post-correction point runtime would use before output.
func (r *replayRunner) processOutcome(spotEntry *spot.Spot, outcome replayResolverApplyOutcome, now time.Time) {
	if spotEntry == nil {
		return
	}
	if outcome.Suppress {
		return
	}
	r.abMetrics.ObserveAppliedOutput(outcome.Confidence)
	r.abMetrics.ObserveResolverSelection(outcome.Selection)
	r.abMetrics.ObserveResolverSnapshot(outcome.Selection.Snapshot, outcome.Selection.SnapshotOK)
	r.abMetrics.ObserveResolverRecentPlus1Gate(outcome.Gate, outcome.GateEvaluated)
	r.abMetrics.ObserveResolverBayesGate(outcome.Gate, outcome.GateEvaluated)

	spotEntry.RefreshBeaconFlag()
	if outcome.Applied {
		band := spotEntry.BandNorm
		if band == "" || band == "???" {
			band = spot.FreqToBand(spotEntry.Frequency)
		}
		r.stabilityCollector.ObserveApplied(now.Unix(), outcome.Winner, spotEntry.Frequency, band)
	}

	if stabilizerDelayProxyEligible(spotEntry, r.recentBandStore, r.cfg.CallCorrection) {
		delayDecision := evaluateStabilizerDelay(spotEntry, r.recentBandStore, r.cfg.CallCorrection, now, outcome.Selection.Snapshot, outcome.Selection.SnapshotOK)
		r.abMetrics.StabilizerDelayProxy.Observe(delayDecision)
	}

	recordRecentBandObservation(spotEntry, r.recentBandStore, r.cfg.CallCorrection)
}

// drainTemporal releases due temporal holds through the replay correction path
// so delayed and immediate observations are scored consistently.
func (r *replayRunner) drainTemporal(now time.Time, force bool) {
	if r.temporalDecoder == nil || !r.temporalDecoder.Enabled() {
		return
	}
	due := popReplayTemporalDue(&r.temporalQueue, now)
	for _, item := range due {
		if item == nil {
			continue
		}
		pending, ok := r.temporalPending[item.id]
		if !ok || pending.spot == nil {
			continue
		}
		decision := r.temporalDecoder.Evaluate(item.id, now, force)
		if decision.Action == correctionflow.TemporalDecisionActionDefer {
			nextDue := pending.maxAt
			if nextDue.Before(now) {
				nextDue = now
			}
			heap.Push(&r.temporalQueue, &replayTemporalItem{
				id:  item.id,
				due: nextDue,
				seq: r.temporalSeq,
			})
			r.temporalSeq++
			continue
		}

		delete(r.temporalPending, item.id)
		r.abMetrics.Temporal.ObserveDecision(decision)

		var outcome replayResolverApplyOutcome
		switch decision.Action {
		case correctionflow.TemporalDecisionActionApply:
			selection := decision.Selection
			outcome = maybeApplyResolverCorrectionReplayWithSelectionOverride(
				pending.spot,
				r.resolver,
				pending.evidence,
				pending.hasEvidence,
				r.cfg.CallCorrection,
				r.ctyDB,
				r.tracker,
				r.adaptiveMinReports,
				r.recentBandStore,
				now,
				&selection,
			)
		case correctionflow.TemporalDecisionActionAbstain:
			outcome = replayResolverApplyOutcome{
				Selection:  pending.selection,
				Confidence: replayConfidenceOutcome{Final: normalizeConfidenceGlyph(pending.spot.Confidence)},
			}
			pending.spot.Confidence = correctionflow.ResolverConfidenceGlyphForCall(
				pending.selection.Snapshot,
				pending.selection.SnapshotOK,
				normalizedDXCall(pending.spot),
			)
			outcome.Confidence.Final = normalizeConfidenceGlyph(pending.spot.Confidence)
		default:
			outcome = maybeApplyResolverCorrectionReplay(
				pending.spot,
				r.resolver,
				pending.evidence,
				pending.hasEvidence,
				r.cfg.CallCorrection,
				r.ctyDB,
				r.tracker,
				r.adaptiveMinReports,
				r.recentBandStore,
				now,
			)
		}
		r.processOutcome(pending.spot, outcome, now)
	}
}

// finalize writes derived interval, gate, manifest, and run-history artifacts in
// one phase so downstream comparisons can treat the output directory as complete
// only after this returns nil.
func (r *replayRunner) finalize() error {
	if err := r.runbookSamplesFile.Sync(); err != nil {
		return fmt.Errorf("sync runbook samples file: %w", err)
	}

	if len(r.samples) < 2 {
		return fmt.Errorf("need at least 2 resolver samples, got %d", len(r.samples))
	}

	intervals, hits, gates := computeIntervalsAndGates(r.samples, shadowResolverQueueSize)
	stabilitySummary := r.stabilityCollector.Evaluate(r.dayStart.Unix())
	gates.Overall.Stability = stabilitySummary
	gates.Overall.ABMetrics = r.abMetrics
	if err := writeIntervalsCSV(r.manifest.Outputs.IntervalsCSV, intervals); err != nil {
		return err
	}
	if err := writeIntervalsCSV(r.manifest.Outputs.ThresholdHitsCSV, hits); err != nil {
		return err
	}
	if err := writeJSONAtomic(r.manifest.Outputs.GatesJSON, gates); err != nil {
		return err
	}

	r.manifest.CSV.RecordsTotal = r.recordsTotal
	r.manifest.CSV.RecordsProcessed = r.recordsProcessed
	r.manifest.CSV.RecordsSkippedMode = r.recordsSkippedMode
	r.manifest.CSV.RecordsSkippedBad = r.recordsSkippedBad

	last := r.samples[len(r.samples)-1].Metrics
	r.manifest.Results.Drops.QueueFull = last.DropQueueFull
	r.manifest.Results.Drops.MaxKeys = last.DropMaxKeys
	r.manifest.Results.Drops.MaxCandidates = last.DropMaxCandidates
	r.manifest.Results.Drops.MaxReporters = last.DropMaxReporters
	r.manifest.Results.Stability = stabilitySummary
	r.manifest.Results.ABMetrics = r.abMetrics

	r.manifest.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeJSONAtomic(r.manifest.Outputs.ManifestJSON, r.manifest); err != nil {
		return err
	}
	return writeReplayRunHistory(r.archiveDir, r.request.replayConfigPath, r.cfg, r.manifest)
}

func resolveReplayConfigDir(override string, replayCfg replayConfig, envValue string) string {
	configDir := strings.TrimSpace(override)
	if configDir == "" {
		configDir = strings.TrimSpace(replayCfg.ClusterConfigDir)
	}
	if configDir == "" {
		if v := strings.TrimSpace(envValue); v != "" {
			configDir = v
		} else {
			configDir = defaultConfigPath
		}
	}
	return configDir
}

func resolveReplayArchiveDir(override string, replayCfg replayConfig) string {
	archiveDir := strings.TrimSpace(override)
	if archiveDir == "" {
		archiveDir = strings.TrimSpace(replayCfg.ArchiveDir)
	}
	return filepath.Clean(archiveDir)
}

func buildReplayOutputDir(archiveDir string, dayStart time.Time) string {
	return filepath.Join(archiveDir, "rbn_replay", dayStart.Format("2006-01-02"))
}

func buildReplayZipURL(dayCompact string) string {
	return fmt.Sprintf("https://data.reversebeacon.net/rbn_history/%s.zip", dayCompact)
}

func buildReplayZipPath(archiveDir, dayCompact string) string {
	return filepath.Join(archiveDir, dayCompact+".zip")
}

func buildReplayManifest(
	startedAt time.Time,
	dayStart time.Time,
	cfg *config.Config,
	archiveDir string,
	outDir string,
	zipPath string,
	zipMeta download.Metadata,
	runbookSamplesPath string,
) replayManifest {
	manifest := replayManifest{
		DateUTC:      dayStart.Format("2006-01-02"),
		ArchiveDir:   archiveDir,
		OutputDir:    outDir,
		InputZipPath: zipPath,
		InputZipMeta: zipMeta,
		StartedAtUTC: startedAt.Format(time.RFC3339Nano),
		GoVersion:    runtime.Version(),
	}
	if cfg != nil {
		manifest.ConfigLoadedFrom = cfg.LoadedFrom
	}
	manifest.Outputs.RunbookSamplesLog = runbookSamplesPath
	manifest.Outputs.ManifestJSON = filepath.Join(outDir, "manifest.json")
	manifest.Outputs.IntervalsCSV = filepath.Join(outDir, "resolver_intervals.csv")
	manifest.Outputs.ThresholdHitsCSV = filepath.Join(outDir, "resolver_threshold_hits.csv")
	manifest.Outputs.GatesJSON = filepath.Join(outDir, "gates.json")
	return manifest
}

func newReplaySpot(row rbnHistoryRow) *spot.Spot {
	spotEntry := spot.NewSpotNormalized(row.DXCall, row.Spotter, row.FreqKHz, row.Mode)
	spotEntry.Time = row.Time.UTC()
	spotEntry.Report = row.ReportDB
	spotEntry.HasReport = true
	spotEntry.SourceType = spot.SourceRBN
	spotEntry.SourceNode = "RBN-HISTORY"
	spotEntry.EnsureNormalized()
	if row.SpotClass.IsBeacon() {
		spotEntry.BeaconSourceClass = true
		if row.SpotClass == rbnfeed.SpotClassNCDXFB {
			spotEntry.BeaconComment = spot.BeaconCommentNCDXF
		}
	}
	spotEntry.RefreshBeaconFlag()
	return spotEntry
}
