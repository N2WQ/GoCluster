package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
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
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/uls"
)

const maxDisagreementsPerClass = 500

func main() {
	var (
		dateFlag       = flag.String("date", "", "UTC date to replay (YYYY-MM-DD or YYYYMMDD)")
		configDirFlag  = flag.String("config", "", "Config directory (defaults to DXC_CONFIG_PATH or data/config)")
		archiveDirFlag = flag.String("archive-dir", "archive data", "Archive directory for downloads and outputs")
		forceDownload  = flag.Bool("force-download", false, "Force re-download of the RBN history zip")
	)
	flag.Parse()

	startedAt := time.Now().UTC()

	if strings.TrimSpace(*dateFlag) == "" {
		fatalf("missing required -date (YYYY-MM-DD or YYYYMMDD)")
	}
	dayStart, dayCompact, err := parseUTCDate(*dateFlag)
	must(err)
	dayEnd := dayStart.Add(24 * time.Hour)

	configDir := strings.TrimSpace(*configDirFlag)
	if configDir == "" {
		if v := strings.TrimSpace(os.Getenv(envConfigPath)); v != "" {
			configDir = v
		} else {
			configDir = defaultConfigPath
		}
	}
	cfg, err := config.Load(configDir)
	must(err)
	if cfg == nil {
		fatalf("config loaded as nil")
	}
	if !cfg.CallCorrection.Enabled {
		fatalf("call_correction.enabled must be true for replay (config=%s)", cfg.LoadedFrom)
	}

	archiveDir := filepath.Clean(strings.TrimSpace(*archiveDirFlag))
	if archiveDir == "" {
		fatalf("archive dir is empty")
	}
	must(ensureDir(archiveDir))

	outDir := filepath.Join(archiveDir, "rbn_replay", dayStart.Format("2006-01-02"))
	must(ensureDir(outDir))

	zipURL := fmt.Sprintf("https://data.reversebeacon.net/rbn_history/%s.zip", dayCompact)
	zipPath := filepath.Join(archiveDir, dayCompact+".zip")
	zipResult, err := download.Download(context.Background(), download.Request{
		URL:         zipURL,
		Destination: zipPath,
		Timeout:     10 * time.Minute,
		Force:       *forceDownload,
		UserAgent:   "dxcluster-rbn-replay",
	})
	must(err)

	uls.SetLicenseChecksEnabled(cfg.FCCULS.Enabled)
	uls.SetLicenseCacheTTL(time.Duration(cfg.FCCULS.CacheTTLSeconds) * time.Second)

	allowlistPath := strings.TrimSpace(cfg.FCCULS.AllowlistPath)
	if allowlistPath != "" {
		if _, err := os.Stat(allowlistPath); err != nil {
			fatalf("fcc_uls.allowlist_path missing/unreadable %s: %v", allowlistPath, err)
		}
		uls.SetAllowlistPath(allowlistPath)
	} else {
		uls.SetAllowlistPath("")
	}

	if cfg.FCCULS.Enabled {
		dbPath := strings.TrimSpace(cfg.FCCULS.DBPath)
		if dbPath == "" {
			fatalf("fcc_uls.enabled=true but fcc_uls.db_path is empty (config=%s)", cfg.LoadedFrom)
		}
		if _, err := os.Stat(dbPath); err != nil {
			fatalf("fcc_uls.db_path missing/unreadable %s: %v", dbPath, err)
		}
		uls.SetLicenseDBPath(dbPath)
	} else {
		uls.SetLicenseDBPath("")
	}

	var ctyDB *cty.CTYDatabase
	if cfg.CTY.Enabled {
		ctyPath := strings.TrimSpace(cfg.CTY.File)
		if ctyPath == "" {
			fatalf("cty.enabled=true but cty.file is empty (config=%s)", cfg.LoadedFrom)
		}
		if _, err := os.Stat(ctyPath); err != nil {
			fatalf("cty.file missing/unreadable %s: %v", ctyPath, err)
		}
		loaded, err := cty.LoadCTYDatabase(ctyPath)
		must(err)
		ctyDB = loaded
	}

	spot.ConfigureMorseWeights(cfg.CallCorrection.MorseWeights.Insert, cfg.CallCorrection.MorseWeights.Delete, cfg.CallCorrection.MorseWeights.Sub, cfg.CallCorrection.MorseWeights.Scale)
	spot.ConfigureBaudotWeights(cfg.CallCorrection.BaudotWeights.Insert, cfg.CallCorrection.BaudotWeights.Delete, cfg.CallCorrection.BaudotWeights.Sub, cfg.CallCorrection.BaudotWeights.Scale)

	pinPriors := true
	if cfg.CallCorrection.CallQualityPinPriors != nil {
		pinPriors = *cfg.CallCorrection.CallQualityPinPriors
	}
	// Disable wall-clock cleanup for deterministic replay; TTL eviction still happens on access/add.
	spot.ConfigureCallQualityStore(
		time.Duration(cfg.CallCorrection.CallQualityTTLSeconds)*time.Second,
		cfg.CallCorrection.CallQualityMaxEntries,
		0,
		pinPriors,
	)
	if priors := strings.TrimSpace(cfg.CallCorrection.QualityPriorsFile); priors != "" {
		if _, err := os.Stat(priors); err != nil {
			fatalf("call_correction.quality_priors_file missing/unreadable %s: %v", priors, err)
		}
		if _, err := spot.LoadCallQualityPriors(priors, cfg.CallCorrection.QualityBinHz); err != nil {
			fatalf("load call quality priors %s: %v", priors, err)
		}
	}

	spotterReliability, spotterReliabilityCW, spotterReliabilityRTTY, err := loadSpotterReliability(cfg.CallCorrection)
	must(err)
	confusionModel, err := loadConfusionModel(cfg.CallCorrection)
	must(err)

	adaptiveMinReports := spot.NewAdaptiveMinReports(cfg.CallCorrection)

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
	}
	cooldownCleanupInterval := time.Duration(cfg.CallCorrection.CooldownTTLSeconds) * time.Second
	if callCooldown != nil && cooldownCleanupInterval <= 0 {
		cooldownCleanupInterval = time.Minute
	}
	lastCooldownCleanup := time.Time{}

	var recentBandStore *spot.RecentBandStore
	if cfg.CallCorrection.Enabled && (cfg.CallCorrection.RecentBandBonusEnabled || cfg.CallCorrection.StabilizerEnabled) {
		recentBandStore = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	}

	knownCallset, err := loadKnownCallset(cfg.KnownCalls.File)
	must(err)

	tracker := stats.NewTracker()
	correctionIndex := spot.NewCorrectionIndex()

	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
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
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.CallCorrection.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	must(err)

	runbookSamplesPath := filepath.Join(outDir, "runbook_samples.log")
	runbookSamplesFile, err := os.OpenFile(runbookSamplesPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	must(err)
	defer runbookSamplesFile.Close()

	disagreementsPath := filepath.Join(outDir, "disagreements_sample.csv")
	disagreeFile, err := os.OpenFile(disagreementsPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	must(err)
	defer disagreeFile.Close()
	disagreeCSV := csv.NewWriter(disagreeFile)
	must(disagreeCSV.Write([]string{
		"TS", "Band", "Mode", "FreqKHz", "Spotter", "PreCall", "FinalCall", "Corrected",
		"SnapshotState", "SnapshotWinner", "SnapshotRunner", "WinnerSupport", "RunnerSupport", "TotalReporters",
		"Class",
	}))

	manifest := replayManifest{
		DateUTC:          dayStart.Format("2006-01-02"),
		ConfigLoadedFrom: cfg.LoadedFrom,
		ArchiveDir:       archiveDir,
		OutputDir:        outDir,
		InputZipPath:     zipPath,
		InputZipMeta:     zipResult.Meta,
		StartedAtUTC:     startedAt.Format(time.RFC3339Nano),
		GoVersion:        runtime.Version(),
	}
	manifest.Outputs.RunbookSamplesLog = runbookSamplesPath
	manifest.Outputs.DisagreementsSampleCSV = disagreementsPath
	manifest.Outputs.ManifestJSON = filepath.Join(outDir, "manifest.json")
	manifest.Outputs.IntervalsCSV = filepath.Join(outDir, "resolver_intervals.csv")
	manifest.Outputs.ThresholdHitsCSV = filepath.Join(outDir, "resolver_threshold_hits.csv")
	manifest.Outputs.GatesJSON = filepath.Join(outDir, "gates.json")

	csvParser, err := openRBNHistoryCSV(zipPath, dayCompact+".csv")
	must(err)
	defer csvParser.Close()
	manifest.CSV.Header = csvParser.Header()

	var (
		samples        []resolverSample
		nextSampleAt   = dayStart
		lastRecordTime time.Time

		recordsTotal       int64
		recordsProcessed   int64
		recordsSkippedMode int64
		recordsSkippedBad  int64

		disagreeCounts = map[string]int{"SP": 0, "DW": 0, "UC": 0}
	)

	emitSample := func(ts time.Time) {
		ts = ts.UTC()
		if callCooldown != nil && cooldownCleanupInterval > 0 {
			if lastCooldownCleanup.IsZero() || ts.Sub(lastCooldownCleanup) >= cooldownCleanupInterval {
				callCooldown.Cleanup(ts)
				lastCooldownCleanup = ts
			}
		}

		driver.Sweep(ts)

		metrics := resolver.MetricsSnapshot()
		if metrics.DropQueueFull > 0 || metrics.DropMaxKeys > 0 || metrics.DropMaxCandidates > 0 || metrics.DropMaxReporters > 0 {
			fatalf("resolver drops observed at %s: Q=%d K=%d C=%d R=%d",
				ts.Format(time.RFC3339),
				metrics.DropQueueFull,
				metrics.DropMaxKeys,
				metrics.DropMaxCandidates,
				metrics.DropMaxReporters,
			)
		}

		prefix := ts.Format("2006/01/02 15:04:05")
		fmt.Fprintf(runbookSamplesFile, "%s %s\n", prefix, formatCorrectionDecisionSummary(tracker))
		fmt.Fprintf(runbookSamplesFile, "%s %s\n", prefix, formatResolverSummaryFromMetrics(metrics))

		samples = append(samples, resolverSample{TS: ts, Metrics: metrics})
	}

	// Emit sample at day start.
	emitSample(nextSampleAt)
	nextSampleAt = nextSampleAt.Add(sampleInterval)

	for {
		row, ok, err := csvParser.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fatalf("csv read: %v", err)
		}
		recordsTotal++
		if !ok {
			recordsSkippedBad++
			continue
		}

		if row.Time.Before(dayStart) || !row.Time.Before(dayEnd) {
			fatalf("csv record out of day bounds at #%d: ts=%s", recordsTotal, row.Time.UTC().Format(time.RFC3339))
		}
		if !lastRecordTime.IsZero() && row.Time.Before(lastRecordTime) {
			fatalf("csv timestamp regression at #%d: prev=%s curr=%s", recordsTotal, lastRecordTime.UTC().Format(time.RFC3339), row.Time.UTC().Format(time.RFC3339))
		}
		lastRecordTime = row.Time

		for !nextSampleAt.After(row.Time) {
			emitSample(nextSampleAt)
			nextSampleAt = nextSampleAt.Add(sampleInterval)
		}

		if !spot.IsCallCorrectionCandidate(row.Mode) {
			recordsSkippedMode++
			continue
		}

		recordsProcessed++
		now := row.Time.UTC()

		// Keep resolver state current before enqueuing evidence, but do not drain the
		// evidence for this spot until after observation (observe-before-drain).
		driver.Step(now)

		spotEntry := spot.NewSpotNormalized(row.DXCall, row.Spotter, row.FreqKHz, row.Mode)
		spotEntry.Time = now
		spotEntry.Report = row.ReportDB
		spotEntry.HasReport = true
		spotEntry.SourceType = spot.SourceRBN
		spotEntry.SourceNode = "RBN-HISTORY"
		spotEntry.EnsureNormalized()
		spotEntry.RefreshBeaconFlag()
		if spotEntry.IsBeacon {
			continue
		}

		resolverEvidence, hasResolverEvidence := buildResolverEvidenceSnapshot(spotEntry, cfg.CallCorrection, adaptiveMinReports, now)
		if hasResolverEvidence {
			if accepted := resolver.Enqueue(resolverEvidence); !accepted {
				fatalf("resolver enqueue failed at %s for key=%s", now.Format(time.RFC3339), resolverEvidence.Key.String())
			}
		}

		preCorrectionCall := normalizedDXCall(spotEntry)
		suppress := maybeApplyCallCorrectionReplay(
			spotEntry,
			correctionIndex,
			cfg.CallCorrection,
			ctyDB,
			tracker,
			callCooldown,
			adaptiveMinReports,
			spotterReliability,
			spotterReliabilityCW,
			spotterReliabilityRTTY,
			confusionModel,
			recentBandStore,
			knownCallset,
			now,
		)

		// Enqueue happened before correction; even suppressed spots should still
		// advance resolver state for future observations.
		if suppress {
			driver.Step(now)
			continue
		}

		spotEntry.RefreshBeaconFlag()

		if hasResolverEvidence {
			observeResolverCurrentDecision(resolver, resolverEvidence.Key, spotEntry, preCorrectionCall)

			row := classifyDisagreementSample(resolver, resolverEvidence.Key, spotEntry, preCorrectionCall)
			if row != nil {
				count := disagreeCounts[row.Class]
				if count < maxDisagreementsPerClass {
					disagreeCounts[row.Class] = count + 1
					must(disagreeCSV.Write(disagreementCSVRow(row)))
				}
			}
		}

		recordRecentBandObservation(spotEntry, recentBandStore, cfg.CallCorrection)

		// Drain the evidence we enqueued for this spot after observation.
		driver.Step(now)
	}

	// Flush remaining samples to day end (inclusive).
	for !nextSampleAt.After(dayEnd) {
		emitSample(nextSampleAt)
		nextSampleAt = nextSampleAt.Add(sampleInterval)
	}

	disagreeCSV.Flush()
	must(disagreeCSV.Error())

	if err := runbookSamplesFile.Sync(); err != nil {
		fatalf("sync runbook samples file: %v", err)
	}

	if len(samples) < 2 {
		fatalf("need at least 2 resolver samples, got %d", len(samples))
	}

	intervals, hits, gates := computeIntervalsAndGates(samples, shadowResolverQueueSize)
	must(writeIntervalsCSV(manifest.Outputs.IntervalsCSV, intervals))
	must(writeIntervalsCSV(manifest.Outputs.ThresholdHitsCSV, hits))
	must(writeJSONAtomic(manifest.Outputs.GatesJSON, gates))

	manifest.CSV.RecordsTotal = recordsTotal
	manifest.CSV.RecordsProcessed = recordsProcessed
	manifest.CSV.RecordsSkippedMode = recordsSkippedMode
	manifest.CSV.RecordsSkippedBad = recordsSkippedBad

	last := samples[len(samples)-1].Metrics
	manifest.Results.ComparableDecisions = last.DecisionsComparable
	manifest.Results.AgreementPct = gates.Overall.AgreementPct
	manifest.Results.DWPct = gates.Overall.DWPct
	manifest.Results.SPPct = gates.Overall.SPPct
	manifest.Results.UCPct = gates.Overall.UCPct
	manifest.Results.Drops.QueueFull = last.DropQueueFull
	manifest.Results.Drops.MaxKeys = last.DropMaxKeys
	manifest.Results.Drops.MaxCandidates = last.DropMaxCandidates
	manifest.Results.Drops.MaxReporters = last.DropMaxReporters

	manifest.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	must(writeJSONAtomic(manifest.Outputs.ManifestJSON, manifest))
}

func loadSpotterReliability(cfg config.CallCorrectionConfig) (base spot.SpotterReliability, cw spot.SpotterReliability, rtty spot.SpotterReliability, err error) {
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFile); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, loadErr
		}
		base = rel
	}
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFileCW); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_cw missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, loadErr
		}
		cw = rel
	}
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFileRTTY); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_rtty missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, loadErr
		}
		rtty = rel
	}
	return base, cw, rtty, nil
}

func loadConfusionModel(cfg config.CallCorrectionConfig) (*spot.ConfusionModel, error) {
	if !cfg.ConfusionModelEnabled {
		return nil, nil
	}
	modelPath := strings.TrimSpace(cfg.ConfusionModelFile)
	if modelPath == "" {
		return nil, errors.New("call_correction.confusion_model_enabled=true but confusion_model_file is empty")
	}
	if _, err := os.Stat(modelPath); err != nil {
		return nil, fmt.Errorf("call_correction.confusion_model_file missing/unreadable %s: %w", modelPath, err)
	}
	loaded, err := spot.LoadConfusionModel(modelPath)
	if err != nil {
		return nil, err
	}
	return loaded, nil
}

func loadKnownCallset(path string) (*spot.KnownCallsigns, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("known_calls.file missing/unreadable %s: %w", path, err)
	}
	return spot.LoadKnownCallsigns(path)
}
