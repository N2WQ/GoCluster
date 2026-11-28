// File role: CLI entry point for cmd/prop_loro, an offline single-pass
// head-to-head LORO evaluator comparing embedded v4.3 internal/propprior
// against VOACAP on a captured live GoCluster spot + space-weather dataset.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/propprior"
	"dxcluster/pathreliability"
)

func main() {
	var (
		captureDir     = flag.String("capture-dir", "data/prop_capture", "Directory containing spots-*.csv.gz and spaceweather-*.jsonl")
		configDir      = flag.String("config", "data/config", "Cluster config directory (path_reliability.yaml, h3_table_path, etc.)")
		outDir         = flag.String("out-dir", "data/prop_loro_out", "Output directory for scorecard.json, scorecard.md, and voacap_cache.jsonl")
		voacapCache    = flag.String("voacap-cache", "", "Optional path to resumable VOACAP directed cache JSONL (defaults to <out-dir>/voacap_cache.jsonl)")
		holdoutUnit    = flag.String("holdout-unit", "receiver", "LORO holdout unit: receiver or fine_cell")
		folds          = flag.Int("k", 5, "Number of deterministic LORO folds")
		fold           = flag.Int("fold", 0, "Selected LORO fold index in [0, k)")
		warmup         = flag.Duration("warmup", 30*time.Minute, "Initial Predictor warm-up duration before scoring held-out spots")
		excludeColdSW  = flag.Bool("exclude-cold-sw", true, "Exclude spots prior to first live space-weather commit (gen == 0)")
		maxVOACAPPairs = flag.Int("max-voacap-pairs", 0, "Optional cap on unique undirected cell pairs evaluated by VOACAP (0 = unlimited)")
		voacapSSN      = flag.Int("voacap-ssn", 0, "Optional fixed VOACAP SSN override (0 = use causal voacap_ssn from spaceweather JSONL)")
		startUTC       = flag.String("start", "", "Optional inclusive UTC start timestamp (RFC3339)")
		endUTC         = flag.String("end", "", "Optional inclusive UTC end timestamp (RFC3339)")
		maxNegBandHour = flag.Int("max-neg-per-band-hour", loroMaxNegPerBandHour, "Cap on active non-heard negative pairs sampled per band-hour")
	)
	flag.Parse()
	if flag.NArg() > 0 {
		log.Fatalf("prop_loro: unexpected positional arguments %v (hint: quote values starting with '.' in PowerShell, e.g. -out-dir \".tmp/out\")", flag.Args())
	}

	if err := runCLI(cliOptions{
		captureDir:     *captureDir,
		configDir:      *configDir,
		outDir:         *outDir,
		voacapCache:    *voacapCache,
		holdoutUnit:    *holdoutUnit,
		folds:          *folds,
		fold:           *fold,
		warmup:         *warmup,
		excludeColdSW:  *excludeColdSW,
		maxVOACAPPairs: *maxVOACAPPairs,
		voacapSSN:      *voacapSSN,
		startUTC:       *startUTC,
		endUTC:         *endUTC,
		maxNegBandHour: *maxNegBandHour,
	}); err != nil {
		log.Fatalf("prop_loro: %v", err)
	}
}

type cliOptions struct {
	captureDir     string
	configDir      string
	outDir         string
	voacapCache    string
	holdoutUnit    string
	folds          int
	fold           int
	warmup         time.Duration
	excludeColdSW  bool
	maxVOACAPPairs int
	voacapSSN      int
	startUTC       string
	endUTC         string
	maxNegBandHour int
}

func runCLI(opts cliOptions) error {
	startUnix, err := parseOptionalRFC3339(opts.startUTC)
	if err != nil {
		return fmt.Errorf("invalid -start: %w", err)
	}
	endUnix, err := parseOptionalRFC3339(opts.endUTC)
	if err != nil {
		return fmt.Errorf("invalid -end: %w", err)
	}

	cfg, err := config.Load(opts.configDir)
	if err != nil {
		return fmt.Errorf("load cluster config from %q: %w", opts.configDir, err)
	}
	if err := pathreliability.InitRequiredH3Mappings(cfg.H3TablePath); err != nil {
		return fmt.Errorf("init H3 mappings (%q): %w", cfg.H3TablePath, err)
	}
	model, err := propprior.LoadEmbedded()
	if err != nil {
		return fmt.Errorf("load embedded v4.3 propprior: %w", err)
	}

	timeline, swFiles, err := loadSpaceWeatherTimeline(opts.captureDir)
	if err != nil {
		return err
	}
	spotFiles, err := discoverSpotFiles(opts.captureDir)
	if err != nil {
		return err
	}

	cachePath := strings.TrimSpace(opts.voacapCache)
	if cachePath == "" {
		cachePath = filepath.Join(opts.outDir, "voacap_cache.jsonl")
	}
	voacapEval, err := newRunnerVOACAPEvaluator(cfg.PathReliability.VOACAPFallback, cachePath, opts.maxVOACAPPairs)
	if err != nil {
		return err
	}
	defer func() { _ = voacapEval.Close() }()

	engine, err := newLOROEngine(loroConfig{
		HoldoutUnit:    opts.holdoutUnit,
		Folds:          opts.folds,
		Fold:           opts.fold,
		WarmupSeconds:  int64(opts.warmup / time.Second),
		ExcludeColdSW:  opts.excludeColdSW,
		VOACAPSSNFixed: opts.voacapSSN,
		MaxNegBandHour: opts.maxNegBandHour,
		ProgressEvery:  30 * time.Second,
	}, cfg.PathReliability, model, voacapEval, timeline)
	if err != nil {
		return err
	}

	ctx := context.Background()
	log.Printf("prop_loro starting: capture=%s files=%d sw_files=%d holdout=%s fold=%d/%d warmup=%s max_voacap_pairs=%d",
		opts.captureDir, len(spotFiles), len(swFiles), opts.holdoutUnit, opts.fold, opts.folds, opts.warmup, opts.maxVOACAPPairs)

	readStats, err := streamSpotFiles(spotFiles, startUnix, endUnix, func(rec capturedSpot) error {
		return engine.processSpot(ctx, rec)
	})
	if err != nil {
		return err
	}
	scorecard, err := engine.finalize(ctx, opts.captureDir, spotFiles, swFiles, readStats)
	if err != nil {
		return err
	}
	if err := voacapEval.Close(); err != nil {
		return fmt.Errorf("flush voacap cache: %w", err)
	}
	jsonPath, mdPath, err := writeScorecardArtifacts(opts.outDir, scorecard)
	if err != nil {
		return err
	}
	log.Printf("prop_loro complete: matched=%d (fallback=%d, live=%d) voacap_calls=%d cache_hits=%d -> %s, %s",
		scorecard.Manifest.HeldOutHFMatched,
		scorecard.RegimeBFallbackAll.MatchedCount,
		scorecard.RegimeBCombined.MatchedCount,
		scorecard.Manifest.VOACAPStats.RunnerCalls,
		scorecard.Manifest.VOACAPStats.CacheHits,
		jsonPath, mdPath)
	return nil
}

func parseOptionalRFC3339(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, err
	}
	return t.UTC().Unix(), nil
}
