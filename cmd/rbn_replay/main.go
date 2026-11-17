// File role: Owns the rbn_replay CLI entry point and optional model loading.
// Crawler notes: Start here for replay flags, config path selection, reliability
// file loading, and confusion-model enablement before the deterministic runner
// processes an archived RBN day.
// Related docs: docs/rbn_replay.md, spot/README.md.
// Related tests: cmd/rbn_replay/*_test.go.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"dxcluster/config"
	"dxcluster/spot"
)

func main() {
	var (
		dateFlag         = flag.String("date", "", "UTC date to replay (YYYY-MM-DD or YYYYMMDD)")
		replayConfigFlag = flag.String("replay-config", defaultReplayConfigPath, "Replay config YAML path")
		configDirFlag    = flag.String("config", "", "Cluster config directory override (defaults to replay config, DXC_CONFIG_PATH, or data/config)")
		archiveDirFlag   = flag.String("archive-dir", "", "Archive directory override (defaults to replay config)")
		forceDownload    = flag.Bool("force-download", false, "Force re-download of the RBN history zip (overrides replay config)")
		studyPlan        = flag.String("study-plan", "", "Frozen CCv2 study JSON (local archives only)")
		studyFreeze      = flag.String("study-freeze", "", "SHA256 of the already frozen study plan")
		studyArm         = flag.String("study-arm", "", "Study arm: shipped, repaired_off, or repaired_on")
		studyCandidate   = flag.String("study-candidate", "", "Complete predeclared study candidate ID")
		studyPhase       = flag.String("study-phase", "holdout", "Study population: tuning or holdout")
		studyOut         = flag.String("study-out", "", "New study output directory (must not exist)")
		studyCompare     = flag.String("study-compare", "", "Comma-separated complete study report paths to compare")
	)
	flag.Parse()
	studyRequested := false
	var replayOverrides []string
	flag.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "study-") {
			studyRequested = true
		} else if f.Name != "date" {
			replayOverrides = append(replayOverrides, f.Name)
		}
	})
	if studyRequested {
		if len(replayOverrides) > 0 || len(flag.Args()) > 0 {
			must(errors.New("study uses only frozen plan inputs; ordinary replay overrides and positional arguments are not allowed"))
		}
		must(runCCV2Study(studyCLIRequest{Plan: *studyPlan, Freeze: *studyFreeze, Arm: *studyArm,
			Candidate: *studyCandidate, Phase: *studyPhase, Output: *studyOut, Compare: *studyCompare, Date: *dateFlag}))
		return
	}
	must(runReplay(replayRequest{
		dateValue:        *dateFlag,
		replayConfigPath: *replayConfigFlag,
		configDir:        *configDirFlag,
		archiveDir:       *archiveDirFlag,
		forceDownload:    *forceDownload,
	}))
}

// loadSpotterReliability resolves optional reliability files up front so replay
// uses the same weighting inputs as the production correction resolver.
func loadSpotterReliability(cfg config.CallCorrectionConfig) (base spot.SpotterReliability, cw spot.SpotterReliability, rtty spot.SpotterReliability, err error) {
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFile); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file invalid %s: %w", relPath, loadErr)
		}
		base = rel
	}
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFileCW); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_cw missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_cw invalid %s: %w", relPath, loadErr)
		}
		cw = rel
	}
	if relPath := strings.TrimSpace(cfg.SpotterReliabilityFileRTTY); relPath != "" {
		if _, statErr := os.Stat(relPath); statErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_rtty missing/unreadable %s: %w", relPath, statErr)
		}
		rel, _, loadErr := spot.LoadSpotterReliability(relPath)
		if loadErr != nil {
			return nil, nil, nil, fmt.Errorf("call_correction.spotter_reliability_file_rtty invalid %s: %w", relPath, loadErr)
		}
		rtty = rel
	}
	return base, cw, rtty, nil
}

// loadConfusionModel treats a disabled model as nil so replay can compare
// configs with and without the model through the same runner path.
//
//nolint:nilnil // A nil confusion model with nil error means the optional model is disabled.
func loadConfusionModel(cfg config.CallCorrectionConfig) (*spot.ConfusionModel, error) {
	if !cfg.ConfusionModelEnabled {
		return nil, nil
	}
	modelPath := strings.TrimSpace(cfg.ConfusionModelFile)
	if modelPath == "" {
		return nil, errors.New("call_correction.confusion_model_file is empty while call_correction.confusion_model_enabled=true")
	}
	if _, err := os.Stat(modelPath); err != nil {
		return nil, fmt.Errorf("call_correction.confusion_model_file missing/unreadable %s: %w", modelPath, err)
	}
	loaded, err := spot.LoadConfusionModel(modelPath)
	if err != nil {
		return nil, fmt.Errorf("call_correction.confusion_model_file invalid %s: %w", modelPath, err)
	}
	return loaded, nil
}
