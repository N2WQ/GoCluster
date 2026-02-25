package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"dxcluster/config"
)

type replayRunRecord struct {
	RunID             string                   `json:"run_id"`
	DateUTC           string                   `json:"date_utc"`
	StartedAtUTC      string                   `json:"started_at_utc"`
	FinishedAtUTC     string                   `json:"finished_at_utc"`
	ArchiveDir        string                   `json:"archive_dir"`
	OutputDir         string                   `json:"output_dir"`
	ConfigLoadedFrom  string                   `json:"config_loaded_from"`
	ReplayConfigPath  string                   `json:"replay_config_path"`
	ReplayConfigSHA   string                   `json:"replay_config_sha256,omitempty"`
	PipelineConfigSHA string                   `json:"pipeline_yaml_sha256,omitempty"`
	CallCorrectionSHA string                   `json:"call_correction_sha256"`
	InputZipPath      string                   `json:"input_zip_path"`
	InputZipSHA       string                   `json:"input_zip_sha256,omitempty"`
	Comparable        uint64                   `json:"comparable_decisions"`
	AgreementPct      float64                  `json:"agreement_pct"`
	DWPct             float64                  `json:"dw_pct"`
	SPPct             float64                  `json:"sp_pct"`
	UCPct             float64                  `json:"uc_pct"`
	Methods           replayMethodStabilitySet `json:"method_stability"`
}

func writeReplayRunHistory(
	archiveDir string,
	replayConfigPath string,
	clusterCfg *config.Config,
	manifest replayManifest,
) error {
	record := replayRunRecord{
		RunID:            buildReplayRunID(manifest),
		DateUTC:          manifest.DateUTC,
		StartedAtUTC:     manifest.StartedAtUTC,
		FinishedAtUTC:    manifest.FinishedAtUTC,
		ArchiveDir:       manifest.ArchiveDir,
		OutputDir:        manifest.OutputDir,
		ConfigLoadedFrom: manifest.ConfigLoadedFrom,
		ReplayConfigPath: strings.TrimSpace(replayConfigPath),
		InputZipPath:     manifest.InputZipPath,
		InputZipSHA:      strings.TrimSpace(manifest.InputZipMeta.SHA256),
		Comparable:       manifest.Results.ComparableDecisions,
		AgreementPct:     manifest.Results.AgreementPct,
		DWPct:            manifest.Results.DWPct,
		SPPct:            manifest.Results.SPPct,
		UCPct:            manifest.Results.UCPct,
		Methods:          manifest.Results.MethodStability,
	}

	if clusterCfg != nil {
		record.CallCorrectionSHA = hashAnyStable(clusterCfg.CallCorrection)
	}
	if record.ReplayConfigPath != "" {
		if hash, err := sha256File(record.ReplayConfigPath); err == nil {
			record.ReplayConfigSHA = hash
		}
	}
	if manifest.ConfigLoadedFrom != "" {
		pipelinePath := filepath.Join(manifest.ConfigLoadedFrom, "pipeline.yaml")
		if hash, err := sha256File(pipelinePath); err == nil {
			record.PipelineConfigSHA = hash
		}
	}
	if record.CallCorrectionSHA == "" {
		record.CallCorrectionSHA = hashAnyStable(struct{}{})
	}

	historyRoot := filepath.Join(strings.TrimSpace(archiveDir), "rbn_replay_history")
	if err := ensureDir(historyRoot); err != nil {
		return err
	}
	runsDir := filepath.Join(historyRoot, "runs")
	if err := ensureDir(runsDir); err != nil {
		return err
	}

	snapshotPath := filepath.Join(runsDir, fmt.Sprintf("%s_%s.json", record.DateUTC, record.RunID))
	if err := writeJSONAtomic(snapshotPath, record); err != nil {
		return fmt.Errorf("write run snapshot %s: %w", snapshotPath, err)
	}

	indexPath := filepath.Join(historyRoot, "runs.jsonl")
	if err := appendJSONL(indexPath, record); err != nil {
		return fmt.Errorf("append run index %s: %w", indexPath, err)
	}
	return nil
}

func buildReplayRunID(manifest replayManifest) string {
	ts := strings.TrimSpace(manifest.FinishedAtUTC)
	ts = strings.ReplaceAll(ts, ":", "")
	ts = strings.ReplaceAll(ts, "-", "")
	ts = strings.ReplaceAll(ts, ".", "")
	return strings.ReplaceAll(ts, "T", "T")
}

func sha256File(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("empty path")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func hashAnyStable(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func appendJSONL(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return err
	}
	return f.Sync()
}
