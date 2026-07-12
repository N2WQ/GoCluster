package main

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/download"
)

func TestResolveReplayConfigDirPrecedence(t *testing.T) {
	t.Parallel()

	replayCfg := replayConfig{ClusterConfigDir: "from-replay"}
	tests := []struct {
		name     string
		override string
		envValue string
		want     string
	}{
		{name: "flag override wins", override: "from-flag", envValue: "from-env", want: "from-flag"},
		{name: "replay config wins over env", envValue: "from-env", want: "from-replay"},
		{name: "env fallback when replay config empty", envValue: "from-env", want: "from-env", override: ""},
		{name: "default fallback", want: defaultConfigPath, override: "", envValue: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := replayCfg
			if tc.name == "env fallback when replay config empty" || tc.name == "default fallback" {
				cfg.ClusterConfigDir = ""
			}
			got := resolveReplayConfigDir(tc.override, cfg, tc.envValue)
			if got != tc.want {
				t.Fatalf("resolveReplayConfigDir() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveReplayArchiveDirPrecedenceAndCleaning(t *testing.T) {
	t.Parallel()

	replayCfg := replayConfig{ArchiveDir: " " + filepath.Join("archive data", "day", "..") + " "}

	got := resolveReplayArchiveDir(" "+filepath.Join("custom-archive", "day", "..")+" ", replayCfg)
	want := filepath.Clean("custom-archive")
	if got != want {
		t.Fatalf("resolveReplayArchiveDir() override = %q, want %q", got, want)
	}

	got = resolveReplayArchiveDir("", replayCfg)
	want = filepath.Clean("archive data")
	if got != want {
		t.Fatalf("resolveReplayArchiveDir() replay config = %q, want %q", got, want)
	}
}

func TestBuildReplayManifestPreservesArtifactPaths(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.March, 7, 12, 34, 56, 789, time.UTC)
	dayStart := time.Date(2026, time.March, 7, 0, 0, 0, 0, time.UTC)
	cfg := &config.Config{LoadedFrom: filepath.Clean("data/config")}
	archiveDir := filepath.Clean("archive data")
	outDir := buildReplayOutputDir(archiveDir, dayStart)
	zipPath := buildReplayZipPath(archiveDir, "20260307")
	runbookPath := filepath.Join(outDir, "runbook_samples.log")
	zipMeta := download.Metadata{SHA256: "abc123"}

	manifest := buildReplayManifest(startedAt, dayStart, cfg, archiveDir, outDir, zipPath, zipMeta, runbookPath)

	if manifest.DateUTC != "2026-03-07" {
		t.Fatalf("expected date_utc 2026-03-07, got %q", manifest.DateUTC)
	}
	if manifest.ConfigLoadedFrom != cfg.LoadedFrom {
		t.Fatalf("expected config_loaded_from %q, got %q", cfg.LoadedFrom, manifest.ConfigLoadedFrom)
	}
	if manifest.ArchiveDir != archiveDir {
		t.Fatalf("expected archive_dir %q, got %q", archiveDir, manifest.ArchiveDir)
	}
	if manifest.OutputDir != outDir {
		t.Fatalf("expected output_dir %q, got %q", outDir, manifest.OutputDir)
	}
	if manifest.InputZipPath != zipPath {
		t.Fatalf("expected input_zip_path %q, got %q", zipPath, manifest.InputZipPath)
	}
	if manifest.InputZipMeta.SHA256 != zipMeta.SHA256 {
		t.Fatalf("expected input zip metadata sha %q, got %q", zipMeta.SHA256, manifest.InputZipMeta.SHA256)
	}
	if manifest.StartedAtUTC != startedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected started_at_utc %q, got %q", startedAt.Format(time.RFC3339Nano), manifest.StartedAtUTC)
	}
	if manifest.GoVersion != runtime.Version() {
		t.Fatalf("expected go version %q, got %q", runtime.Version(), manifest.GoVersion)
	}
	if manifest.Outputs.RunbookSamplesLog != runbookPath {
		t.Fatalf("expected runbook_samples_log %q, got %q", runbookPath, manifest.Outputs.RunbookSamplesLog)
	}
	if manifest.Outputs.ManifestJSON != filepath.Join(outDir, "manifest.json") {
		t.Fatalf("expected manifest path under %q, got %q", outDir, manifest.Outputs.ManifestJSON)
	}
	if manifest.Outputs.IntervalsCSV != filepath.Join(outDir, "resolver_intervals.csv") {
		t.Fatalf("expected intervals path under %q, got %q", outDir, manifest.Outputs.IntervalsCSV)
	}
	if manifest.Outputs.ThresholdHitsCSV != filepath.Join(outDir, "resolver_threshold_hits.csv") {
		t.Fatalf("expected threshold hits path under %q, got %q", outDir, manifest.Outputs.ThresholdHitsCSV)
	}
	if manifest.Outputs.GatesJSON != filepath.Join(outDir, "gates.json") {
		t.Fatalf("expected gates path under %q, got %q", outDir, manifest.Outputs.GatesJSON)
	}
}
