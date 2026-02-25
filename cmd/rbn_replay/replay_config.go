package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultReplayConfigPath = "cmd/rbn_replay/replay.yaml"

type replayConfig struct {
	ClusterConfigDir string                `yaml:"cluster_config_dir"`
	ArchiveDir       string                `yaml:"archive_dir"`
	ForceDownload    bool                  `yaml:"force_download"`
	Stability        replayStabilityConfig `yaml:"stability"`
}

func defaultReplayConfig() replayConfig {
	return replayConfig{
		ClusterConfigDir: defaultConfigPath,
		ArchiveDir:       "archive data",
		ForceDownload:    false,
		Stability: replayStabilityConfig{
			WindowMinutes:   60,
			MinFollowOn:     2,
			FreqToleranceHz: 1000,
		},
	}
}

func loadReplayConfig(path string) (replayConfig, error) {
	cfg := defaultReplayConfig()
	path = strings.TrimSpace(path)
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return replayConfig{}, fmt.Errorf("read replay config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return replayConfig{}, fmt.Errorf("parse replay config %s: %w", path, err)
	}
	cfg.ClusterConfigDir = strings.TrimSpace(cfg.ClusterConfigDir)
	if cfg.ClusterConfigDir == "" {
		cfg.ClusterConfigDir = defaultConfigPath
	}
	cfg.ArchiveDir = strings.TrimSpace(cfg.ArchiveDir)
	if cfg.ArchiveDir == "" {
		cfg.ArchiveDir = "archive data"
	}
	cfg.ClusterConfigDir = filepath.Clean(cfg.ClusterConfigDir)
	cfg.ArchiveDir = filepath.Clean(cfg.ArchiveDir)
	cfg.Stability = normalizeReplayStabilityConfig(cfg.Stability)
	return cfg, nil
}
