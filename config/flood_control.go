package config

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	FloodControlPartitionExactSourceType = "exact_source_type"

	FloodActionObserve  = "observe"
	FloodActionSuppress = "suppress"
	FloodActionDrop     = "drop"

	floodModeConservative = "conservative"
	floodModeModerate     = "moderate"
	floodModeAggressive   = "aggressive"
)

var floodSupportedSourceTypes = []string{
	"MANUAL",
	"UPSTREAM",
	"PEER",
	"RBN",
	"FT8",
	"FT4",
	"PSKREPORTER",
}

// FloodControlConfig controls shared-ingest flood suppression before primary dedupe.
type FloodControlConfig struct {
	Enabled            bool                    `yaml:"enabled"`
	LogIntervalSeconds int                     `yaml:"log_interval_seconds"`
	PartitionMode      string                  `yaml:"partition_mode"`
	Rails              FloodControlRailsConfig `yaml:"rails"`
}

// FloodControlRailsConfig groups the supported flood rails.
type FloodControlRailsConfig struct {
	DECall     FloodRailConfig `yaml:"decall"`
	SourceNode FloodRailConfig `yaml:"source_node"`
	SpotterIP  FloodRailConfig `yaml:"spotter_ip"`
	DXCall     FloodRailConfig `yaml:"dxcall"`
}

func (r FloodControlRailsConfig) summary() string {
	parts := []string{
		r.DECall.summary("decall"),
		r.SourceNode.summary("source_node"),
		r.SpotterIP.summary("spotter_ip"),
		r.DXCall.summary("dxcall"),
	}
	return strings.Join(parts, ", ")
}

// FloodRailConfig defines one actor-based flood rail.
type FloodRailConfig struct {
	Enabled                 bool                      `yaml:"enabled"`
	Action                  string                    `yaml:"action"`
	WindowSeconds           int                       `yaml:"window_seconds"`
	MaxEntriesPerPartition  int                       `yaml:"max_entries_per_partition"`
	ThresholdsPerSourceType map[string]int            `yaml:"thresholds_per_source_type"`
	ActiveMode              string                    `yaml:"active_mode"`
	ThresholdsByMode        FloodRailThresholdsByMode `yaml:"thresholds_by_mode"`
}

func (c FloodRailConfig) summary(name string) string {
	action := strings.ToLower(strings.TrimSpace(c.Action))
	if action == "" {
		action = "invalid"
	}
	if !c.Enabled {
		return fmt.Sprintf("%s=off/%s", name, action)
	}
	if name == "dxcall" {
		return fmt.Sprintf("%s=%s/%s", name, action, strings.ToLower(strings.TrimSpace(c.ActiveMode)))
	}
	return fmt.Sprintf("%s=%s", name, action)
}

// FloodRailThresholdsByMode defines the DXCALL mode tables.
type FloodRailThresholdsByMode struct {
	Conservative map[string]int `yaml:"conservative"`
	Moderate     map[string]int `yaml:"moderate"`
	Aggressive   map[string]int `yaml:"aggressive"`
}

func normalizeFloodControlConfig(cfg *Config, raw map[string]any) error {
	if cfg == nil {
		return fmt.Errorf("nil config")
	}
	if !yamlKeyPresent(raw, "flood_control") {
		return fmt.Errorf("missing required config block flood_control (expected in floodcontrol.yaml)")
	}
	requiredTopLevel := []string{"enabled", "log_interval_seconds", "partition_mode", "rails"}
	for _, key := range requiredTopLevel {
		if !yamlKeyPresent(raw, "flood_control", key) {
			return fmt.Errorf("missing required flood_control.%s", key)
		}
	}

	cfg.FloodControl.PartitionMode = strings.ToLower(strings.TrimSpace(cfg.FloodControl.PartitionMode))
	switch cfg.FloodControl.PartitionMode {
	case FloodControlPartitionExactSourceType:
	default:
		return fmt.Errorf("invalid flood_control.partition_mode %q (must be %s)", cfg.FloodControl.PartitionMode, FloodControlPartitionExactSourceType)
	}
	if cfg.FloodControl.LogIntervalSeconds <= 0 {
		return fmt.Errorf("invalid flood_control.log_interval_seconds %d (must be > 0)", cfg.FloodControl.LogIntervalSeconds)
	}

	if err := validateSimpleFloodRail(raw, "decall", cfg.FloodControl.Rails.DECall); err != nil {
		return err
	}
	if err := validateSimpleFloodRail(raw, "source_node", cfg.FloodControl.Rails.SourceNode); err != nil {
		return err
	}
	if err := validateSimpleFloodRail(raw, "spotter_ip", cfg.FloodControl.Rails.SpotterIP); err != nil {
		return err
	}
	if err := validateDXCallFloodRail(raw, cfg.FloodControl.Rails.DXCall); err != nil {
		return err
	}
	return validateFloodActorCardinality(cfg.FloodControl)
}

func validateSimpleFloodRail(raw map[string]any, name string, rail FloodRailConfig) error {
	path := []string{"flood_control", "rails", name}
	if err := validateFloodRailCommon(raw, path, rail); err != nil {
		return err
	}
	if !yamlKeyPresent(raw, append(path, "thresholds_per_source_type")...) {
		return fmt.Errorf("missing required %s.thresholds_per_source_type", strings.Join(path, "."))
	}
	if err := normalizeFloodThresholdMap(strings.Join(append(path, "thresholds_per_source_type"), "."), rail.ThresholdsPerSourceType); err != nil {
		return err
	}
	return nil
}

func validateDXCallFloodRail(raw map[string]any, rail FloodRailConfig) error {
	path := []string{"flood_control", "rails", "dxcall"}
	if err := validateFloodRailCommon(raw, path, rail); err != nil {
		return err
	}
	if !yamlKeyPresent(raw, append(path, "active_mode")...) {
		return fmt.Errorf("missing required %s.active_mode", strings.Join(path, "."))
	}
	if !yamlKeyPresent(raw, append(path, "thresholds_by_mode")...) {
		return fmt.Errorf("missing required %s.thresholds_by_mode", strings.Join(path, "."))
	}
	rail.ActiveMode = strings.ToLower(strings.TrimSpace(rail.ActiveMode))
	switch rail.ActiveMode {
	case floodModeConservative, floodModeModerate, floodModeAggressive:
	default:
		return fmt.Errorf("invalid %s.active_mode %q (must be conservative, moderate, or aggressive)", strings.Join(path, "."), rail.ActiveMode)
	}
	if err := normalizeFloodThresholdMap(strings.Join(append(path, "thresholds_by_mode", floodModeConservative), "."), rail.ThresholdsByMode.Conservative); err != nil {
		return err
	}
	if err := normalizeFloodThresholdMap(strings.Join(append(path, "thresholds_by_mode", floodModeModerate), "."), rail.ThresholdsByMode.Moderate); err != nil {
		return err
	}
	if err := normalizeFloodThresholdMap(strings.Join(append(path, "thresholds_by_mode", floodModeAggressive), "."), rail.ThresholdsByMode.Aggressive); err != nil {
		return err
	}
	return nil
}

func validateFloodRailCommon(raw map[string]any, path []string, rail FloodRailConfig) error {
	requiredKeys := []string{"enabled", "action", "window_seconds", "max_entries_per_partition"}
	for _, key := range requiredKeys {
		if !yamlKeyPresent(raw, append(path, key)...) {
			return fmt.Errorf("missing required %s.%s", strings.Join(path, "."), key)
		}
	}
	action := strings.ToLower(strings.TrimSpace(rail.Action))
	switch action {
	case FloodActionObserve, FloodActionSuppress, FloodActionDrop:
	default:
		return fmt.Errorf("invalid %s.action %q (must be observe, suppress, or drop)", strings.Join(path, "."), rail.Action)
	}
	if rail.WindowSeconds <= 0 {
		return fmt.Errorf("invalid %s.window_seconds %d (must be > 0)", strings.Join(path, "."), rail.WindowSeconds)
	}
	if rail.MaxEntriesPerPartition <= 0 {
		return fmt.Errorf("invalid %s.max_entries_per_partition %d (must be > 0)", strings.Join(path, "."), rail.MaxEntriesPerPartition)
	}
	return nil
}

// normalizeFloodThresholdMap publishes the same canonical source names that
// validation accepts. Check the entire map before mutation so collisions cannot
// silently overwrite one another; ordinary exact-key file merges happen earlier.
func normalizeFloodThresholdMap(path string, values map[string]int) error {
	if len(values) != len(floodSupportedSourceTypes) {
		return fmt.Errorf("invalid %s: expected thresholds for %s", path, strings.Join(floodSupportedSourceTypes, ", "))
	}
	canonical := make(map[string]int, len(values))
	for _, rawKey := range slices.Sorted(maps.Keys(values)) {
		threshold := values[rawKey]
		key := strings.ToUpper(strings.TrimSpace(rawKey))
		if key == "" {
			return fmt.Errorf("invalid %s: blank source type key", path)
		}
		if !floodSupportedSourceType(key) {
			return fmt.Errorf("invalid %s.%s: unsupported source type", path, rawKey)
		}
		if _, exists := canonical[key]; exists {
			return fmt.Errorf("invalid %s.%s: duplicate source type", path, rawKey)
		}
		canonical[key] = threshold
		if threshold < 0 || threshold > maxFloodThreshold {
			return fmt.Errorf("invalid %s.%s %d (must be between 0 and %d)", path, key, threshold, maxFloodThreshold)
		}
	}
	for _, sourceType := range floodSupportedSourceTypes {
		if _, ok := canonical[sourceType]; !ok {
			return fmt.Errorf("missing required %s.%s", path, sourceType)
		}
	}
	clear(values)
	maps.Copy(values, canonical)
	return nil
}

func floodSupportedSourceType(candidate string) bool {
	for _, sourceType := range floodSupportedSourceTypes {
		if candidate == sourceType {
			return true
		}
	}
	return false
}
