// File role: Rejects lossy flood YAML scalars before decoding and bounds the
// existing controller's arithmetic. These are representation limits, not CPU
// or retained-memory budgets; runtime policy and constructor APIs are unchanged.
package config

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
)

const (
	maxFloodSeconds   = min(int64(math.MaxInt), math.MaxInt64/int64(time.Second))
	maxFloodThreshold = math.MaxInt - 2 // Retain T+1 timestamps, temporarily append T+2.
)

// validateRawFloodControl runs on the final merged YAML before typed decoding.
// yaml.v3 otherwise truncates floats to integers and turns null map values into
// zero, which is an intentional per-source disable sentinel in this policy.
func validateRawFloodControl(raw map[string]any) error {
	for _, path := range [][]string{{"flood_control"}, {"flood_control", "rails"}} {
		if _, err := rawFloodMap(raw, path); err != nil {
			return err
		}
	}
	if err := validateFloodRawIntegerAt(raw, []string{"flood_control", "log_interval_seconds"}, 1, int(maxFloodSeconds)); err != nil {
		return err
	}
	for _, name := range []string{"decall", "source_node", "spotter_ip", "dxcall"} {
		path := []string{"flood_control", "rails", name}
		if _, err := rawFloodMap(raw, path); err != nil {
			return err
		}
		if err := validateFloodRawIntegerAt(raw, append(path, "window_seconds"), 1, int(maxFloodSeconds)); err != nil {
			return err
		}
		if err := validateFloodRawIntegerAt(raw, append(path, "max_entries_per_partition"), 1, math.MaxInt); err != nil {
			return err
		}
		if name != "dxcall" {
			if err := validateRawFloodThresholds(raw, append(path, "thresholds_per_source_type")); err != nil {
				return err
			}
			continue
		}
		path = append(path, "thresholds_by_mode")
		if _, err := rawFloodMap(raw, path); err != nil {
			return err
		}
		// Inactive modes and disabled rails must remain valid for the next restart.
		for _, mode := range []string{floodModeConservative, floodModeModerate, floodModeAggressive} {
			if err := validateRawFloodThresholds(raw, append(path, mode)); err != nil {
				return err
			}
		}
	}
	return nil
}

func rawFloodMap(raw map[string]any, path []string) (map[string]any, error) {
	value, present := yamlValueAt(raw, path...)
	values, ok := value.(map[string]any)
	if !present || !ok || values == nil {
		return nil, fmt.Errorf("invalid YAML setting %q: requires a non-null mapping", strings.Join(path, "."))
	}
	return values, nil
}

func validateFloodRawIntegerAt(raw map[string]any, path []string, minimum, maximum int) error {
	value, _ := yamlValueAt(raw, path...)
	_, err := rawFloodInteger(value, strings.Join(path, "."), minimum, maximum)
	return err
}

func rawFloodInteger(value any, path string, minimum, maximum int) (int, error) {
	n, ok := value.(int)
	if !ok || n < minimum || n > maximum {
		return 0, fmt.Errorf("invalid YAML setting %q: requires an integer between %d and %d", path, minimum, maximum)
	}
	return n, nil
}

func validateRawFloodThresholds(raw map[string]any, path []string) error {
	values, err := rawFloodMap(raw, path)
	if err != nil {
		return err
	}
	joined := strings.Join(path, ".")
	if len(values) != len(floodSupportedSourceTypes) {
		return fmt.Errorf("invalid %s: expected thresholds for %s", joined, strings.Join(floodSupportedSourceTypes, ", "))
	}
	thresholds := make(map[string]int, len(values))
	for _, key := range slices.Sorted(maps.Keys(values)) {
		threshold, err := rawFloodInteger(values[key], joined+"."+key, 0, maxFloodThreshold)
		if err != nil {
			return err
		}
		thresholds[key] = threshold
	}
	// Validate source completeness/collisions without rewriting merged raw keys:
	// normalization is published only by the typed startup normalizer.
	return normalizeFloodThresholdMap(joined, thresholds)
}

// validateFloodActorCardinality protects GetStats' int sum without allocating
// actor state. It does not promise that these theoretical maxima fit in RAM.
func validateFloodActorCardinality(cfg FloodControlConfig) error {
	if !cfg.Enabled {
		return nil
	}
	dxThresholds := cfg.Rails.DXCall.ThresholdsByMode.Conservative
	switch strings.ToLower(strings.TrimSpace(cfg.Rails.DXCall.ActiveMode)) {
	case floodModeModerate:
		dxThresholds = cfg.Rails.DXCall.ThresholdsByMode.Moderate
	case floodModeAggressive:
		dxThresholds = cfg.Rails.DXCall.ThresholdsByMode.Aggressive
	}
	total := 0
	for _, entry := range []struct {
		name       string
		rail       FloodRailConfig
		thresholds map[string]int
	}{
		{"decall", cfg.Rails.DECall, cfg.Rails.DECall.ThresholdsPerSourceType},
		{"source_node", cfg.Rails.SourceNode, cfg.Rails.SourceNode.ThresholdsPerSourceType},
		{"spotter_ip", cfg.Rails.SpotterIP, cfg.Rails.SpotterIP.ThresholdsPerSourceType},
		{"dxcall", cfg.Rails.DXCall, dxThresholds},
	} {
		if !entry.rail.Enabled {
			continue
		}
		active := 0
		for _, threshold := range entry.thresholds {
			if threshold > 0 {
				active++
			}
		}
		if active == 0 {
			continue
		}
		if entry.rail.MaxEntriesPerPartition > (math.MaxInt-total)/active {
			return fmt.Errorf("invalid flood_control.rails.%s.max_entries_per_partition: combined active actor capacity exceeds %d", entry.name, math.MaxInt)
		}
		total += active * entry.rail.MaxEntriesPerPartition
	}
	return nil
}
