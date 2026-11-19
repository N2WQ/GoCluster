// File role: Owns optional Bayes mode/distance selection and its presence
// contract. Configuration is validated before owners start; immutable loaded
// masks contain at most four canonical mode keys and two distances per key.
package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

const bayesBonusMaskPath = "call_correction.bayes_bonus.enabled_distances_by_mode"

// Keep the existing correction profiles, including voice, available to omitted
// masks. The archive study separately limits its evidence claims to CW/RTTY.
var bayesBonusModes = [...]string{"CW", "LSB", "RTTY", "USB"}

// BayesBonusCellEnabled is the shared structural eligibility contract. Nil
// preserves legacy cells; nonnil empty masks and nil/empty mode slices select
// none. Scores and weights do not remove an eligible history consumer.
// Callers validate configuration before starting owners and do not mutate masks
// afterward. Direct-Go keys may use the same case/space normalization as YAML.
func BayesBonusCellEnabled(enabled bool, enabledDistancesByMode map[string][]int, mode string, distance, maxDistance int) bool {
	if !enabled || distance < 1 || distance > 2 || (maxDistance >= 0 && distance > maxDistance) {
		return false
	}
	modeIndex := bayesBonusModeIndex(mode)
	if modeIndex < 0 {
		return false
	}
	if enabledDistancesByMode == nil {
		return true
	}
	if len(enabledDistancesByMode) > len(bayesBonusModes) {
		return false
	}
	var selected []int
	matches := 0
	for key, distances := range enabledDistancesByMode {
		if bayesBonusModeIndex(key) == modeIndex {
			selected = distances
			matches++
		}
	}
	if matches != 1 {
		return false
	}
	for _, selectedDistance := range selected {
		if selectedDistance == distance {
			return true
		}
	}
	return false
}

// BayesBonusModeEnabled is the diagnostic counterpart of the same cell check.
// It does not infer eligibility from the master flag or score reachability.
func BayesBonusModeEnabled(enabled bool, enabledDistancesByMode map[string][]int, mode string, maxDistance int) bool {
	return BayesBonusCellEnabled(enabled, enabledDistancesByMode, mode, 1, maxDistance) ||
		BayesBonusCellEnabled(enabled, enabledDistancesByMode, mode, 2, maxDistance)
}

func bayesBonusModeIndex(mode string) int {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	for i, supported := range bayesBonusModes {
		if mode == supported {
			return i
		}
	}
	return -1
}

// ValidateEnabledDistancesByMode also serves callers that bypass YAML loading.
// A nil slice is explicit no-distances in Go; YAML null is rejected separately
// while the raw document still distinguishes it from an empty sequence.
func (c CallCorrectionBayesBonusConfig) ValidateEnabledDistancesByMode() error {
	if len(c.EnabledDistancesByMode) > len(bayesBonusModes) {
		return fmt.Errorf("%s: accepts at most CW, RTTY, USB and LSB once each", bayesBonusMaskPath)
	}
	seenModes := [len(bayesBonusModes)]bool{}
	for mode, distances := range c.EnabledDistancesByMode {
		index := bayesBonusModeIndex(mode)
		if index < 0 {
			return fmt.Errorf("%s: unsupported mode %q (allowed: CW, RTTY, USB, LSB)", bayesBonusMaskPath, mode)
		}
		if seenModes[index] {
			return fmt.Errorf("%s: duplicate normalized mode %q", bayesBonusMaskPath, bayesBonusModes[index])
		}
		seenModes[index] = true
		seenDistances := [3]bool{}
		for _, distance := range distances {
			if distance < 1 || distance > 2 {
				return fmt.Errorf("%s.%s: unsupported distance %d (allowed: 1, 2)", bayesBonusMaskPath, mode, distance)
			}
			if seenDistances[distance] {
				return fmt.Errorf("%s.%s: duplicate distance %d", bayesBonusMaskPath, mode, distance)
			}
			seenDistances[distance] = true
		}
	}
	return nil
}

func normalizeCallCorrectionBayesMaskConfig(cfg *Config) error {
	bonus := &cfg.CallCorrection.BayesBonus
	if err := bonus.ValidateEnabledDistancesByMode(); err != nil {
		return err
	}
	if bonus.EnabledDistancesByMode == nil {
		return nil
	}
	// Rebuild only at load time. Canonical keys and exact-sized slices avoid
	// retaining arbitrary input spellings or caller backing arrays in the mask.
	normalized := make(map[string][]int, len(bonus.EnabledDistancesByMode))
	for mode, distances := range bonus.EnabledDistancesByMode {
		selected := make([]int, len(distances))
		copy(selected, distances)
		if len(selected) == 2 && selected[0] > selected[1] {
			selected[0], selected[1] = selected[1], selected[0]
		}
		normalized[bayesBonusModes[bayesBonusModeIndex(mode)]] = selected
	}
	bonus.EnabledDistancesByMode = normalized
	return nil
}

func (c CallCorrectionConfig) bayesBonusDistanceEnabled(distance int) bool {
	for _, mode := range bayesBonusModes {
		if BayesBonusCellEnabled(c.BayesBonus.Enabled, c.BayesBonus.EnabledDistancesByMode, mode, distance, c.MaxEditDistance) {
			return true
		}
	}
	return false
}

func (c CallCorrectionConfig) bayesBonusEnabledCells() []string {
	var cells []string
	for _, mode := range bayesBonusModes {
		for distance := 1; distance <= 2; distance++ {
			if BayesBonusCellEnabled(c.BayesBonus.Enabled, c.BayesBonus.EnabledDistancesByMode, mode, distance, c.MaxEditDistance) {
				cells = append(cells, fmt.Sprintf("%s/%d", mode, distance))
			}
		}
	}
	return cells
}

// MarshalYAML preserves legacy omission and explicit emptiness when a typed
// config is round-tripped. A plain map tag encodes nil as {}; omitempty instead
// drops explicit {}. Keeping the normal struct tag preserves decoder field and
// unknown-key diagnostics; no custom unmarshaller bypasses that boundary.
func (c CallCorrectionBayesBonusConfig) MarshalYAML() (any, error) {
	type plain CallCorrectionBayesBonusConfig
	var node yaml.Node
	if err := node.Encode(plain(c)); err != nil {
		return nil, err
	}
	if c.EnabledDistancesByMode == nil {
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == "enabled_distances_by_mode" {
				node.Content = append(node.Content[:i], node.Content[i+2:]...)
				break
			}
		}
	}
	return &node, nil
}

// This runs before raw maps are marshaled into the typed config. Once decoded
// into map[string][]int, omitted values and YAML null cannot be distinguished.
func bayesBonusMaskPresenceDiagnostics(raw map[string]any) []string {
	value, present := yamlValueAt(raw, "call_correction", "bayes_bonus", "enabled_distances_by_mode")
	if !present {
		return nil
	}
	invalid := func(detail string) []string {
		return []string{fmt.Sprintf("invalid YAML setting %q: %s", bayesBonusMaskPath, detail)}
	}
	if value == nil {
		return invalid("must not be null; omit for legacy cells or use {} for none")
	}
	mapping, ok := value.(map[string]any)
	if !ok {
		return invalid("must be a mode-to-distance-list mapping")
	}
	if len(mapping) > len(bayesBonusModes) {
		return invalid("accepts at most CW, RTTY, USB and LSB once each")
	}
	typed := CallCorrectionBayesBonusConfig{EnabledDistancesByMode: make(map[string][]int, len(mapping))}
	for mode, value := range mapping {
		if value == nil {
			return invalid(fmt.Sprintf("mode %q must not be null; use [] for none", mode))
		}
		sequence, ok := value.([]any)
		if !ok {
			return invalid(fmt.Sprintf("mode %q must contain a distance list", mode))
		}
		if len(sequence) > 2 {
			return invalid(fmt.Sprintf("mode %q accepts only unique distances 1 and 2", mode))
		}
		distances := make([]int, len(sequence))
		for i, value := range sequence {
			distance, ok := value.(int)
			if !ok {
				return invalid(fmt.Sprintf("mode %q distances must be integers 1 or 2", mode))
			}
			distances[i] = distance
		}
		typed.EnabledDistancesByMode[mode] = distances
	}
	if err := typed.ValidateEnabledDistancesByMode(); err != nil {
		return []string{err.Error()}
	}
	return nil
}
