// File role: owns receive-noise class penalty lookup for path reliability.
// Crawler notes: start here when changing SET NOISE penalty semantics, required
// noise_offsets validation, or default in-memory noise tables.
// Related docs: data/config/path_reliability.yaml,
// docs/decisions/ADR-0127-location-specific-path-noise-penalties.md.
// Related tests: pathreliability/noise_test.go,
// pathreliability/config_test.go, telnet/path_settings_test.go.
package pathreliability

import (
	"fmt"
	"strings"

	"dxcluster/strutil"

	"gopkg.in/yaml.v3"
)

var canonicalNoiseClasses = []string{"QUIET", "RURAL", "SUBURBAN", "URBAN", "INDUSTRIAL"}

var defaultNoisePenaltyByClass = map[string]float64{
	"QUIET":      0,
	"RURAL":      4,
	"SUBURBAN":   12,
	"URBAN":      17,
	"INDUSTRIAL": 20,
}

// NoiseModel is the immutable startup-built receive-side penalty lookup.
// It is bounded by the configured noise classes; callers must not mutate the
// source table after Config normalization.
type NoiseModel struct {
	penalties map[string]float64
	classes   map[string]struct{}
}

func defaultNoiseOffsets() map[string]float64 {
	return cloneNoiseOffsets(defaultNoisePenaltyByClass)
}

func cloneNoiseOffsets(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for class, penalty := range in {
		classKey := strutil.NormalizeUpper(class)
		if classKey == "" {
			continue
		}
		out[classKey] = penalty
	}
	return out
}

func normalizeNoiseOffsets(in map[string]float64, defaults map[string]float64) map[string]float64 {
	out := cloneNoiseOffsets(defaults)
	if out == nil {
		out = map[string]float64{}
	}
	for class, penalty := range in {
		classKey := strutil.NormalizeUpper(class)
		if classKey == "" {
			continue
		}
		out[classKey] = penalty
	}
	return out
}

func newNoiseModel(table map[string]float64) NoiseModel {
	normalized := cloneNoiseOffsets(table)
	classes := make(map[string]struct{}, len(normalized))
	for class := range normalized {
		classes[class] = struct{}{}
	}
	return NoiseModel{
		penalties: normalized,
		classes:   classes,
	}
}

func (m NoiseModel) empty() bool {
	return len(m.classes) == 0 && len(m.penalties) == 0
}

// Empty reports whether the model has no configured classes or penalties.
func (m NoiseModel) Empty() bool {
	return m.empty()
}

// HasClass reports whether class is a configured noise class.
func (m NoiseModel) HasClass(class string) bool {
	key := strutil.NormalizeUpper(class)
	if key == "" {
		return false
	}
	_, ok := m.classes[key]
	return ok
}

// Penalty returns the configured receive-side penalty for class.
func (m NoiseModel) Penalty(class string) float64 {
	classKey := strutil.NormalizeUpper(class)
	if classKey == "" {
		return 0
	}
	return m.penalties[classKey]
}

func decodeNoiseOffsets(bs []byte) (map[string]float64, bool, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(bs, &root); err != nil {
		return nil, false, err
	}
	if len(root.Content) == 0 {
		return nil, false, nil
	}
	doc := root.Content[0]
	if doc.Kind == yaml.ScalarNode && doc.Tag == "!!null" {
		return nil, false, nil
	}
	if doc.Kind != yaml.MappingNode {
		return nil, false, fmt.Errorf("path reliability config must be a mapping")
	}
	var noiseNode *yaml.Node
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key := strings.ToLower(strings.TrimSpace(doc.Content[i].Value))
		switch key {
		case "noise_offsets_by_band":
			return nil, false, fmt.Errorf("noise_offsets_by_band is no longer supported; use noise_offsets")
		case "noise_offsets":
			noiseNode = doc.Content[i+1]
		}
	}
	if noiseNode == nil {
		return nil, false, nil
	}
	if noiseNode.Kind != yaml.MappingNode {
		return nil, true, fmt.Errorf("noise_offsets must be a mapping of class to penalty")
	}
	if err := validateNoiseOffsetNode(noiseNode); err != nil {
		return nil, true, err
	}
	var table map[string]float64
	if err := noiseNode.Decode(&table); err != nil {
		return nil, true, fmt.Errorf("noise_offsets: %w", err)
	}
	if err := validateNoiseOffsets(table); err != nil {
		return nil, true, err
	}
	return table, true, nil
}

func validateNoiseOffsetNode(noiseNode *yaml.Node) error {
	seen := make(map[string]struct{}, len(noiseNode.Content)/2)
	for i := 0; i+1 < len(noiseNode.Content); i += 2 {
		class := noiseNode.Content[i].Value
		classKey := strutil.NormalizeUpper(class)
		if !isCanonicalNoiseClass(classKey) {
			return fmt.Errorf("unsupported noise class %q in noise_offsets", class)
		}
		if _, exists := seen[classKey]; exists {
			return fmt.Errorf("duplicate noise class %q in noise_offsets", classKey)
		}
		if noiseNode.Content[i+1].Kind != yaml.ScalarNode {
			return fmt.Errorf("noise_offsets.%s must be a scalar penalty", class)
		}
		seen[classKey] = struct{}{}
	}
	return nil
}

func validateNoiseOffsets(table map[string]float64) error {
	if len(table) == 0 {
		return fmt.Errorf("noise_offsets must define QUIET, RURAL, SUBURBAN, URBAN, and INDUSTRIAL")
	}
	seen := make(map[string]struct{}, len(table))
	for class, penalty := range table {
		classKey := strutil.NormalizeUpper(class)
		if !isCanonicalNoiseClass(classKey) {
			return fmt.Errorf("unsupported noise class %q in noise_offsets", class)
		}
		if penalty < 0 {
			return fmt.Errorf("noise_offsets.%s must be >= 0", classKey)
		}
		seen[classKey] = struct{}{}
	}
	for _, class := range canonicalNoiseClasses {
		if _, ok := seen[class]; !ok {
			return fmt.Errorf("noise_offsets missing required class %s", class)
		}
	}
	return nil
}

func isCanonicalNoiseClass(class string) bool {
	for _, canonical := range canonicalNoiseClasses {
		if class == canonical {
			return true
		}
	}
	return false
}
