// File role: owns receive-noise class and band penalty lookup for path reliability.
// Crawler notes: start here when changing SET NOISE penalty semantics, required
// noise_offsets_by_band validation, or default in-memory noise tables.
// Related docs: data/config/path_reliability.yaml,
// docs/decisions/ADR-0124-no-noise-penalty-path-evaluation.md.
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

var defaultNoisePenaltyByClassBand = map[string]map[string]float64{
	"QUIET": {
		"160m": 0, "80m": 0, "60m": 0, "40m": 0, "30m": 0, "20m": 0,
		"17m": 0, "15m": 0, "12m": 0, "10m": 0, "6m": 0,
	},
	"RURAL": {
		"160m": 0, "80m": 0, "60m": 0, "40m": 0, "30m": 0, "20m": 0,
		"17m": 0, "15m": 0, "12m": 0, "10m": 0, "6m": 0,
	},
	"SUBURBAN": {
		"160m": 0, "80m": 0, "60m": 0, "40m": 0, "30m": 0, "20m": 0,
		"17m": 0, "15m": 0, "12m": 0, "10m": 0, "6m": 0,
	},
	"URBAN": {
		"160m": 0, "80m": 0, "60m": 0, "40m": 0, "30m": 0, "20m": 0,
		"17m": 0, "15m": 0, "12m": 0, "10m": 0, "6m": 0,
	},
	"INDUSTRIAL": {
		"160m": 0, "80m": 0, "60m": 0, "40m": 0, "30m": 0, "20m": 0,
		"17m": 0, "15m": 0, "12m": 0, "10m": 0, "6m": 0,
	},
}

// NoiseModel is the immutable startup-built receive-side penalty lookup.
// It is bounded by the configured noise classes and band keys; callers must not
// mutate the source table after Config normalization.
type NoiseModel struct {
	penalties map[string]map[string]float64
	classes   map[string]struct{}
}

func defaultNoiseOffsetsByBand() map[string]map[string]float64 {
	return cloneNoiseOffsetsByBand(defaultNoisePenaltyByClassBand)
}

func cloneNoiseOffsetsByBand(in map[string]map[string]float64) map[string]map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[string]float64, len(in))
	for class, bands := range in {
		classKey := strutil.NormalizeUpper(class)
		if classKey == "" {
			continue
		}
		copied := make(map[string]float64, len(bands))
		for band, penalty := range bands {
			bandKey := normalizeBand(band)
			if bandKey == "" {
				continue
			}
			copied[bandKey] = clampNoisePenalty(penalty)
		}
		out[classKey] = copied
	}
	return out
}

func normalizeNoiseOffsetsByBand(in map[string]map[string]float64, defaults map[string]map[string]float64) map[string]map[string]float64 {
	out := cloneNoiseOffsetsByBand(defaults)
	if out == nil {
		out = map[string]map[string]float64{}
	}
	for class, bands := range in {
		classKey := strutil.NormalizeUpper(class)
		if classKey == "" {
			continue
		}
		target := out[classKey]
		if target == nil {
			target = map[string]float64{}
			out[classKey] = target
		}
		for band, penalty := range bands {
			bandKey := normalizeBand(band)
			if bandKey == "" {
				continue
			}
			target[bandKey] = clampNoisePenalty(penalty)
		}
	}
	return out
}

func clampNoisePenalty(penalty float64) float64 {
	if penalty < 0 {
		return 0
	}
	return penalty
}

func newNoiseModel(table map[string]map[string]float64) NoiseModel {
	normalized := cloneNoiseOffsetsByBand(table)
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

// Penalty returns the configured receive-side penalty for class and band.
func (m NoiseModel) Penalty(class string, band string) float64 {
	classKey := strutil.NormalizeUpper(class)
	if classKey == "" {
		return 0
	}
	bandKey := normalizeBand(band)
	if bandKey == "" {
		return 0
	}
	byBand := m.penalties[classKey]
	if byBand == nil {
		return 0
	}
	return byBand[bandKey]
}

func (m NoiseModel) uniquePenaltyCount() int {
	seen := map[float64]struct{}{}
	m.visitPenalties(func(penalty float64) {
		seen[penalty] = struct{}{}
	})
	return len(seen)
}

func (m NoiseModel) visitPenalties(fn func(float64)) {
	if fn == nil {
		return
	}
	for _, byBand := range m.penalties {
		for _, penalty := range byBand {
			fn(penalty)
		}
	}
}

func decodeNoiseOffsetsByBand(bs []byte) (map[string]map[string]float64, bool, error) {
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
		case "noise_offsets":
			return nil, false, fmt.Errorf("noise_offsets is no longer supported; use noise_offsets_by_band")
		case "noise_offsets_by_band":
			noiseNode = doc.Content[i+1]
		}
	}
	if noiseNode == nil {
		return nil, false, nil
	}
	if noiseNode.Kind != yaml.MappingNode {
		return nil, true, fmt.Errorf("noise_offsets_by_band must be a mapping of class to band penalties")
	}
	var table map[string]map[string]float64
	if err := noiseNode.Decode(&table); err != nil {
		return nil, true, fmt.Errorf("noise_offsets_by_band: %w", err)
	}
	if err := validateProvidedNoiseOffsetsByBand(table); err != nil {
		return nil, true, err
	}
	return table, true, nil
}

func validateProvidedNoiseOffsetsByBand(table map[string]map[string]float64) error {
	if len(table) == 0 {
		return fmt.Errorf("noise_offsets_by_band must define QUIET, RURAL, SUBURBAN, URBAN, and INDUSTRIAL")
	}
	seen := make(map[string]struct{}, len(table))
	for class, bands := range table {
		classKey := strutil.NormalizeUpper(class)
		if !isCanonicalNoiseClass(classKey) {
			return fmt.Errorf("unsupported noise class %q in noise_offsets_by_band", class)
		}
		if _, exists := seen[classKey]; exists {
			return fmt.Errorf("duplicate noise class %q in noise_offsets_by_band", classKey)
		}
		if bands == nil {
			return fmt.Errorf("noise_offsets_by_band.%s must be a mapping of band to penalty", class)
		}
		seen[classKey] = struct{}{}
	}
	for _, class := range canonicalNoiseClasses {
		if _, ok := seen[class]; !ok {
			return fmt.Errorf("noise_offsets_by_band missing required class %s", class)
		}
	}
	return nil
}

func validateNoiseOffsetsByBandForBands(table map[string]map[string]float64, allowedBands []string) error {
	if err := validateProvidedNoiseOffsetsByBand(table); err != nil {
		return err
	}
	requiredBands := make(map[string]struct{}, len(allowedBands))
	for _, band := range allowedBands {
		key := normalizeBand(band)
		if key == "" {
			return fmt.Errorf("allowed_bands contains empty band")
		}
		requiredBands[key] = struct{}{}
	}
	if len(requiredBands) == 0 {
		return fmt.Errorf("allowed_bands must not be empty")
	}
	for class, bands := range table {
		classKey := strutil.NormalizeUpper(class)
		seenBands := make(map[string]struct{}, len(bands))
		for band, penalty := range bands {
			bandKey := normalizeBand(band)
			if bandKey == "" {
				return fmt.Errorf("noise_offsets_by_band.%s contains empty band", classKey)
			}
			if _, ok := requiredBands[bandKey]; !ok {
				return fmt.Errorf("noise_offsets_by_band.%s contains unsupported band %s", classKey, band)
			}
			if penalty < 0 {
				return fmt.Errorf("noise_offsets_by_band.%s.%s must be >= 0", classKey, bandKey)
			}
			seenBands[bandKey] = struct{}{}
		}
		for band := range requiredBands {
			if _, ok := seenBands[band]; !ok {
				return fmt.Errorf("noise_offsets_by_band.%s missing required band %s", classKey, band)
			}
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
