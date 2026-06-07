// File role: Loads and applies the startup-owned IARU regional mode inference
// reference table.
// Crawler notes: Start here for regional frequency policy, unknown-region CW
// safety, and reference-table startup diagnostics.
// Related docs: data/config/README.md, docs/decisions/ADR-0153-startup-config-diagnostics-and-gridstore-logging.md.
// Related tests: spot/mode_region_test.go.
package spot

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"dxcluster/internal/yamlconfig"

	"gopkg.in/yaml.v3"
)

type regionModeClass string

const (
	regionModeClassUnknown      regionModeClass = ""
	regionModeClassCWSafe       regionModeClass = "cw_safe"
	regionModeClassVoiceDefault regionModeClass = "voice_default"
	regionModeClassMixed        regionModeClass = "mixed"
)

type regionModeSegment struct {
	FromKHz float64 `yaml:"from_khz"`
	ToKHz   float64 `yaml:"to_khz"`
	Class   string  `yaml:"class"`
}

type regionModeTable struct {
	VoiceModeByBand     map[string]string                         `yaml:"voice_mode_by_band"`
	UnknownRegionCWSafe map[string][]regionModeSegment            `yaml:"unknown_region_cw_safe"`
	Regions             map[string]map[string][]regionModeSegment `yaml:"regions"`
}

type RegionalModeResult struct {
	Mode       string
	Provenance ModeProvenance
}

var (
	regionModeOnce sync.Once
	regionModes    regionModeTable
	regionModesSet bool
)

const regionModePath = "data/config/iaru_mode_inference.yaml"

func loadRegionModes() {
	regionModeOnce.Do(func() {
		if regionModesSet {
			return
		}
		paths := []string{regionModePath, filepath.Join("..", regionModePath)}
		for _, path := range paths {
			if err := LoadIARUModeInferenceFile(path); err == nil {
				return
			}
		}
	})
}

// LoadIARUModeInferenceFile replaces the startup-owned region-mode table from
// YAML. Runtime startup calls this with the active config directory; the package
// no longer seeds hard-coded band/mode tables before YAML load.
func LoadIARUModeInferenceFile(path string) error {
	_, err := LoadIARUModeInferenceFileWithWarnings(path)
	return err
}

func LoadIARUModeInferenceFileWithWarnings(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	unknown, err := yamlconfig.UnknownFieldDiagnostics(path, data, regionModeTable{})
	if err != nil {
		return nil, err
	}
	warnings := make([]string, 0, len(unknown))
	for _, diag := range unknown {
		warnings = append(warnings, diag.Error())
	}
	var table regionModeTable
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&table); err != nil {
		return warnings, fmt.Errorf("parse regional mode table %s: %w", path, err)
	}
	if err := validateRegionModeTable(table); err != nil {
		return warnings, fmt.Errorf("validate regional mode table %s: %w", path, err)
	}
	regionModes = regionModeTable{
		VoiceModeByBand:     cloneStringMap(table.VoiceModeByBand),
		UnknownRegionCWSafe: cloneSegmentMap(table.UnknownRegionCWSafe),
		Regions:             cloneRegionSegmentMap(table.Regions),
	}
	regionModesSet = true
	return warnings, nil
}

func validateRegionModeTable(table regionModeTable) error {
	if len(table.VoiceModeByBand) == 0 {
		return fmt.Errorf("voice_mode_by_band must not be empty")
	}
	if len(table.UnknownRegionCWSafe) == 0 {
		return fmt.Errorf("unknown_region_cw_safe must not be empty")
	}
	if len(table.Regions) == 0 {
		return fmt.Errorf("regions must not be empty")
	}
	for band, mode := range table.VoiceModeByBand {
		if strings.TrimSpace(band) == "" {
			return fmt.Errorf("voice_mode_by_band contains empty band")
		}
		normalizedMode := strings.TrimSpace(strings.ToUpper(mode))
		if normalizedMode != "USB" && normalizedMode != "LSB" && normalizedMode != "SSB" {
			return fmt.Errorf("voice_mode_by_band.%s has invalid mode %q", band, mode)
		}
	}
	if err := validateRegionSegmentMap("unknown_region_cw_safe", table.UnknownRegionCWSafe); err != nil {
		return err
	}
	for region, bands := range table.Regions {
		if NormalizeIARURegion(region) == IARURegionUnknown {
			return fmt.Errorf("regions contains invalid region %q", region)
		}
		if err := validateRegionSegmentMap("regions."+region, bands); err != nil {
			return err
		}
	}
	return nil
}

func validateRegionSegmentMap(name string, byBand map[string][]regionModeSegment) error {
	if len(byBand) == 0 {
		return fmt.Errorf("%s must not be empty", name)
	}
	for band, segments := range byBand {
		if strings.TrimSpace(band) == "" {
			return fmt.Errorf("%s contains empty band", name)
		}
		if len(segments) == 0 {
			return fmt.Errorf("%s.%s must not be empty", name, band)
		}
		for i, segment := range segments {
			if segment.ToKHz <= segment.FromKHz {
				return fmt.Errorf("%s.%s[%d] must have to_khz > from_khz", name, band, i)
			}
			if normalizeRegionModeClass(segment.Class) == regionModeClassUnknown {
				return fmt.Errorf("%s.%s[%d] has invalid class %q", name, band, i, segment.Class)
			}
		}
	}
	return nil
}

func cloneStringMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneSegmentMap(src map[string][]regionModeSegment) map[string][]regionModeSegment {
	dst := make(map[string][]regionModeSegment, len(src))
	for key, segments := range src {
		dst[key] = append([]regionModeSegment(nil), segments...)
	}
	return dst
}

func cloneRegionSegmentMap(src map[string]map[string][]regionModeSegment) map[string]map[string][]regionModeSegment {
	dst := make(map[string]map[string][]regionModeSegment, len(src))
	for region, bands := range src {
		dstBands := make(map[string][]regionModeSegment, len(bands))
		for band, segments := range bands {
			dstBands[band] = append([]regionModeSegment(nil), segments...)
		}
		dst[region] = dstBands
	}
	return dst
}

func normalizeRegionModeClass(class string) regionModeClass {
	switch strings.TrimSpace(strings.ToLower(class)) {
	case string(regionModeClassCWSafe):
		return regionModeClassCWSafe
	case string(regionModeClassVoiceDefault):
		return regionModeClassVoiceDefault
	case string(regionModeClassMixed):
		return regionModeClassMixed
	default:
		return regionModeClassUnknown
	}
}

func classifySegment(segments []regionModeSegment, freqKHz float64) regionModeClass {
	for _, segment := range segments {
		if freqKHz < segment.FromKHz || freqKHz > segment.ToKHz {
			continue
		}
		return normalizeRegionModeClass(segment.Class)
	}
	return regionModeClassUnknown
}

func regionModeBandForFrequency(freqKHz float64) string {
	band := strings.TrimSpace(NormalizeBand(FreqToBand(freqKHz)))
	if band == "" || band == "???" {
		return ""
	}
	return band
}

func regionVoiceModeForBand(band string, freqKHz float64) string {
	loadRegionModes()
	mode := strings.TrimSpace(regionModes.VoiceModeByBand[band])
	if mode == "" {
		return ""
	}
	return NormalizeVoiceMode(mode, freqKHz)
}

// ClassifyRegionalMode applies the final region-aware frequency policy after
// explicit mode, recent-evidence reuse, and digital inference are exhausted.
// It returns a radio mode only for cw_safe and voice_default segments.
func ClassifyRegionalMode(s *Spot) RegionalModeResult {
	if s == nil {
		return RegionalModeResult{}
	}
	loadRegionModes()

	band := regionModeBandForFrequency(s.Frequency)
	if band == "" {
		return RegionalModeResult{Provenance: ModeProvenanceRegionalUnknownBlank}
	}

	region := s.DXMetadata.IARURegion
	if region != IARURegionUnknown {
		segments := regionModes.Regions[string(region)][band]
		switch classifySegment(segments, s.Frequency) {
		case regionModeClassCWSafe:
			return RegionalModeResult{Mode: "CW", Provenance: ModeProvenanceRegionalCW}
		case regionModeClassVoiceDefault:
			if mode := regionVoiceModeForBand(band, s.Frequency); mode != "" {
				return RegionalModeResult{Mode: mode, Provenance: ModeProvenanceRegionalVoiceDefault}
			}
			return RegionalModeResult{Provenance: ModeProvenanceRegionalMixedBlank}
		case regionModeClassMixed:
			return RegionalModeResult{Provenance: ModeProvenanceRegionalMixedBlank}
		default:
			return RegionalModeResult{Provenance: ModeProvenanceRegionalMixedBlank}
		}
	}

	switch classifySegment(regionModes.UnknownRegionCWSafe[band], s.Frequency) {
	case regionModeClassCWSafe:
		return RegionalModeResult{Mode: "CW", Provenance: ModeProvenanceRegionalCW}
	default:
		return RegionalModeResult{Provenance: ModeProvenanceRegionalUnknownBlank}
	}
}
