// File role: Loads and applies the startup-owned IARU region reference table.
// Crawler notes: Start here for DXCC/ADIF-to-IARU-region config diagnostics,
// region normalization, and reference-table startup failures.
// Related docs: data/config/README.md, docs/decisions/ADR-0153-startup-config-diagnostics-and-gridstore-logging.md.
// Related tests: spot/iaru_region_test.go.
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

// IARURegion identifies the DX station's IARU region for region-aware
// frequency policy. The value is derived from CTY/DXCC metadata and stored on
// spot metadata so downstream consumers do not repeat the lookup.
type IARURegion string

const (
	IARURegionUnknown IARURegion = ""
	IARURegion1       IARURegion = "R1"
	IARURegion2       IARURegion = "R2"
	IARURegion3       IARURegion = "R3"
)

type iaruRegionTable struct {
	DefaultsByContinent map[string]string `yaml:"defaults_by_continent"`
	ADIFOverrides       map[int]string    `yaml:"adif_overrides"`
}

var (
	iaruRegionOnce sync.Once
	iaruRegions    iaruRegionTable
	iaruRegionsSet bool
)

const iaruRegionPath = "data/config/iaru_regions.yaml"

// NormalizeIARURegion normalizes a config/runtime region token to R1/R2/R3.
func NormalizeIARURegion(region string) IARURegion {
	switch strings.TrimSpace(strings.ToUpper(region)) {
	case "R1", "1", "REGION1", "REGION 1":
		return IARURegion1
	case "R2", "2", "REGION2", "REGION 2":
		return IARURegion2
	case "R3", "3", "REGION3", "REGION 3":
		return IARURegion3
	default:
		return IARURegionUnknown
	}
}

// LoadIARURegionsFile replaces the startup-owned IARU region table from YAML.
// Runtime startup calls this with the active config directory so region policy
// cannot silently fall back to built-in tables.
func LoadIARURegionsFile(path string) error {
	_, err := LoadIARURegionsFileWithWarnings(path)
	return err
}

func LoadIARURegionsFileWithWarnings(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	unknown, err := yamlconfig.UnknownFieldDiagnostics(path, data, iaruRegionTable{})
	if err != nil {
		return nil, err
	}
	warnings := make([]string, 0, len(unknown))
	for _, diag := range unknown {
		warnings = append(warnings, diag.Error())
	}
	var table iaruRegionTable
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&table); err != nil {
		return warnings, fmt.Errorf("parse IARU region map %s: %w", path, err)
	}
	if err := validateIARURegionTable(table); err != nil {
		return warnings, fmt.Errorf("validate IARU region map %s: %w", path, err)
	}
	iaruRegions = table
	iaruRegionsSet = true
	return warnings, nil
}

func validateIARURegionTable(table iaruRegionTable) error {
	if len(table.DefaultsByContinent) == 0 {
		return fmt.Errorf("defaults_by_continent must not be empty")
	}
	for continent, region := range table.DefaultsByContinent {
		if strings.TrimSpace(continent) == "" {
			return fmt.Errorf("defaults_by_continent contains empty continent")
		}
		if NormalizeIARURegion(region) == IARURegionUnknown {
			return fmt.Errorf("defaults_by_continent.%s has invalid region %q", continent, region)
		}
	}
	for adif, region := range table.ADIFOverrides {
		if adif <= 0 {
			return fmt.Errorf("adif_overrides contains invalid ADIF %d", adif)
		}
		if NormalizeIARURegion(region) == IARURegionUnknown {
			return fmt.Errorf("adif_overrides.%d has invalid region %q", adif, region)
		}
	}
	return nil
}

func loadIARURegions() {
	iaruRegionOnce.Do(func() {
		if iaruRegionsSet {
			return
		}
		paths := []string{iaruRegionPath, filepath.Join("..", iaruRegionPath)}
		for _, path := range paths {
			if err := LoadIARURegionsFile(path); err == nil {
				return
			}
		}
	})
}

// ResolveIARURegion maps CTY/DXCC metadata to an IARU region. Unknown is
// returned when the ADIF/continent cannot be resolved deterministically.
func ResolveIARURegion(adif int, continent string) IARURegion {
	loadIARURegions()
	if adif > 0 {
		if region := NormalizeIARURegion(iaruRegions.ADIFOverrides[adif]); region != IARURegionUnknown {
			return region
		}
	}
	continent = strings.TrimSpace(strings.ToUpper(continent))
	if continent == "" {
		return IARURegionUnknown
	}
	return NormalizeIARURegion(iaruRegions.DefaultsByContinent[continent])
}
