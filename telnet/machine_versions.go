// File role: Machine schema 1 is a stable projection of the complete preferences. State
// rules remain owned by the full configuration, including revisions and disk.
// Schema 2 exposes those rules; schema 3 additionally exposes per-mode SNR minima.
// Both additions require explicit request selection.
package telnet

import "dxcluster/filter"

// Keep the v1 declaration order and YAML names unchanged. Encoding the expanded
// internal type directly would break existing strict clients even on GET.
type machineFilterV1 struct {
	Bands            filter.StringRules `yaml:"bands"`
	Modes            filter.StringRules `yaml:"modes"`
	Sources          filter.StringRules `yaml:"sources"`
	Events           filter.StringRules `yaml:"events"`
	Confidence       filter.StringRules `yaml:"confidence"`
	PathClasses      filter.StringRules `yaml:"path_classes"`
	DXContinents     filter.StringRules `yaml:"dx_continents"`
	DEContinents     filter.StringRules `yaml:"de_continents"`
	DXZones          filter.IntRules    `yaml:"dx_zones"`
	DEZones          filter.IntRules    `yaml:"de_zones"`
	DXGrid2          filter.StringRules `yaml:"dx_grid2"`
	DEGrid2          filter.StringRules `yaml:"de_grid2"`
	DXDXCC           filter.IntRules    `yaml:"dx_dxcc"`
	DEDXCC           filter.IntRules    `yaml:"de_dxcc"`
	DXCallsigns      []string           `yaml:"dx_callsigns"`
	BlockDXCallsigns []string           `yaml:"block_dx_callsigns"`
	DECallsigns      []string           `yaml:"de_callsigns"`
	BlockDECallsigns []string           `yaml:"block_de_callsigns"`
	IncludeBeacons   filter.DefaultBool `yaml:"include_beacons"`
	AllowWWV         filter.DefaultBool `yaml:"allow_wwv"`
	AllowWCY         filter.DefaultBool `yaml:"allow_wcy"`
	AllowAnnounce    filter.DefaultBool `yaml:"allow_announce"`
	AllowSelf        filter.DefaultBool `yaml:"allow_self"`
	AllowToxic       filter.DefaultBool `yaml:"allow_toxic"`
	NearbyEnabled    bool               `yaml:"nearby_enabled"`
}

// Keep schema 2 frozen as well: its original state-field placement differs
// from the append-only presence mask, and later internal fields must stay hidden.
type machineFilterV2 struct {
	Bands            filter.StringRules `yaml:"bands"`
	Modes            filter.StringRules `yaml:"modes"`
	Sources          filter.StringRules `yaml:"sources"`
	Events           filter.StringRules `yaml:"events"`
	Confidence       filter.StringRules `yaml:"confidence"`
	PathClasses      filter.StringRules `yaml:"path_classes"`
	DXStates         filter.StringRules `yaml:"dx_states"`
	DEStates         filter.StringRules `yaml:"de_states"`
	DXContinents     filter.StringRules `yaml:"dx_continents"`
	DEContinents     filter.StringRules `yaml:"de_continents"`
	DXZones          filter.IntRules    `yaml:"dx_zones"`
	DEZones          filter.IntRules    `yaml:"de_zones"`
	DXGrid2          filter.StringRules `yaml:"dx_grid2"`
	DEGrid2          filter.StringRules `yaml:"de_grid2"`
	DXDXCC           filter.IntRules    `yaml:"dx_dxcc"`
	DEDXCC           filter.IntRules    `yaml:"de_dxcc"`
	DXCallsigns      []string           `yaml:"dx_callsigns"`
	BlockDXCallsigns []string           `yaml:"block_dx_callsigns"`
	DECallsigns      []string           `yaml:"de_callsigns"`
	BlockDECallsigns []string           `yaml:"block_de_callsigns"`
	IncludeBeacons   filter.DefaultBool `yaml:"include_beacons"`
	AllowWWV         filter.DefaultBool `yaml:"allow_wwv"`
	AllowWCY         filter.DefaultBool `yaml:"allow_wcy"`
	AllowAnnounce    filter.DefaultBool `yaml:"allow_announce"`
	AllowSelf        filter.DefaultBool `yaml:"allow_self"`
	AllowToxic       filter.DefaultBool `yaml:"allow_toxic"`
	NearbyEnabled    bool               `yaml:"nearby_enabled"`
}

func machineSchemaVersion(version int) int {
	if version == 2 || version == 3 {
		return version
	}
	return 1
}

func projectMachineConfiguration(cfg filter.Configuration, version int) filter.Configuration {
	if machineSchemaVersion(version) < 3 {
		cfg.Filters.MinSNR = nil
	}
	if machineSchemaVersion(version) == 1 {
		cfg.Filters.DXStates, cfg.Filters.DEStates = filter.StringRules{}, filter.StringRules{}
	}
	return cfg
}

func machineV1Filter(f filter.FilterConfiguration) machineFilterV1 {
	return machineFilterV1{
		Bands: f.Bands, Modes: f.Modes, Sources: f.Sources, Events: f.Events,
		Confidence: f.Confidence, PathClasses: f.PathClasses,
		DXContinents: f.DXContinents, DEContinents: f.DEContinents,
		DXZones: f.DXZones, DEZones: f.DEZones, DXGrid2: f.DXGrid2, DEGrid2: f.DEGrid2,
		DXDXCC: f.DXDXCC, DEDXCC: f.DEDXCC,
		DXCallsigns: f.DXCallsigns, BlockDXCallsigns: f.BlockDXCallsigns,
		DECallsigns: f.DECallsigns, BlockDECallsigns: f.BlockDECallsigns,
		IncludeBeacons: f.IncludeBeacons, AllowWWV: f.AllowWWV, AllowWCY: f.AllowWCY,
		AllowAnnounce: f.AllowAnnounce, AllowSelf: f.AllowSelf, AllowToxic: f.AllowToxic,
		NearbyEnabled: f.NearbyEnabled,
	}
}

func machineV2Filter(f filter.FilterConfiguration) machineFilterV2 {
	return machineFilterV2{
		Bands: f.Bands, Modes: f.Modes, Sources: f.Sources, Events: f.Events,
		Confidence: f.Confidence, PathClasses: f.PathClasses,
		DXStates: f.DXStates, DEStates: f.DEStates,
		DXContinents: f.DXContinents, DEContinents: f.DEContinents,
		DXZones: f.DXZones, DEZones: f.DEZones, DXGrid2: f.DXGrid2, DEGrid2: f.DEGrid2,
		DXDXCC: f.DXDXCC, DEDXCC: f.DEDXCC,
		DXCallsigns: f.DXCallsigns, BlockDXCallsigns: f.BlockDXCallsigns,
		DECallsigns: f.DECallsigns, BlockDECallsigns: f.BlockDECallsigns,
		IncludeBeacons: f.IncludeBeacons, AllowWWV: f.AllowWWV, AllowWCY: f.AllowWCY,
		AllowAnnounce: f.AllowAnnounce, AllowSelf: f.AllowSelf, AllowToxic: f.AllowToxic,
		NearbyEnabled: f.NearbyEnabled,
	}
}

func readbackResourceConfigurationVersion(cfg filter.Configuration, resource string, version int) (filter.Configuration, any, error) {
	bounded, data, err := readbackResourceConfiguration(cfg, resource)
	if err != nil || machineSchemaVersion(version) == 3 || resource == "SETTINGS" {
		return bounded, data, err
	}
	bounded = projectMachineConfiguration(bounded, version)
	var f any = machineV1Filter(cfg.Filters)
	if machineSchemaVersion(version) == 2 {
		f = machineV2Filter(cfg.Filters)
	}
	if resource == "FILTER" {
		return bounded, f, nil
	}
	return bounded, struct {
		Filters  any                          `yaml:"filters"`
		Settings filter.SettingsConfiguration `yaml:"settings"`
	}{f, cfg.Settings}, nil
}

// Old schemas bound their visible projection, with hidden maps independently
// checked before any full clone. This preserves old admission without inventing
// an ordinary-user file size limit. V3 bounds the complete configuration.
func machineConfigurationFits(cfg filter.Configuration, version int) error {
	if err := cfg.ValidateMinSNRRules(); err != nil {
		return err
	}
	if err := cfg.ValidateStateRules(); err != nil {
		return err
	}
	if !projectMachineConfiguration(cfg, version).MinimumSizeFits(maxYAMLBytes) {
		return errReadbackTooLarge
	}
	return nil
}
