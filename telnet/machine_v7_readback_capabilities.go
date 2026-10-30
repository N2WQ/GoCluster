// File role: Advertises native schema-seven ownership and actual resource bounds
// while labeling the retained schema-six GO interface independently.
package telnet

import (
	"slices"

	"dxcluster/filter"
)

type v7CCCapability struct {
	Fields         []string                  `yaml:"fields"`
	Geography      []string                  `yaml:"geography"`
	Bands          []string                  `yaml:"bands"`
	Slots          []filter.CCSlotDefinition `yaml:"slots"`
	MaxModes       int                       `yaml:"max_modes"`
	MaxCountries   int                       `yaml:"max_countries_per_row"`
	MaxStates      int                       `yaml:"max_states_per_row"`
	CanonicalBytes int                       `yaml:"canonical_bytes"`
}

// Legacy protocol limits stay explicitly labeled: the top-level limits govern
// schema-seven GO and CC, while the nested reference describes schema six.
// Per-band terminal/wire compatibility is deliberately not claimed here.
func v7MachineCapabilities() any {
	return struct {
		SupportedSchemaVersions       []int          `yaml:"supported_schema_versions"`
		Resources                     []string       `yaml:"resources"`
		GOFilterFields                []string       `yaml:"go_filter_fields"`
		GOLegacySchema6               any            `yaml:"go_legacy_schema6_capabilities"`
		CC                            v7CCCapability `yaml:"cc"`
		SettingsFields                []string       `yaml:"settings_fields"`
		ResponseBytes                 int            `yaml:"response_bytes"`
		UploadBytes                   int            `yaml:"upload_bytes"`
		ConfigurationBytes            int            `yaml:"configuration_bytes"`
		CompleteBackupResource        string         `yaml:"complete_backup_resource"`
		NativePerBandTerminalVerified bool           `yaml:"native_per_band_terminal_verified"`
	}{
		SupportedSchemaVersions: []int{6, 7}, Resources: []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"},
		GOFilterFields: slices.Clone(machineV6FilterFields[:]), GOLegacySchema6: v6MachineCapabilities(),
		CC:             v7CCCapability{machineV7CCFields[:], machineV7GeographyNames[:], filter.CCSupportedBands(), filter.CCSupportedSlots(), filter.MaxCCModes, filter.MaxCCCountries, filter.MaxCCStates, filter.MaxCCCanonicalBytes},
		SettingsFields: slices.Clone(machineSettingFields[:]), ResponseBytes: maxV7YAMLBytes, UploadBytes: maxV7YAMLBytes,
		ConfigurationBytes: filter.MaxV7ConfigurationBytes, CompleteBackupResource: "PUT YAML CONFIG SCHEMA 7",
		NativePerBandTerminalVerified: false,
	}
}
