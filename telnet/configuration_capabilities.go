// File role: Publishes the exact writable schema and active server choices.
// Taxonomy enumeration is bounded at startup (128 modes, 64 events); all other
// domains are fixed lists or ranges. No CTY traversal or user-rule clone occurs.
package telnet

import (
	"math"

	"dxcluster/filter"
	"dxcluster/spot"
)

type readbackIntegerDomain struct {
	Minimum int `yaml:"minimum"`
	Maximum int `yaml:"maximum"`
}

type readbackValueChoices struct {
	Bands       []string              `yaml:"bands"`
	Modes       []string              `yaml:"modes"`
	Sources     []string              `yaml:"sources"`
	Events      []string              `yaml:"events"`
	Confidence  []string              `yaml:"confidence"`
	PathClasses []string              `yaml:"path_classes"`
	Continents  []string              `yaml:"continents"`
	CQZones     readbackIntegerDomain `yaml:"cq_zones"`
	DXCC        readbackIntegerDomain `yaml:"dxcc"`
	Grid2       struct {
		FirstCharacter  string `yaml:"first_character"`
		SecondCharacter string `yaml:"second_character"`
	} `yaml:"grid2"`
	CallsignPatterns struct {
		Characters string `yaml:"characters"`
		Wildcard   string `yaml:"wildcard"`
	} `yaml:"callsign_patterns"`
	Dialects []string `yaml:"dialects"`
	Grid     struct {
		Lengths         []int `yaml:"lengths"`
		Uppercase       bool  `yaml:"uppercase"`
		EmptyUsesLookup bool  `yaml:"empty_uses_lookup"`
	} `yaml:"grid"`
	NoiseClasses   []string `yaml:"noise_classes"`
	DedupePolicies []string `yaml:"dedupe_policies"`
	PathMinimum    struct {
		ZeroUsesDefault   bool `yaml:"zero_uses_default"`
		OverrideAvailable bool `yaml:"override_available"`
		MinimumExclusive  int  `yaml:"minimum_exclusive"`
		Maximum           int  `yaml:"maximum"`
	} `yaml:"path_min_observation_count"`
	SolarSummaryMinutes []int `yaml:"solar_summary_minutes"`
}

func (s *Server) readbackCapabilities() any {
	choices := readbackValueChoices{
		Bands: spot.SupportedBandNames(), Modes: filter.SupportedModes(), Events: filter.SupportedEvents(),
		Sources: filter.SupportedSources, Confidence: filter.SupportedConfidenceSymbols,
		PathClasses: filter.SupportedPathClasses, Continents: filter.SupportedContinents,
		CQZones: readbackIntegerDomain{Minimum: 1, Maximum: 40}, DXCC: readbackIntegerDomain{Minimum: 1, Maximum: math.MaxInt},
		Dialects: []string{"", "go", "cc"}, NoiseClasses: []string{""}, DedupePolicies: []string{"", "FAST", "MED", "SLOW"},
		SolarSummaryMinutes: []int{0, 15, 30, 60},
	}
	choices.Grid2.FirstCharacter, choices.Grid2.SecondCharacter = "A-R", "A-R"
	choices.CallsignPatterns.Characters = "A-Z a-z 0-9 / -"
	choices.CallsignPatterns.Wildcard = "at most one * at the beginning or end"
	choices.Grid.Lengths, choices.Grid.Uppercase, choices.Grid.EmptyUsesLookup = []int{0, 4, 6}, true, true
	for _, class := range [...]string{"QUIET", "RURAL", "SUBURBAN", "URBAN", "INDUSTRIAL"} {
		if s.noiseClassKnown(class) {
			choices.NoiseClasses = append(choices.NoiseClasses, class)
		}
	}
	choices.PathMinimum.ZeroUsesDefault = true
	choices.PathMinimum.MinimumExclusive, choices.PathMinimum.OverrideAvailable = s.pathMinObservationDefault()
	choices.PathMinimum.Maximum = maxUserPathMinObservationCount
	return struct {
		Commands              []string             `yaml:"commands"`
		Resources             []string             `yaml:"resources"`
		SchemaVersions        []int                `yaml:"schema_versions"`
		FilterCategories      []string             `yaml:"filter_categories"`
		FilterFields          []string             `yaml:"filter_fields"`
		Settings              []string             `yaml:"settings"`
		RuleSetFields         []string             `yaml:"rule_set_fields"`
		Choices               readbackValueChoices `yaml:"choices"`
		DedupeAvailable       map[string]bool      `yaml:"dedupe_available"`
		DefaultBooleanChoices []filter.DefaultBool `yaml:"default_boolean_choices"`
		ResponseBytes         int                  `yaml:"response_bytes"`
		UploadBytes           int                  `yaml:"upload_bytes"`
		UploadSeconds         int                  `yaml:"upload_seconds"`
		PutComplete           bool                 `yaml:"put_requires_complete_resource"`
		PatchReplace          bool                 `yaml:"patch_collections_replace"`
		RevisionRequired      bool                 `yaml:"write_revision_required"`
		FreshGet              bool                 `yaml:"fresh_get_after_reconnect"`
		AdvancedYAML          bool                 `yaml:"advanced_yaml_features"`
	}{
		Commands:  []string{"GET YAML FILTER", "GET YAML SETTINGS", "GET YAML CONFIG", "GET YAML CAPABILITIES", "PUT YAML FILTER", "PUT YAML SETTINGS", "PUT YAML CONFIG", "PATCH YAML FILTER", "PATCH YAML SETTINGS", "PATCH YAML CONFIG", "VALIDATE YAML CONFIG"},
		Resources: []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"}, SchemaVersions: []int{1},
		FilterCategories: []string{"BAND", "MODE", "SOURCE", "EVENT", "CONFIDENCE", "PATH", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXGRID2", "DEGRID2", "DXDXCC", "DEDXCC", "DXCALL", "DECALL", "BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC", "NEARBY"},
		FilterFields:     machineFilterFields[:], Settings: machineSettingFields[:], RuleSetFields: []string{"allow_all", "block_all", "allow", "block"}, Choices: choices,
		DedupeAvailable:       map[string]bool{"FAST": s != nil && s.dedupeFastEnabled, "MED": s != nil && s.dedupeMedEnabled, "SLOW": s != nil && s.dedupeSlowEnabled},
		DefaultBooleanChoices: []filter.DefaultBool{filter.DefaultBoolDefault, filter.DefaultBoolFalse, filter.DefaultBoolTrue},
		ResponseBytes:         maxYAMLBytes, UploadBytes: maxYAMLBytes, UploadSeconds: 30,
		PutComplete: true, PatchReplace: true, RevisionRequired: true, FreshGet: true,
	}
}
