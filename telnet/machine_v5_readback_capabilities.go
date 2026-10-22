package telnet

import (
	"slices"

	"dxcluster/filter"
	"dxcluster/filter/expr"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

type v5RuleFieldCapability struct {
	Name       string       `yaml:"name"`
	Operators  []*yaml.Node `yaml:"operators"`
	Geographic bool         `yaml:"geographic"`
}

// This advertises model-owned syntax and vocabularies, not fabricated server
// availability, human line limits, or runtime setting restrictions.
func v5MachineCapabilities() any {
	fields := filter.SupportedRuleFields()
	ruleFields := make([]v5RuleFieldCapability, 0, len(fields))
	for _, name := range fields {
		operators := filter.RuleFieldOperators(name)
		quoted := make([]*yaml.Node, len(operators))
		for i, operator := range operators {
			// Bare "=" is a YAML 1.1 value tag in common client loaders.
			quoted[i] = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: operator, Style: yaml.DoubleQuotedStyle}
		}
		ruleFields = append(ruleFields, v5RuleFieldCapability{
			Name: name, Operators: quoted, Geographic: filter.IsGeographicRuleField(name),
		})
	}
	type ruleLimits struct {
		MinSlot          int `yaml:"min_slot"`
		MaxSlot          int `yaml:"max_slot"`
		MaxRules         int `yaml:"max_rules"`
		MaxExprBytes     int `yaml:"max_expr_bytes"`
		MaxAtomsPerRule  int `yaml:"max_atoms_per_rule"`
		MaxNodesPerRule  int `yaml:"max_nodes_per_rule"`
		MaxDepth         int `yaml:"max_depth"`
		MaxValuesPerAtom int `yaml:"max_values_per_atom"`
		MaxTotalAtoms    int `yaml:"max_total_atoms"`
		MaxTotalValues   int `yaml:"max_total_values"`
	}
	type vocabulary struct {
		Bands       []string `yaml:"bands"`
		Modes       []string `yaml:"modes"`
		Sources     []string `yaml:"sources"`
		Events      []string `yaml:"events"`
		Continents  []string `yaml:"continents"`
		States      []string `yaml:"states"`
		Confidence  []string `yaml:"confidence"`
		PathClasses []string `yaml:"path_classes"`
	}
	return struct {
		SupportedSchemaVersions []int                   `yaml:"supported_schema_versions"`
		Resources               []string                `yaml:"resources"`
		FilterFields            []string                `yaml:"filter_fields"`
		SettingsFields          []string                `yaml:"settings_fields"`
		RuleFields              []v5RuleFieldCapability `yaml:"rule_fields"`
		RuleLimits              ruleLimits              `yaml:"rule_limits"`
		Vocabulary              vocabulary              `yaml:"vocabulary"`
		Actions                 []filter.RuleAction     `yaml:"actions"`
		DefaultBooleanChoices   []filter.DefaultBool    `yaml:"default_boolean_choices"`
		ResponseBytes           int                     `yaml:"response_bytes"`
		UploadBytes             int                     `yaml:"upload_bytes"`
		PutComplete             bool                    `yaml:"put_requires_complete_resource"`
		PatchReplace            bool                    `yaml:"patch_collections_replace"`
		WriteRevisionRequired   bool                    `yaml:"write_revision_required"`
	}{
		SupportedSchemaVersions: []int{filter.MVPConfigurationVersion},
		Resources:               []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"},
		FilterFields:            slices.Clone(machineV5FilterFields[:]),
		SettingsFields:          slices.Clone(machineSettingFields[:]),
		RuleFields:              ruleFields,
		RuleLimits: ruleLimits{
			MinSlot: 0, MaxSlot: filter.MaxRuleSlots - 1, MaxRules: filter.MaxRuleSlots,
			MaxExprBytes: expr.MaxBytes, MaxAtomsPerRule: expr.MaxAtoms, MaxNodesPerRule: expr.MaxNodes,
			MaxDepth: expr.MaxDepth, MaxValuesPerAtom: expr.MaxValuesPerAtom,
			MaxTotalAtoms: filter.MaxTotalRuleAtoms, MaxTotalValues: filter.MaxTotalRuleValues,
		},
		Vocabulary: vocabulary{
			Bands: spot.SupportedBandNames(), Modes: filter.SupportedModes(),
			Sources: slices.Clone(filter.SupportedSources), Events: append([]string{"NONE"}, filter.SupportedEvents()...),
			Continents: slices.Clone(filter.SupportedContinents), States: spot.StateCodes(),
			Confidence: slices.Clone(filter.SupportedConfidenceSymbols), PathClasses: slices.Clone(filter.SupportedPathClasses),
		},
		Actions:               []filter.RuleAction{filter.RulePass, filter.RuleReject},
		DefaultBooleanChoices: []filter.DefaultBool{filter.DefaultBoolDefault, filter.DefaultBoolFalse, filter.DefaultBoolTrue},
		ResponseBytes:         maxYAMLBytes, UploadBytes: maxYAMLBytes,
		PutComplete: true, PatchReplace: true, WriteRevisionRequired: true,
	}
}
