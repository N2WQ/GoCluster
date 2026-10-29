// File role: Frozen generation-5 recognition for login-owned discard/reset only.
// These DTOs never back an active codec. The document owns one decode-work
// budget, including nested baselines and every shared-library entry.
package filter

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

const retiredV5Version = 5

type retiredV5Rule struct {
	Slot   int        `yaml:"slot"`
	Action RuleAction `yaml:"action"`
	Expr   string     `yaml:"expr"`
}

type retiredV5Filters struct {
	Rules          []retiredV5Rule `yaml:"rules"`
	NearbyEnabled  bool            `yaml:"nearby_enabled"`
	IncludeBeacons DefaultBool     `yaml:"include_beacons"`
	AllowWWV       DefaultBool     `yaml:"allow_wwv"`
	AllowWCY       DefaultBool     `yaml:"allow_wcy"`
	AllowAnnounce  DefaultBool     `yaml:"allow_announce"`
	AllowSelf      DefaultBool     `yaml:"allow_self"`
	AllowToxic     DefaultBool     `yaml:"allow_toxic"`
}

type retiredV5Settings struct {
	Dialect                 string `yaml:"dialect"`
	Grid                    string `yaml:"grid"`
	NoiseClass              string `yaml:"noise_class"`
	DedupePolicy            string `yaml:"dedupe_policy"`
	PathMinObservationCount int    `yaml:"path_min_observation_count"`
	SolarSummaryMinutes     int    `yaml:"solar_summary_minutes"`
}

type retiredV5Configuration struct {
	Filters  retiredV5Filters  `yaml:"filters"`
	Settings retiredV5Settings `yaml:"settings"`
}

type retiredV5Preset struct {
	ConfigurationVersion int                    `yaml:"configuration_version"`
	Configuration        retiredV5Configuration `yaml:"configuration"`
}

type retiredV5Reference struct {
	Name     string           `yaml:"name"`
	Baseline *retiredV5Preset `yaml:"baseline"`
}

type retiredV5Record struct {
	ConfigurationVersion int                    `yaml:"configuration_version"`
	Configuration        retiredV5Configuration `yaml:"configuration"`
	Preset               *retiredV5Reference    `yaml:"preset,omitempty"`
	RecentIPs            []string               `yaml:"recent_ips,omitempty"`
	LastLoginUTC         time.Time              `yaml:"last_login_utc,omitempty"`
}

func (state *storageDecodeState) validateRetiredV5Fields(node *yaml.Node, typ reflect.Type) error {
	if typ == reflect.TypeFor[retiredV5Record]() || typ == reflect.TypeFor[retiredV5Preset]() {
		marker := v6StoredField(node, "configuration_version")
		var version int
		if marker == nil || marker.Kind != yaml.ScalarNode || marker.Tag != "!!int" ||
			marker.Decode(&version) != nil || version != retiredV5Version {
			return ErrUnsupportedConfigurationVersion
		}
	}
	if err := state.validateStoredMapping(node, typ); err != nil {
		return err
	}
	fields := state.storedFields(typ)
	if len(node.Content)/2 > len(fields) {
		return errors.New("too many or duplicate retired-v5 fields")
	}
	if err := state.rawDuplicates(node); err != nil {
		return err
	}
	var required []string
	switch typ {
	case reflect.TypeFor[retiredV5Record](), reflect.TypeFor[retiredV5Preset]():
		required = []string{"configuration"}
	case reflect.TypeFor[retiredV5Configuration]():
		required = []string{"filters", "settings"}
	case reflect.TypeFor[retiredV5Filters]():
		required = []string{"rules"}
		if rules := v6StoredField(node, "rules"); rules != nil && rules.Kind == yaml.SequenceNode && len(rules.Content) > 10 {
			return storageAdmissionf("retired-v5 rules exceed slot limit 10")
		}
	case reflect.TypeFor[retiredV5Rule]():
		required = []string{"slot", "action", "expr"}
		if source := v6StoredField(node, "expr"); source != nil && len(source.Value) > 1024 {
			return storageAdmissionf("retired-v5 expression exceeds byte limit")
		}
	case reflect.TypeFor[retiredV5Reference]():
		required = []string{"name", "baseline"}
	}
	for _, name := range required {
		if v6StoredField(node, name) == nil {
			return fmt.Errorf("missing required retired-v5 field %s", name)
		}
	}
	for i := 0; i < len(node.Content); i += 2 {
		name := node.Content[i].Value
		if err := validateStoredValueWithFields(node.Content[i+1], fields[name].typ, state.validateRetiredV5Fields); err != nil {
			return fmt.Errorf("invalid retired-v5 field %s: %w", name, err)
		}
	}
	return nil
}

// No nested decoder is opened here: dry-run visits charge the caller's budget
// before the same admitted values are materialized into a temporary frozen DTO.
func decodeRetiredV5Value(node *yaml.Node, state *storageDecodeState, output any) error {
	out := reflect.ValueOf(output).Elem()
	if err := validateStoredValueWithFields(node, out.Type(), state.validateRetiredV5Fields); err != nil {
		return err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(node, out.Type(), reflect.Value{}, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	prior := state.materialize
	state.materialize = true
	defer func() { state.materialize = prior }()
	_, _, err := state.decode(node, out.Type(), out, newStorageDecodeGuard(true))
	return err
}

func retiredV5ComparisonField(field string) bool {
	switch field {
	case "BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL", "DXCONT", "DECONT",
		"DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2",
		"FREQ", "SNR", "CONF", "PATH", "COMMENT":
		return true
	default:
		return false
	}
}

func prepareRetiredV5Configuration(cfg *retiredV5Configuration) error {
	if _, fits := retiredV5MinimumRemaining(cfg, MaxPresetBytes); !fits {
		return storageAdmissionf("retired-v5 configuration exceeds preset bounds")
	}
	definitions := make([]RuleDefinition, len(cfg.Filters.Rules))
	for i, rule := range cfg.Filters.Rules {
		tree, err := expr.Parse(rule.Expr)
		if err != nil {
			return err
		}
		for _, node := range tree.Nodes {
			switch node.Kind {
			case expr.AtomKind:
				if !retiredV5ComparisonField(node.Atom.Field) || len(node.Atom.Values) == 0 {
					return errors.New("retired-v5 rules require comparison fields")
				}
			case expr.NotKind, expr.AndKind, expr.OrKind:
			default:
				return errors.New("retired-v5 rules do not support predicates")
			}
		}
		definitions[i] = RuleDefinition(rule)
	}
	// Temporary current preparation checks the unchanged comparison vocabulary,
	// operators, operands, identities and aggregate budgets. No program escapes.
	set, err := PrepareRuleSet(definitions)
	if err != nil {
		return err
	}
	for i, rule := range set.Definitions() {
		cfg.Filters.Rules[i] = retiredV5Rule(rule)
	}
	return admitRetiredV5Bytes(&retiredV5Preset{ConfigurationVersion: retiredV5Version, Configuration: *cfg}, MaxPresetBytes)
}

// Keep the historical lower-bound admission before rule copies or rendering.
// Raw expressions may shrink during canonicalization, so charge one byte after
// checking their separate raw bound; settings and action bytes cannot shrink.
func retiredV5MinimumRemaining(cfg *retiredV5Configuration, remaining int) (int, bool) {
	consume := func(size int) bool {
		if size < 0 || size > remaining {
			return false
		}
		remaining -= size
		return true
	}
	s := cfg.Settings
	for _, value := range [...]string{s.Dialect, s.Grid, s.NoiseClass, s.DedupePolicy} {
		if !consume(len(value)) {
			return 0, false
		}
	}
	if len(cfg.Filters.Rules) > 10 {
		return 0, false
	}
	for _, rule := range cfg.Filters.Rules {
		if len(rule.Expr) > 1024 || !consume(len(rule.Action)) || rule.Expr != "" && !consume(1) {
			return 0, false
		}
	}
	return remaining, true
}

func retiredV5RecordMinimumFits(record *retiredV5Record) bool {
	remaining := MaxUserRecordBytes
	for _, ip := range trimRecentIPs(record.RecentIPs) {
		if len(ip) > remaining || remaining-len(ip) < 3 {
			return false
		}
		remaining -= len(ip) + 3
	}
	var fits bool
	remaining, fits = retiredV5MinimumRemaining(&record.Configuration, remaining)
	if !fits {
		return false
	}
	if ref := record.Preset; ref != nil {
		if len(ref.Name) > remaining || ref.Baseline == nil {
			return false
		}
		_, fits = retiredV5MinimumRemaining(&ref.Baseline.Configuration, remaining-len(ref.Name))
	}
	return fits
}

func admitRetiredV5Bytes(value any, limit int) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > limit {
		return storageAdmissionf("retired-v5 encoding exceeds %d bytes", limit)
	}
	return nil
}

func validateRetiredV5Preset(node *yaml.Node, state *storageDecodeState) error {
	var value retiredV5Preset
	if err := decodeRetiredV5Value(node, state, &value); err != nil {
		return err
	}
	return prepareRetiredV5Configuration(&value.Configuration)
}

func decodeRetiredV5Login(node *yaml.Node, state *storageDecodeState, defaults *PreparedRuleSet) (*UserRecord, error) {
	var value retiredV5Record
	if err := decodeRetiredV5Value(node, state, &value); err != nil {
		return nil, err
	}
	if !retiredV5RecordMinimumFits(&value) {
		return nil, storageAdmissionf("retired-v5 user record exceeds byte bound")
	}
	if err := prepareRetiredV5Configuration(&value.Configuration); err != nil {
		return nil, err
	}
	if ref := value.Preset; ref != nil {
		name, err := NormalizePresetName(ref.Name)
		if err != nil || name != ref.Name || ref.Baseline == nil {
			return nil, errors.New("invalid retired-v5 applied preset")
		}
		if err := prepareRetiredV5Configuration(&ref.Baseline.Configuration); err != nil {
			return nil, err
		}
	}
	value.RecentIPs = trimRecentIPs(value.RecentIPs)
	if err := admitRetiredV5Bytes(&value, MaxUserRecordBytes); err != nil {
		return nil, err
	}
	f, s := value.Configuration.Filters, value.Configuration.Settings
	return &UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion,
		Filter: Filter{preparedRules: defaults, IncludeBeacons: f.IncludeBeacons.Pointer(),
			AllowWWV: f.AllowWWV.Pointer(), AllowWCY: f.AllowWCY.Pointer(), AllowAnnounce: f.AllowAnnounce.Pointer(),
			AllowSelf: f.AllowSelf.Pointer(), AllowToxic: f.AllowToxic.Pointer()},
		Dialect: s.Dialect, Grid: s.Grid, NoiseClass: s.NoiseClass, DedupePolicy: s.DedupePolicy,
		PathMinObservationCount: s.PathMinObservationCount, SolarSummaryMinutes: s.SolarSummaryMinutes,
		RecentIPs: value.RecentIPs, LastLoginUTC: value.LastLoginUTC,
	}, nil
}
