// File role: Bounded recognition of retired flat profiles at the explicit login
// upgrade boundary. Only settings, controls and login history are materialized;
// retired category maps are checked and discarded, never installed or translated.
package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type legacyLoginPreferences struct {
	ConfigurationVersion    int       `yaml:"configuration_version"`
	Dialect                 string    `yaml:"dialect"`
	Grid                    string    `yaml:"grid"`
	NoiseClass              string    `yaml:"noise_class"`
	DedupePolicy            string    `yaml:"dedupe_policy"`
	PathMinObservationCount int       `yaml:"path_min_observation_count"`
	SolarSummaryMinutes     int       `yaml:"solar_summary_minutes"`
	IncludeBeacons          *bool     `yaml:"include_beacons"`
	AllowWWV                *bool     `yaml:"allow_wwv"`
	AllowWCY                *bool     `yaml:"allow_wcy"`
	AllowAnnounce           *bool     `yaml:"allow_announce"`
	AllowSelf               *bool     `yaml:"allow_self"`
	AllowToxic              *bool     `yaml:"allow_toxic"`
	RecentIPs               []string  `yaml:"recent_ips"`
	LastLoginUTC            time.Time `yaml:"last_login_utc"`
}

func parseUpgradeDocument(data []byte, limit int, work uint64) (*yaml.Node, *storageDecodeState, error) {
	if len(data) > limit {
		return nil, nil, storageAdmissionf("upgrade document exceeds %d bytes", limit)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document, extra yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode || document.Content[0].Tag != "!!map" {
		return nil, nil, errors.New("upgrade document must contain one YAML mapping")
	}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("upgrade document must contain one YAML document")
	}
	state, err := newStorageDecodeStateWithLimits(&document, storageDecodeLimits{work: work})
	return document.Content[0], state, err
}

func upgradeStoredVersion(node *yaml.Node) (int, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return 0, errors.New("profile must be a mapping")
	}
	version, seen := 0, false
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Value != "configuration_version" {
			continue
		}
		aliased := value.Kind == yaml.AliasNode
		value = v5StoredNode(value)
		if seen || key.Kind != yaml.ScalarNode || key.Tag != "!!str" ||
			value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
			return 0, fmt.Errorf("%w: explicit integer marker required", ErrUnsupportedConfigurationVersion)
		}
		seen = true
		if err := value.Decode(&version); err != nil || version < 1 || version > CurrentConfigurationVersion {
			return 0, ErrUnsupportedConfigurationVersion
		}
		if aliased && version != CurrentConfigurationVersion {
			return 0, ErrUnsupportedConfigurationVersion
		}
	}
	return version, nil
}

// Field names are the actual yaml.v3 flat-schema names (many are lowercase
// without underscores). This fixed schema is operation-owned, not retained state.
func legacyDiscardedField(name string) (reflect.Type, int) {
	switch name {
	case "comments", "block_comments":
		return reflect.TypeFor[[]string](), 4
	case "min_snr":
		return reflect.TypeFor[map[string]int](), 3
	case "minconfidence":
		return reflect.TypeFor[int](), 0
	case "callsigns", "block_callsigns", "decallsigns", "block_decallsigns":
		return reflect.TypeFor[[]string](), 0
	case "bands", "blockbands", "modes", "blockmodes", "sources", "blocksources",
		"events", "blockevents", "confidence", "blockconfidence", "pathclasses", "blockpathclasses",
		"dxcontinents", "blockdxcontinents", "decontinents", "blockdecontinents",
		"dxgrid2prefixes", "blockdxgrid2", "degrid2prefixes", "blockdegrid2":
		return reflect.TypeFor[map[string]bool](), 0
	case "dxstates", "blockdxstates", "destates", "blockdestates":
		return reflect.TypeFor[map[string]bool](), 2
	case "dxzones", "blockdxzones", "dezones", "blockdezones",
		"dxdxcc", "blockdxdxcc", "dedxcc", "blockdedxcc":
		return reflect.TypeFor[map[int]bool](), 0
	case "alldxstates", "blockalldxstates", "alldestates", "blockalldestates":
		return reflect.TypeFor[bool](), 2
	case "allbands", "blockallbands", "allmodes", "blockallmodes",
		"allsources", "blockallsources", "allevents", "blockallevents",
		"allconfidence", "blockallconfidence", "allpathclasses", "blockallpathclasses",
		"alldxcontinents", "blockalldxcontinents", "alldecontinents", "blockalldecontinents",
		"alldxzones", "blockalldxzones", "alldezones", "blockalldezones",
		"alldxgrid2", "blockalldxgrid2", "alldegrid2", "blockalldegrid2",
		"alldxdxcc", "blockalldxdxcc", "alldedxcc", "blockalldedxcc", "nearby_enabled":
		return reflect.TypeFor[bool](), 0
	default:
		return nil, 0
	}
}

func validateLegacyOwner(node *yaml.Node, version int, state *storageDecodeState, preset bool) error {
	guard := newStorageDecodeGuard(false)
	var walk func(*yaml.Node) error
	walk = func(current *yaml.Node) error {
		if err := state.visit(current, guard); err != nil {
			return err
		}
		if current.Kind == yaml.AliasNode {
			if guard.active[current] {
				return errors.New("recursive legacy YAML alias")
			}
			guard.active[current] = true
			guard.aliasDepth++
			err := walk(current.Alias)
			guard.aliasDepth--
			delete(guard.active, current)
			return err
		}
		if current.Kind == yaml.SequenceNode {
			for _, item := range current.Content {
				if err := walk(item); err != nil {
					return err
				}
			}
			return nil
		}
		if current.Kind != yaml.MappingNode || current.Tag != "!!map" {
			return errors.New("legacy profile or merge must be a mapping")
		}
		if err := state.rawDuplicates(current); err != nil {
			return err
		}
		fields := state.storedFields(reflect.TypeFor[legacyLoginPreferences]())
		for i := 0; i < len(current.Content); i += 2 {
			key, value := current.Content[i], current.Content[i+1]
			if storedMergeKey(key) {
				// Marked formats never supported merges in their strict schema.
				if version != 0 {
					return errors.New("merge is not a marked flat profile field")
				}
				if err := walk(value); err != nil {
					return err
				}
				continue
			}
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return errors.New("legacy profile key must be a string")
			}
			if key.Value == "configuration_version" {
				var marker int
				if value.Kind != yaml.ScalarNode || value.Tag != "!!int" ||
					value.Decode(&marker) != nil || marker != version {
					return ErrUnsupportedConfigurationVersion
				}
			}
			if key.Value == "preset" && !preset {
				if version == 0 && value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
					continue // Historical unversioned nil association.
				}
				if err := validateLegacyAssociation(value, state); err != nil {
					return err
				}
				continue
			}
			typ, minimum := legacyDiscardedField(key.Value)
			if field := fields[key.Value]; field.typ != nil {
				if preset && (key.Value == "recent_ips" || key.Value == "last_login_utc") {
					return errors.New("login metadata in saved preset")
				}
				typ = field.typ
			}
			if typ == nil || version < minimum {
				return fmt.Errorf("unrecognized legacy field %q for version %d", key.Value, version)
			}
			if version != 0 {
				if err := validateStoredValueWithFields(value, typ, state.validateStoredMapping); err != nil {
					return fmt.Errorf("legacy field %s: %w", key.Value, err)
				}
			}
			if _, _, err := state.decode(value, typ, reflect.Value{}, guard); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(node); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return nil
}

func validateLegacyAssociation(node *yaml.Node, state *storageDecodeState) error {
	if node.Kind != yaml.MappingNode || len(node.Content) != 4 {
		return errors.New("invalid old applied preset")
	}
	if err := state.rawDuplicates(node); err != nil {
		return err
	}
	name, baseline := v5StoredField(node, "name"), v5StoredField(node, "baseline")
	if name == nil || name.Kind != yaml.ScalarNode || name.Tag != "!!str" || baseline == nil {
		return errors.New("invalid old applied preset fields")
	}
	canonical, err := NormalizePresetName(name.Value)
	if err != nil || canonical != name.Value {
		return errors.New("invalid old applied preset name")
	}
	version, err := upgradeStoredVersion(baseline)
	if err != nil || version == CurrentConfigurationVersion {
		return errors.New("invalid old applied preset baseline version")
	}
	if err := validateLegacyOwner(baseline, version, state, true); err != nil {
		return err
	}
	return admitLegacyPresetNode(baseline)
}

func admitLegacyPresetNode(node *yaml.Node) error {
	// Physical/work admission has already bounded this tree. Measure only after
	// recognition, without allocating a retired filter or expanding alias values.
	data, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	if len(data) > MaxPresetBytes {
		return storageAdmissionf("old saved preset exceeds %d bytes", MaxPresetBytes)
	}
	return nil
}

func decodeLegacyLogin(node *yaml.Node, version int, state *storageDecodeState, defaults *PreparedRuleSet) (*UserRecord, error) {
	if err := validateLegacyOwner(node, version, state, false); err != nil {
		return nil, err
	}
	var value legacyLoginPreferences
	guard := newStorageDecodeGuard(false)
	typ := reflect.TypeFor[legacyLoginPreferences]()
	if _, _, err := state.decode(node, typ, reflect.Value{}, guard); err != nil {
		return nil, err
	}
	if len(guard.typeErrors) != 0 {
		return nil, &yaml.TypeError{Errors: guard.typeErrors}
	}
	state.materialize = true
	defer func() { state.materialize = false }()
	if _, _, err := state.decode(node, typ, reflect.ValueOf(&value).Elem(), newStorageDecodeGuard(false)); err != nil {
		return nil, err
	}
	if value.ConfigurationVersion != version {
		return nil, ErrUnsupportedConfigurationVersion
	}
	if version == 0 {
		if strings.TrimSpace(value.Dialect) == "" {
			value.Dialect = "go"
		}
		value.DedupePolicy = NormalizeDedupePolicy(value.DedupePolicy)
		value.Grid = strings.ToUpper(strings.TrimSpace(value.Grid))
		value.NoiseClass = strings.ToUpper(strings.TrimSpace(value.NoiseClass))
		if value.PathMinObservationCount < 0 {
			value.PathMinObservationCount = 0
		}
		switch value.SolarSummaryMinutes {
		case 15, 30, 60:
		default:
			value.SolarSummaryMinutes = 0
		}
	}
	return &UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion,
		Filter: Filter{preparedRules: defaults,
			IncludeBeacons: value.IncludeBeacons, AllowWWV: value.AllowWWV,
			AllowWCY: value.AllowWCY, AllowAnnounce: value.AllowAnnounce,
			AllowSelf: value.AllowSelf, AllowToxic: value.AllowToxic},
		Dialect: value.Dialect, Grid: value.Grid, NoiseClass: value.NoiseClass,
		DedupePolicy: value.DedupePolicy, PathMinObservationCount: value.PathMinObservationCount,
		SolarSummaryMinutes: value.SolarSummaryMinutes,
		RecentIPs:           trimRecentIPs(value.RecentIPs), LastLoginUTC: value.LastLoginUTC,
	}, nil
}
