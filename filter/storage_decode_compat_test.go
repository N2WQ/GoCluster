package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/strutil"
	"gopkg.in/yaml.v3"
)

// This is the pre-04a typed-decoding oracle frozen at 745a722. Its record,
// reference and preset callbacks each retain their own stock Node.Decode, so
// candidate nested callbacks cannot become their own compatibility oracle.
// The unchanged version/prevalidation, migration, configuration-validation,
// size-check and IP-trimming helpers are deliberately shared. If those seams
// change, review this oracle before treating its agreement as independent proof.
type frozenUserRecord struct {
	ConfigurationVersion    int `yaml:"configuration_version,omitempty"`
	Filter                  `yaml:",inline"`
	Preset                  *frozenPresetReference `yaml:"preset,omitempty"`
	RecentIPs               []string               `yaml:"recent_ips,omitempty"`
	Dialect                 string                 `yaml:"dialect,omitempty"`
	DedupePolicy            string                 `yaml:"dedupe_policy,omitempty"`
	LastLoginUTC            time.Time              `yaml:"last_login_utc,omitempty"`
	Grid                    string                 `yaml:"grid,omitempty"`
	NoiseClass              string                 `yaml:"noise_class,omitempty"`
	PathMinObservationCount int                    `yaml:"path_min_observation_count,omitempty"`
	SolarSummaryMinutes     int                    `yaml:"solar_summary_minutes,omitempty"`
}

type frozenSavedPreset SavedPreset

type frozenPresetReference struct {
	Name     string             `yaml:"name"`
	Baseline *frozenSavedPreset `yaml:"baseline"`
}

func frozenStoredVersion(node *yaml.Node, typ reflect.Type, strictLegacy bool) (int, error) {
	version, err := storedConfigurationVersion(node)
	if err != nil {
		return 0, err
	}
	if err := validateStoredStateFields(node, version); err != nil {
		return 0, err
	}
	if err := validateStoredMinSNRFields(node, version); err != nil {
		return 0, err
	}
	if err := validateStoredCommentFields(node, version); err != nil {
		return 0, err
	}
	if strictLegacy || version != 0 {
		if err := validateStoredMapping(node, typ); err != nil {
			return 0, err
		}
	}
	if version != 0 {
		if err := validateCurrentStoredValues(node, typ); err != nil {
			return 0, err
		}
	}
	return version, nil
}

func frozenFinishFilter(f *Filter, version int) error {
	if version < stateConfigurationVersion {
		f.ResetDXStates()
		f.ResetDEStates()
	}
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{})
	if err := cfg.ValidateStateRules(); err != nil {
		return err
	}
	if err := cfg.ValidateMinSNRRules(); err != nil {
		return err
	}
	return cfg.ValidateCommentRules()
}

func (record *frozenUserRecord) UnmarshalYAML(node *yaml.Node) error {
	version, err := frozenStoredVersion(node, reflect.TypeFor[UserRecord](), false)
	if err != nil {
		return err
	}
	type plain frozenUserRecord
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	if decoded.ConfigurationVersion != version {
		return fmt.Errorf("%w: marker must be an explicit field", ErrUnsupportedConfigurationVersion)
	}
	*record = frozenUserRecord(decoded)
	if version == 0 {
		record.migrateLegacyConfidence()
		record.normalizeDefaults()
		if strings.TrimSpace(record.Dialect) == "" {
			record.Dialect = "go"
		}
		record.DedupePolicy = NormalizeDedupePolicy(record.DedupePolicy)
		record.Grid = strutil.NormalizeUpper(record.Grid)
		record.NoiseClass = strutil.NormalizeUpper(record.NoiseClass)
		record.PathMinObservationCount = normalizePathMinObservationCount(record.PathMinObservationCount)
		record.SolarSummaryMinutes = normalizeSolarSummaryMinutes(record.SolarSummaryMinutes)
	}
	if err := frozenFinishFilter(&record.Filter, version); err != nil {
		return err
	}
	record.ConfigurationVersion = CurrentConfigurationVersion
	return nil
}

func (set *frozenSavedPreset) UnmarshalYAML(node *yaml.Node) error {
	version, err := frozenStoredVersion(node, reflect.TypeFor[SavedPreset](), true)
	if err != nil {
		return err
	}
	type plain frozenSavedPreset
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	if decoded.ConfigurationVersion != version {
		return fmt.Errorf("%w: marker must be an explicit field", ErrUnsupportedConfigurationVersion)
	}
	*set = frozenSavedPreset(decoded)
	if version == 0 {
		set.migrateLegacyConfidence()
		set.normalizeDefaults()
		if strings.TrimSpace(set.Dialect) == "" {
			set.Dialect = "go"
		}
		set.DedupePolicy = NormalizeDedupePolicy(set.DedupePolicy)
		set.Grid = strutil.NormalizeUpper(set.Grid)
		set.NoiseClass = strutil.NormalizeUpper(set.NoiseClass)
		set.PathMinObservationCount = normalizePathMinObservationCount(set.PathMinObservationCount)
		set.SolarSummaryMinutes = normalizeSolarSummaryMinutes(set.SolarSummaryMinutes)
	}
	if err := frozenFinishFilter(&set.Filter, version); err != nil {
		return err
	}
	set.ConfigurationVersion = CurrentConfigurationVersion
	return nil
}

func (ref *frozenPresetReference) UnmarshalYAML(node *yaml.Node) error {
	if err := validateStoredMapping(node, reflect.TypeFor[PresetReference]()); err != nil {
		return err
	}
	if err := validateCurrentStoredValues(node, reflect.TypeFor[PresetReference]()); err != nil {
		return err
	}
	type plain frozenPresetReference
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	name, err := NormalizePresetName(decoded.Name)
	if err != nil || name != decoded.Name {
		return errors.New("invalid applied preset name")
	}
	if decoded.Baseline == nil {
		return errors.New("invalid applied preset baseline: nil saved preset")
	}
	baseline := SavedPreset(*decoded.Baseline)
	if baseline.ConfigurationVersion != 0 && baseline.ConfigurationVersion != CurrentConfigurationVersion {
		return fmt.Errorf("invalid applied preset baseline: %w", ErrUnsupportedConfigurationVersion)
	}
	detached, err := ConfigurationFromPreset(&baseline).Preset()
	if err != nil {
		return fmt.Errorf("invalid applied preset baseline: %w", err)
	}
	frozen := frozenSavedPreset(*detached)
	*ref = frozenPresetReference{Name: name, Baseline: &frozen}
	return nil
}

func (record *frozenUserRecord) productionValue() *UserRecord {
	result := &UserRecord{
		ConfigurationVersion: record.ConfigurationVersion, Filter: record.Filter,
		RecentIPs: record.RecentIPs, Dialect: record.Dialect, DedupePolicy: record.DedupePolicy,
		LastLoginUTC: record.LastLoginUTC, Grid: record.Grid, NoiseClass: record.NoiseClass,
		PathMinObservationCount: record.PathMinObservationCount, SolarSummaryMinutes: record.SolarSummaryMinutes,
	}
	if record.Preset != nil {
		baseline := SavedPreset(*record.Preset.Baseline)
		result.Preset = &PresetReference{Name: record.Preset.Name, Baseline: &baseline}
	}
	return result
}

func frozenDecodeUserRecord(raw []byte) (*UserRecord, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if len(document.Content) != 1 {
		return nil, errors.New("user record must contain one YAML mapping")
	}
	var record frozenUserRecord
	if err := record.UnmarshalYAML(document.Content[0]); err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("user record must contain one YAML document")
	}
	record.RecentIPs = trimRecentIPs(record.RecentIPs)
	return record.productionValue(), nil
}

func frozenDecodePresetCollection(raw []byte) (map[string]*SavedPreset, error) {
	if len(raw) > maxPresetCollectionBytes {
		return nil, errors.New("preset collection exceeds 8 MiB")
	}
	var collection struct {
		Presets map[string]*frozenSavedPreset `yaml:"presets"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&collection); err != nil {
		return nil, fmt.Errorf("invalid preset collection: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("preset collection must contain one YAML document")
	}
	if collection.Presets == nil || len(collection.Presets) > MaxPresets {
		return nil, errors.New("invalid preset collection count")
	}
	result := make(map[string]*SavedPreset, len(collection.Presets))
	for name, set := range collection.Presets {
		canonical, err := NormalizePresetName(name)
		if err != nil || name != canonical {
			return nil, fmt.Errorf("invalid stored preset name %q", name)
		}
		var value *SavedPreset
		if set != nil {
			converted := SavedPreset(*set)
			value = &converted
		}
		if err := checkPresetSize(value); err != nil {
			return nil, fmt.Errorf("invalid stored preset %s: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

type storageCompatCase struct {
	name      string
	raw       string
	wantError string
}

const frozenAliasTrap = "a: &a {x: 0}\nb: &b {<<: [*a,*a,*a,*a,*a,*a,*a]}\nc: &c {<<: [*b,*b,*b,*b,*b,*b,*b]}\nd: &d {<<: [*c,*c,*c,*c,*c,*c,*c]}\n<<: *d\n"
const frozenAliasUnused = "a: &a {x: 0}\nb: &b {<<: [*a,*a,*a,*a,*a,*a,*a]}\nc: &c {<<: [*b,*b,*b,*b,*b,*b,*b]}\nd: &d {<<: [*c,*c,*c,*c,*c,*c,*c]}\n"
const frozenAliasAccepted = "a: &a {x: 0}\nb: &b {<<: [*a,*a,*a,*a,*a,*a]}\nc: &c {<<: [*b,*b,*b,*b,*b,*b]}\nd: &d {<<: [*c,*c,*c,*c,*c,*c]}\n<<: *d\n"

func frozenAliasedList(padding int) string {
	var raw strings.Builder
	fmt.Fprintf(&raw, "legacy: &list [%s]\n", strings.TrimSuffix(strings.Repeat("K1ABC,", 2000), ","))
	for i := 0; i < padding; i++ {
		fmt.Fprintf(&raw, "ignored%d: false\n", i)
	}
	raw.WriteString("callsigns: *list\n")
	return raw.String()
}

func frozenRecordCases() []storageCompatCase {
	return []storageCompatCase{
		{"empty mapping", "{}\n", ""},
		{"empty document", "", "EOF"},
		{"multiple documents", "{}\n---\n{}\n", "one YAML document"},
		{"null root", "null\n", "must be a mapping"},
		{"sequence root", "[]\n", "must be a mapping"},
		{"excessive reachable merges", frozenAliasTrap, "yaml: document contains excessive aliasing"},
		{"same unused anchors", frozenAliasUnused, ""},
		{"accepted guard neighbor", frozenAliasAccepted, ""},
		{"padded aliased list accepted", frozenAliasedList(100), ""},
		{"unpadded aliased list refused", frozenAliasedList(0), "yaml: document contains excessive aliasing"},
		{"ignored root duplicate", "legacy: 1\nlegacy: 2\n", "mapping key"},
		{"ignored nested duplicate", "legacy: {x: 1, x: 2}\n", ""},
		{"raw duplicate different tags", "bands: {1: true, \"1\": false}\n", "mapping key"},
		{"legacy interpreted integer duplicate", "dxzones: {1: true, 01: false}\n", ""},
		{"marked interpreted integer duplicate", "configuration_version: 4\ndxzones: {1: true, 01: false}\n", "duplicate interpreted rule key"},
		{"legacy numeric scalar conversion", "dxzones: {1: true, 1.0: false}\n", ""},
		{"legacy float parent merge key", "dxzones: {1.0: true, <<: {1: false}}\n", ""},
		{"legacy integer parent merge key", "dxzones: {1: true, <<: {1.0: false}}\n", ""},
		{"legacy bool scalar conversion", "bands: {20m: yes, 40m: off}\n", ""},
		{"legacy mode and confidence migrations", "modes: {PSK31: true, PSK63: false, CW: true}\nblockmodes: {UNKNOWN: true}\nminconfidence: 80\nevents: {sota: true, unsupported: true}\n", ""},
		{"marked bool scalar conversion refused", "configuration_version: 4\nbands: {20m: yes}\n", "expected true or false"},
		{"explicit merge wins", "<<: {bands: {40m: true}}\nbands: {20m: true}\n", ""},
		{"first sequence merge wins", "<<: [{bands: {20m: true}}, {bands: {40m: true}}]\n", ""},
		{"nested merge", "legacy: &a {bands: {20m: true}}\n<<: {<<: *a}\n", ""},
		{"map merge", "legacy: &a {20m: true, 40m: true}\nbands: {<<: *a, 40m: false}\n", ""},
		{"skipped malformed merged value", "bands: {20m: true}\n<<: {bands: 42}\n", ""},
		{"duplicate skipped merge source", "bands: {20m: true}\n<<: {bands: 42, bands: 43}\n", "mapping key"},
		{"unused cycle", "legacy: &a {x: *a}\n", ""},
		{"reachable cycle", "&a\n<<: *a\n", "value contains itself"},
		{"quoted merge key", "\"<<\": {x: 1, x: 2}\nbands: {20m: true}\n", ""},
		{"aliased merge key", "key: &key <<\n? *key\n: {x: 1, x: 2}\nbands: {20m: true}\n", ""},
		{"aliased struct field duplicate", "key: &key bands\nbands: {20m: true}\n? *key\n: {40m: true}\n", "field bands already set"},
		{"legacy nulls", "bands: null\ndialect: null\nallow_wwv: null\ncallsigns: null\n", ""},
		{"legacy map null", "bands: {20m: null}\n", ""},
		{"legacy string map null key", "bands: {null: true}\n", ""},
		{"legacy integer map null key", "dxzones: {null: true}\n", ""},
		{"legacy timestamp unknown mapping", "last_login_utc: {ignored: {x: 1, x: 2}}\n", ""},
		{"legacy timestamp duplicate mapping", "last_login_utc: {ignored: 1, ignored: 2}\n", "mapping key"},
		{"marked null", "configuration_version: 4\nbands: null\n", "null is not a configured value"},
		{"legacy unknown", "legacy: {configuration_version: 99}\nbands: {20m: true}\n", ""},
		{"marked unknown", "configuration_version: 4\nlegacy: true\n", "unknown stored configuration field"},
		{"merged marker", "<<: {configuration_version: 4}\n", "marker must be an explicit field"},
		{"future marker", "configuration_version: 5\n", "unsupported configuration version"},
		{"zero marker", "configuration_version: 0\n", "unsupported configuration version"},
		{"null marker", "configuration_version: null\n", "unsupported configuration version"},
		{"legacy future state", "<<: {dxstates: {CA: false}}\n", "state field dxstates requires"},
		{"legacy future SNR", "<<: {min_snr: {CW: -5}}\n", "min_snr requires"},
		{"legacy future comments", "<<: {comments: []}\n", "comments requires"},
		{"null reference", "preset: null\n", ""},
		{"null baseline", "preset: {name: TEST, baseline: null}\n", "null is not a configured value"},
		{"baseline root merge refused", "preset: {name: TEST, baseline: {<<: {bands: {20m: true}}}}\n", "unknown stored configuration field"},
		{"baseline unknown refused", "preset: {name: TEST, baseline: {legacy: true}}\n", "unknown stored configuration field"},
		{"baseline future marker", "preset: {name: TEST, baseline: {configuration_version: 5}}\n", "unsupported configuration version"},
	}
}

func frozenVersionBody(version int) string {
	var body strings.Builder
	if version != 0 {
		fmt.Fprintf(&body, "configuration_version: %d\n", version)
	}
	body.WriteString("bands: {20m: true, 40m: false}\nallbands: false\nblockbands: {80m: true}\nmodes: {CW: true, FT8: false}\nallmodes: false\nblockmodes: {RTTY: true}\nsources: {HUMAN: true, SKIMMER: false}\nallevents: false\nevents: {SOTA: false}\ncallsigns: [K1*, W1ABC, K1*]\nblock_callsigns: [W2*]\ndecallsigns: ['*ABC']\nblock_decallsigns: [W3ABC]\ndxzones: {1: true, 5: false}\ndezones: {3: false}\ndxgrid2prefixes: {FN: true}\ndxcontinents: {NA: true}\ndedxcc: {291: false}\ninclude_beacons: false\nallow_wwv: false\nallow_wcy: true\nallow_announce: true\nallow_self: false\nallow_toxic: false\nnearby_enabled: true\ndialect: cc\ndedupe_policy: MED\ngrid: FN31\nnoise_class: URBAN\npath_min_observation_count: 0\nsolar_summary_minutes: 15\n")
	if version >= 2 {
		body.WriteString("dxstates: {CA: true, NY: false}\nblockdestates: {ON: false}\nalldxstates: false\n")
	}
	if version >= 3 {
		body.WriteString("min_snr: {CW: -5, FT8: 0}\n")
	}
	if version >= 4 {
		body.WriteString("comments: ['CQ TEST', 'CQ TEST']\nblock_comments: ['bad!']\n")
	}
	return body.String()
}

func frozenIndent(raw string) string {
	return "    " + strings.ReplaceAll(strings.TrimSuffix(raw, "\n"), "\n", "\n    ") + "\n"
}

func assertStorageCompatError(t *testing.T, wantSubstring string, frozenErr, actualErr error) {
	t.Helper()
	if wantSubstring == "" {
		if frozenErr != nil || actualErr != nil {
			t.Fatalf("accepted fixture refused: frozen=%v actual=%v", frozenErr, actualErr)
		}
		return
	}
	if frozenErr == nil || actualErr == nil || !strings.Contains(frozenErr.Error(), wantSubstring) || !strings.Contains(actualErr.Error(), wantSubstring) {
		t.Fatalf("expected refusal %q: frozen=%v actual=%v", wantSubstring, frozenErr, actualErr)
	}
	if errors.Is(frozenErr, ErrUnsupportedConfigurationVersion) != errors.Is(actualErr, ErrUnsupportedConfigurationVersion) {
		t.Fatalf("unsupported-version classification changed: frozen=%v actual=%v", frozenErr, actualErr)
	}
}

func assertUserRecordCompat(t *testing.T, fixture storageCompatCase) {
	t.Helper()
	raw := []byte(fixture.raw)
	want, frozenErr := frozenDecodeUserRecord(raw)
	path := userRecordPath("K1ABC-1")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	got, actualErr := LoadUserRecord("K1ABC-1")
	assertStorageCompatError(t, fixture.wantError, frozenErr, actualErr)
	if actualErr == nil && !reflect.DeepEqual(got, want) {
		t.Fatalf("complete record differs:\nactual=%#v\nfrozen=%#v", got, want)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, raw) {
		t.Fatalf("load changed stored bytes: %v", err)
	}
}

func TestStorageDecodeUserRecordCompatibility(t *testing.T) {
	usePresetTestDir(t)
	if len(frozenAliasTrap) != 125 || len(frozenAliasUnused) != 118 || len(frozenAliasAccepted) != 116 {
		t.Fatal("pinned alias fixtures changed")
	}
	for _, fixture := range frozenRecordCases() {
		t.Run(fixture.name, func(t *testing.T) { assertUserRecordCompat(t, fixture) })
	}
	t.Run("aliased baseline", func(t *testing.T) {
		raw := "legacy: &baseline {bands: {20m: true}, allbands: false}\npreset: {name: TEST, baseline: *baseline}\n"
		assertUserRecordCompat(t, storageCompatCase{raw: raw})
	})
	t.Run("parent padding cannot relax baseline guard", func(t *testing.T) {
		var raw strings.Builder
		raw.WriteString(frozenAliasUnused)
		for i := 0; i < 100; i++ {
			fmt.Fprintf(&raw, "ignored%d: false\n", i)
		}
		raw.WriteString("preset: {name: TEST, baseline: {bands: *d}}\n")
		assertUserRecordCompat(t, storageCompatCase{raw: raw.String(), wantError: "yaml: document contains excessive aliasing"})
	})
	for version := 0; version <= 4; version++ {
		t.Run(fmt.Sprintf("record and baseline version %d", version), func(t *testing.T) {
			raw := frozenVersionBody(version) + "recent_ips: [192.0.2.1, 192.0.2.2, 192.0.2.3, 192.0.2.4, 192.0.2.5, 192.0.2.6]\nlast_login_utc: 2026-10-08T12:34:56Z\npreset:\n  name: TEST\n  baseline:\n" + frozenIndent(frozenVersionBody(version))
			assertUserRecordCompat(t, storageCompatCase{raw: raw})
		})
	}
	for _, versions := range [][2]int{{0, 4}, {4, 0}, {1, 4}, {4, 1}} {
		t.Run(fmt.Sprintf("record %d baseline %d", versions[0], versions[1]), func(t *testing.T) {
			raw := frozenVersionBody(versions[0]) + "preset:\n  name: TEST\n  baseline:\n" + frozenIndent(frozenVersionBody(versions[1]))
			assertUserRecordCompat(t, storageCompatCase{raw: raw})
		})
	}
}

func TestStorageDecodePresetCollectionCompatibility(t *testing.T) {
	fixtures := []storageCompatCase{
		{"empty collection", "presets: {}\n", ""},
		{"empty mapping", "{}\n", "invalid preset collection count"},
		{"null collection", "presets: null\n", "invalid preset collection count"},
		{"null entry", "presets: {TEST: null}\n", "nil saved preset"},
		{"root merge", "<<: {presets: {TEST: {bands: {20m: true}}}}\n", ""},
		{"duplicate entries", "presets: {TEST: {}, TEST: {}}\n", "mapping key"},
		{"unknown collection field", "presets: {}\nlegacy: {x: 1, x: 2}\n", "field legacy not found"},
		{"unknown legacy preset field", "presets: {TEST: {legacy: true}}\n", "unknown stored configuration field"},
		{"legacy preset root merge", "presets: {TEST: {<<: {bands: {20m: true}}}}\n", "unknown stored configuration field"},
		{"legacy preset map merge", "presets: {TEST: {bands: {<<: {20m: true}, 40m: false}}}\n", ""},
		{"marked preset map merge", "presets: {TEST: {configuration_version: 4, bands: {<<: {20m: true}}}}\n", "expected a string"},
		{"legacy preset null", "presets: {TEST: {bands: null}}\n", ""},
		{"marked preset null", "presets: {TEST: {configuration_version: 4, bands: null}}\n", "null is not a configured value"},
		{"multiple documents", "presets: {}\n---\n{}\n", "one YAML document"},
		{"invalid name", "presets: {lower: {}}\n", "invalid stored preset name"},
	}
	for version := 0; version <= 4; version++ {
		fixtures = append(fixtures, storageCompatCase{name: fmt.Sprintf("preset version %d", version), raw: "presets:\n  TEST:\n" + frozenIndent(frozenVersionBody(version))})
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			raw := []byte(fixture.raw)
			want, frozenErr := frozenDecodePresetCollection(raw)
			path := filepath.Join(t.TempDir(), "collection.yaml")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			got, actualErr := readPresetCollection(path)
			assertStorageCompatError(t, fixture.wantError, frozenErr, actualErr)
			if actualErr == nil && !reflect.DeepEqual(got.Presets, want) {
				t.Fatalf("complete collection differs:\nactual=%#v\nfrozen=%#v", got.Presets, want)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, raw) {
				t.Fatalf("load changed collection bytes: %v", err)
			}
		})
	}
}

func TestStorageDecodeNewThresholdsInactive(t *testing.T) {
	usePresetTestDir(t)
	// These controls apply to 04a. Quota activation in 04d replaces their
	// acceptance assertions with the separately approved refusal assertions.
	const futureRecordBytes = 1 << 20
	const futureWorkUnits = 2 << 20
	raw := "legacy: \"" + strings.Repeat("X", futureRecordBytes+1) + "\"\n"
	if len(raw) <= futureRecordBytes {
		t.Fatal("raw-limit control does not exceed the future limit")
	}
	t.Run("raw bytes", func(t *testing.T) {
		assertUserRecordCompat(t, storageCompatCase{raw: raw})
	})
	const scalarBytes = 32768
	const occurrences = 70
	// The old plain-record decoder visits root, two keys, a sequence and
	// two nodes per occurrence (alias plus scalar): 144 visits, below 1000.
	// Scalar bytes alone exceed the future work quota without a large map.
	if 4+2*occurrences > 1000 || scalarBytes*occurrences <= futureWorkUnits {
		t.Fatal("logical-work control does not separate the old and future guards")
	}
	workRaw := "legacy: &text " + strings.Repeat("K", scalarBytes) + "\nrecent_ips: [" + strings.TrimSuffix(strings.Repeat("*text,", occurrences), ",") + "]\n"
	if len(workRaw) >= futureRecordBytes {
		t.Fatal("logical-work control also exceeds the raw limit")
	}
	t.Run("logical scalar bytes", func(t *testing.T) {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(workRaw), &document); err != nil {
			t.Fatal(err)
		}
		root := document.Content[0]
		if len(root.Content) != 4 || root.Content[2].Value != "recent_ips" || root.Content[3].Kind != yaml.SequenceNode || len(root.Content[3].Content) != occurrences {
			t.Fatal("logical-work input does not contain seventy recent-IP aliases")
		}
		for _, alias := range root.Content[3].Content {
			if alias.Kind != yaml.AliasNode || alias.Alias != root.Content[1] || len(alias.Alias.Value) != scalarBytes {
				t.Fatal("logical-work occurrence does not resolve to the intended scalar")
			}
		}
		assertUserRecordCompat(t, storageCompatCase{raw: workRaw})
		record, err := frozenDecodeUserRecord([]byte(workRaw))
		if err != nil || len(record.RecentIPs) != 5 {
			t.Fatalf("old trimming did not retain five recent IPs: %v", err)
		}
		for _, retained := range record.RecentIPs {
			if len(retained) != scalarBytes {
				t.Fatal("retained IP did not contain the intended scalar bytes")
			}
		}
		canonical, err := yaml.Marshal(record)
		if err != nil || len(canonical) >= futureRecordBytes {
			t.Fatalf("logical-work control exceeds the future canonical-record limit: bytes=%d err=%v", len(canonical), err)
		}
		preset, err := ConfigurationFromFilter(&record.Filter, SettingsConfiguration{
			Dialect: record.Dialect, DedupePolicy: record.DedupePolicy,
		}).Preset()
		if err != nil {
			t.Fatalf("logical-work control exceeds the existing configuration-snapshot limit: %v", err)
		}
		if err := checkPresetSize(preset); err != nil {
			t.Fatal(err)
		}
	})
}
