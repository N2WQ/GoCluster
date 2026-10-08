package telnet

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
)

const machineCompleteSettingsFixture = `dialect: GO
grid: ""
noise_class: QUIET
dedupe_policy: FAST
path_min_observation_count: 0
solar_summary_minutes: 0
`

const machineCompleteFilterFixture = `bands: {allow_all: false, block_all: false, allow: {20m: false}, block: {40m: true}}
modes: {allow_all: false, block_all: true, allow: {CW: true, USB: false}, block: {UNKNOWN: false}}
sources: {allow_all: false, block_all: false, allow: {}, block: {}}
events: {allow_all: false, block_all: false, allow: {}, block: {}}
confidence: {allow_all: false, block_all: false, allow: {}, block: {}}
path_classes: {allow_all: false, block_all: false, allow: {}, block: {}}
dx_continents: {allow_all: false, block_all: false, allow: {}, block: {}}
de_continents: {allow_all: false, block_all: false, allow: {}, block: {}}
dx_zones: {allow_all: false, block_all: false, allow: {1: false, 40: true}, block: {}}
de_zones: {allow_all: false, block_all: false, allow: {}, block: {}}
dx_grid2: {allow_all: false, block_all: false, allow: {}, block: {}}
de_grid2: {allow_all: false, block_all: false, allow: {}, block: {}}
dx_dxcc: {allow_all: false, block_all: false, allow: {}, block: {}}
de_dxcc: {allow_all: false, block_all: false, allow: {}, block: {}}
dx_callsigns: ["W1*", "K1?", "W1*"]
block_dx_callsigns: []
de_callsigns: ["DL1ABC"]
block_de_callsigns: ["K2BAD"]
include_beacons: false
allow_wwv: true
allow_wcy: DEFAULT
allow_announce: false
allow_self: DEFAULT
allow_toxic: true
nearby_enabled: false
`

func machineRequestFixture(configuration string, revision bool) []byte {
	header := "schema_version: 1\nrequest_id: noise-Ab1\n"
	if revision {
		header += "if_revision: opaque-revision-1\n"
	}
	return []byte(header + "configuration:\n" + indentMachineFixture(configuration))
}

func indentMachineFixture(value string) string {
	return "  " + strings.ReplaceAll(strings.TrimSuffix(value, "\n"), "\n", "\n  ") + "\n"
}

func TestMachineSchemaCompleteSETTINGS(t *testing.T) {
	request, err := decodeMachineRequest(machineRequestFixture(machineCompleteSettingsFixture, true), machineCommand{Verb: "PUT", Resource: "SETTINGS"})
	if err != nil {
		t.Fatal(err)
	}
	if request.SchemaVersion != 1 || request.RequestID != "noise-Ab1" || request.IfRevision != "opaque-revision-1" {
		t.Fatalf("envelope changed: %+v", request)
	}
	before := filter.Configuration{Filters: filter.FilterConfiguration{DXCallsigns: []string{"K1*"}}, Settings: filter.SettingsConfiguration{Grid: "FN42", NoiseClass: "URBAN", SolarSummaryMinutes: 30}}
	got, err := request.apply(before)
	if err != nil {
		t.Fatal(err)
	}
	want := filter.SettingsConfiguration{Dialect: "GO", Grid: "", NoiseClass: "QUIET", DedupePolicy: "FAST", PathMinObservationCount: 0, SolarSummaryMinutes: 0}
	if got.Settings != want || !reflect.DeepEqual(got.Filters.DXCallsigns, []string{"K1*"}) {
		t.Fatalf("replacement changed exact values or other resource: %+v", got)
	}
	if before.Settings.NoiseClass != "URBAN" || before.Settings.SolarSummaryMinutes != 30 {
		t.Fatal("apply mutated its input")
	}
}

func TestMachineSchemaCompleteFILTERPreservesExactRules(t *testing.T) {
	request, err := decodeMachineRequest(machineRequestFixture(machineCompleteFilterFixture, true), machineCommand{Verb: "PUT", Resource: "FILTER"})
	if err != nil {
		t.Fatal(err)
	}
	before := filter.Configuration{Settings: filter.SettingsConfiguration{NoiseClass: "URBAN"}}
	got, err := request.apply(before)
	if err != nil {
		t.Fatal(err)
	}
	f := got.Filters
	if f.Bands.AllowAll || f.Bands.BlockAll || !reflect.DeepEqual(f.Bands.Allow, map[string]bool{"20m": false}) || !reflect.DeepEqual(f.Bands.Block, map[string]bool{"40m": true}) {
		t.Fatalf("explicit false band rules changed: %+v", f.Bands)
	}
	if f.Modes.AllowAll || !f.Modes.BlockAll || !reflect.DeepEqual(f.Modes.Allow, map[string]bool{"CW": true, "USB": false}) || !reflect.DeepEqual(f.Modes.Block, map[string]bool{"UNKNOWN": false}) {
		t.Fatalf("mode values normalized: %+v", f.Modes)
	}
	if !reflect.DeepEqual(f.DXZones.Allow, map[int]bool{1: false, 40: true}) || f.DXZones.AllowAll || f.DXZones.BlockAll {
		t.Fatalf("integer selections changed: %+v", f.DXZones)
	}
	if !reflect.DeepEqual(f.DXCallsigns, []string{"W1*", "K1?", "W1*"}) || !reflect.DeepEqual(f.DECallsigns, []string{"DL1ABC"}) || !reflect.DeepEqual(f.BlockDECallsigns, []string{"K2BAD"}) {
		t.Fatalf("pattern order or duplicate multiplicity changed: %+v", f)
	}
	if f.IncludeBeacons != filter.DefaultBoolFalse || f.AllowWWV != filter.DefaultBoolTrue || f.AllowWCY != filter.DefaultBoolDefault || f.AllowAnnounce != filter.DefaultBoolFalse || f.AllowSelf != filter.DefaultBoolDefault || f.AllowToxic != filter.DefaultBoolTrue {
		t.Fatalf("default/explicit selections changed: %+v", f)
	}
	if f.NearbyEnabled || got.Settings.NoiseClass != "URBAN" {
		t.Fatal("filter replacement changed omitted settings or explicit NEARBY false")
	}
}

func TestMachineSchemaPATCHMembersReplaceOnlySuppliedCollections(t *testing.T) {
	configuration := `filters:
  bands:
    allow: {20m: false}
  dx_callsigns: ["W1*", "K1?", "W1*"]
  nearby_enabled: false
settings:
  noise_class: URBAN
`
	request, err := decodeMachineRequest(machineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: "CONFIG"})
	if err != nil {
		t.Fatal(err)
	}
	before := filter.Configuration{Filters: filter.FilterConfiguration{
		Bands:       filter.StringRules{AllowAll: true, BlockAll: true, Allow: map[string]bool{"40m": true}, Block: map[string]bool{"80m": true}},
		DXCallsigns: []string{"K2*"}, NearbyEnabled: true, AllowWWV: filter.DefaultBoolDefault,
	}, Settings: filter.SettingsConfiguration{Dialect: "CC", Grid: "FN42", NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 30}}
	got, err := request.apply(before)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Filters.Bands.Allow, map[string]bool{"20m": false}) || !got.Filters.Bands.AllowAll || !got.Filters.Bands.BlockAll || !reflect.DeepEqual(got.Filters.Bands.Block, map[string]bool{"80m": true}) {
		t.Fatalf("partial RuleSet merged or reset omitted fields: %+v", got.Filters.Bands)
	}
	if !reflect.DeepEqual(got.Filters.DXCallsigns, []string{"W1*", "K1?", "W1*"}) || got.Filters.NearbyEnabled || got.Settings.NoiseClass != "URBAN" || got.Settings.Grid != "FN42" || got.Settings.SolarSummaryMinutes != 30 {
		t.Fatalf("partial configuration changed omissions: %+v", got)
	}
	got.Filters.Bands.Allow["20m"] = true
	got.Filters.DXCallsigns[0] = "CHANGED"
	if !reflect.DeepEqual(before.Filters.Bands.Allow, map[string]bool{"40m": true}) || !reflect.DeepEqual(before.Filters.DXCallsigns, []string{"K2*"}) || before.Settings.NoiseClass != "QUIET" || !before.Filters.NearbyEnabled {
		t.Fatal("supplied collections borrowed or mutated previous configuration")
	}
}

func TestMachineSchemaPATCHExplicitEmptyCollections(t *testing.T) {
	request, err := decodeMachineRequest(machineRequestFixture("bands: {allow: {}}\ndx_callsigns: []\n", true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil {
		t.Fatal(err)
	}
	before := filter.Configuration{Filters: filter.FilterConfiguration{Bands: filter.StringRules{Allow: map[string]bool{"20m": true}, AllowAll: false}, DXCallsigns: []string{"W1*"}}}
	got, err := request.apply(before)
	if err != nil || len(got.Filters.Bands.Allow) != 0 || len(got.Filters.DXCallsigns) != 0 || got.Filters.Bands.AllowAll {
		t.Fatalf("empty collections treated as omission or normalized: %+v err=%v", got, err)
	}
}

func TestMachineSchemaPATCHDistinctCategoryTargets(t *testing.T) {
	configuration := `bands: {allow: {20m: false}}
modes: {allow: {CW: true}}
sources: {allow: {HUMAN: false}}
events: {allow: {SOTA: true}}
confidence: {allow: {"?": false}}
path_classes: {allow: {HIGH: true}}
dx_continents: {allow: {EU: false}}
de_continents: {allow: {AS: true}}
dx_zones: {allow: {1: false}}
de_zones: {allow: {2: true}}
dx_grid2: {allow: {FN: false}}
de_grid2: {allow: {JN: true}}
dx_dxcc: {allow: {291: false}}
de_dxcc: {allow: {230: true}}
`
	request, err := decodeMachineRequest(machineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := request.apply(filter.Configuration{})
	want := filter.FilterConfiguration{
		Bands: filter.StringRules{Allow: map[string]bool{"20m": false}}, Modes: filter.StringRules{Allow: map[string]bool{"CW": true}},
		Sources: filter.StringRules{Allow: map[string]bool{"HUMAN": false}}, Events: filter.StringRules{Allow: map[string]bool{"SOTA": true}},
		Confidence: filter.StringRules{Allow: map[string]bool{"?": false}}, PathClasses: filter.StringRules{Allow: map[string]bool{"HIGH": true}},
		DXContinents: filter.StringRules{Allow: map[string]bool{"EU": false}}, DEContinents: filter.StringRules{Allow: map[string]bool{"AS": true}},
		DXZones: filter.IntRules{Allow: map[int]bool{1: false}}, DEZones: filter.IntRules{Allow: map[int]bool{2: true}},
		DXGrid2: filter.StringRules{Allow: map[string]bool{"FN": false}}, DEGrid2: filter.StringRules{Allow: map[string]bool{"JN": true}},
		DXDXCC: filter.IntRules{Allow: map[int]bool{291: false}}, DEDXCC: filter.IntRules{Allow: map[int]bool{230: true}},
	}
	if err != nil || !reflect.DeepEqual(got.Filters, want) {
		t.Fatalf("category assigned to wrong target: got=%+v want=%+v err=%v", got.Filters, want, err)
	}
}

func TestMachineSchemaPATCHRuleFlagsAndSettingsIntegers(t *testing.T) {
	before := filter.Configuration{Filters: filter.FilterConfiguration{Bands: filter.StringRules{AllowAll: true, BlockAll: true}}, Settings: filter.SettingsConfiguration{PathMinObservationCount: 8, SolarSummaryMinutes: 60}}
	for _, tt := range []struct {
		configuration string
		allowAll      bool
		blockAll      bool
		path          int
		solar         int
	}{
		{"filters: {bands: {allow_all: false}}\n", false, true, 8, 60},
		{"filters: {bands: {block_all: false}}\n", true, false, 8, 60},
		{"settings: {path_min_observation_count: 12}\n", true, true, 12, 60},
		{"settings: {solar_summary_minutes: 30}\n", true, true, 8, 30},
	} {
		request, err := decodeMachineRequest(machineRequestFixture(tt.configuration, true), machineCommand{Verb: "PATCH", Resource: "CONFIG"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := request.apply(before)
		if err != nil || got.Filters.Bands.AllowAll != tt.allowAll || got.Filters.Bands.BlockAll != tt.blockAll || got.Settings.PathMinObservationCount != tt.path || got.Settings.SolarSummaryMinutes != tt.solar {
			t.Fatalf("scalar assigned to wrong target: %+v err=%v", got, err)
		}
	}
}

func TestMachineSchemaPUTRequiresEveryField(t *testing.T) {
	for _, resource := range []string{"FILTER", "SETTINGS"} {
		fixture := machineCompleteFilterFixture
		if resource == "SETTINGS" {
			fixture = machineCompleteSettingsFixture
		}
		for _, line := range strings.Split(strings.TrimSuffix(fixture, "\n"), "\n") {
			field, _, _ := strings.Cut(line, ":")
			t.Run(resource+"_"+field, func(t *testing.T) {
				without := strings.Replace(fixture, line+"\n", "", 1)
				_, err := decodeMachineRequest(machineRequestFixture(without, true), machineCommand{Verb: "PUT", Resource: resource})
				if err == nil || !strings.Contains(err.Error(), "required field is missing") {
					t.Fatalf("missing %s admitted: %v", field, err)
				}
			})
		}
	}
	for _, member := range []string{"allow_all: false, ", "block_all: false, ", "allow: {}, ", ", block: {}"} {
		t.Run("RuleSet_"+member, func(t *testing.T) {
			original := "sources: {allow_all: false, block_all: false, allow: {}, block: {}}"
			incomplete := strings.Replace(original, member, "", 1)
			fixture := strings.Replace(machineCompleteFilterFixture, original, incomplete, 1)
			_, err := decodeMachineRequest(machineRequestFixture(fixture, true), machineCommand{Verb: "PUT", Resource: "FILTER"})
			if err == nil || !strings.Contains(err.Error(), "required field is missing") {
				t.Fatalf("incomplete RuleSet admitted: %v", err)
			}
		})
	}
}

func TestMachineSchemaVALIDATERequiresCompleteCONFIGWithoutRevision(t *testing.T) {
	configuration := "filters:\n" + indentMachineFixture(machineCompleteFilterFixture) + "settings:\n" + indentMachineFixture(machineCompleteSettingsFixture)
	request, err := decodeMachineRequest(machineRequestFixture(configuration, false), machineCommand{Verb: "VALIDATE", Resource: "CONFIG"})
	if err != nil || request.IfRevision != "" || request.Configuration.Settings.NoiseClass != "QUIET" || request.Configuration.Filters.Modes.Allow["USB"] {
		t.Fatalf("complete VALIDATE failed: %+v %v", request, err)
	}
	for _, incomplete := range []string{"filters: {}\n", "settings: {}\n", "{}\n"} {
		_, err := decodeMachineRequest(machineRequestFixture(incomplete, false), machineCommand{Verb: "VALIDATE", Resource: "CONFIG"})
		if err == nil {
			t.Fatalf("incomplete VALIDATE admitted: %s", incomplete)
		}
	}
}

func TestMachineSchemaRejectsInvalidShapesAndTypes(t *testing.T) {
	for _, tt := range []struct {
		resource      string
		configuration string
	}{
		{"SETTINGS", "noise_class: false\n"},
		{"SETTINGS", "solar_summary_minutes: \"0\"\n"},
		{"SETTINGS", "path_min_observation_count: 0.0\n"},
		{"SETTINGS", "noise_class: null\n"},
		{"SETTINGS", "noise_class:\n"},
		{"SETTINGS", "pause: true\n"},
		{"SETTINGS", "diagnostics: ALL\n"},
		{"SETTINGS", "preset: CONTEST\n"},
		{"SETTINGS", "effective: {}\n"},
		{"FILTER", "bands: {allow: []}\n"},
		{"FILTER", "bands: {allow_all: \"false\"}\n"},
		{"FILTER", "bands: {allow: {20m: 1}}\n"},
		{"FILTER", "modes: {allow: {1: true}}\n"},
		{"FILTER", "dx_zones: {allow: {wrong: true}}\n"},
		{"FILTER", "include_beacons: default\n"},
		{"FILTER", "allow_wwv: yes\n"},
		{"FILTER", "nearby_enabled: DEFAULT\n"},
		{"FILTER", "dx_callsigns: {}\n"},
		{"FILTER", "dx_callsigns: [123]\n"},
		{"FILTER", "nearby_snapshot: {}\n"},
		{"CONFIG", "filters: []\n"},
		{"CONFIG", "settings: null\n"},
		{"CONFIG", "session: {}\n"},
	} {
		t.Run(tt.configuration, func(t *testing.T) {
			_, err := decodeMachineRequest(machineRequestFixture(tt.configuration, true), machineCommand{Verb: "PATCH", Resource: tt.resource})
			if err == nil {
				t.Fatalf("invalid shape/type admitted: %q", tt.configuration)
			}
		})
	}
}

func TestMachineSchemaRejectsDuplicateInterpretedKeys(t *testing.T) {
	for _, configuration := range []string{
		"noise_class: QUIET\nnoise_class: URBAN\n",
		"modes: {allow: {CW: true, CW: false}}\n",
		"modes: {allow: {CW: true, cw: false}}\n",
		"dx_zones: {allow: {1: true, 01: false}}\n",
		"dx_zones: {allow: {1: true, 0x1: false}}\n",
		"dx_zones: {allow: {\"1\": true, 0b1: false}}\n",
	} {
		resource := "FILTER"
		if strings.HasPrefix(configuration, "noise_class") {
			resource = "SETTINGS"
		}
		_, err := decodeMachineRequest(machineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: resource})
		if err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("duplicate admitted: %q err=%v", configuration, err)
		}
	}
}

func TestMachineSchemaRejectsAdvancedYAMLAndMultipleDocuments(t *testing.T) {
	for _, configuration := range []string{
		"noise_class: &choice URBAN\n",
		"noise_class: !policy URBAN\n",
		"<<: {noise_class: URBAN}\n",
		"noise_class: { ? [x, y]: URBAN }\n",
	} {
		_, err := decodeMachineRequest(machineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: "SETTINGS"})
		if err == nil {
			t.Fatalf("advanced YAML admitted: %q", configuration)
		}
	}
	alias := machineRequestFixture("dx_callsigns: [&rule \"W1*\", *rule]\n", true)
	if _, err := decodeMachineRequest(alias, machineCommand{Verb: "PATCH", Resource: "FILTER"}); err == nil {
		t.Fatal("alias admitted")
	}
	multiple := append(machineRequestFixture("noise_class: URBAN\n", true), []byte("---\nvalue: other\n")...)
	if _, err := decodeMachineRequest(multiple, machineCommand{Verb: "PATCH", Resource: "SETTINGS"}); err == nil {
		t.Fatal("second document admitted")
	}
}

func TestMachineSchemaEnvelopeBoundsAndCompleteness(t *testing.T) {
	good := machineRequestFixture("noise_class: URBAN\n", true)
	for _, mutated := range [][]byte{
		bytes.Replace(good, []byte("schema_version: 1\n"), nil, 1),
		bytes.Replace(good, []byte("schema_version: 1"), []byte("schema_version: 4"), 1),
		bytes.Replace(good, []byte("schema_version: 1"), []byte("schema_version: \"1\""), 1),
		bytes.Replace(good, []byte("request_id: noise-Ab1\n"), nil, 1),
		bytes.Replace(good, []byte("request_id: noise-Ab1"), []byte("request_id: noise_1"), 1),
		bytes.Replace(good, []byte("request_id: noise-Ab1"), []byte("request_id: "+strings.Repeat("a", 33)), 1),
		bytes.Replace(good, []byte("if_revision: opaque-revision-1\n"), nil, 1),
		bytes.Replace(good, []byte("if_revision: opaque-revision-1"), []byte("if_revision: \"\""), 1),
		bytes.Replace(good, []byte("if_revision: opaque-revision-1"), []byte("if_revision: "+strings.Repeat("a", 129)), 1),
		bytes.Replace(good, []byte("if_revision: opaque-revision-1"), []byte("if_revision: \"x\\ny\""), 1),
		append([]byte("status: success\n"), good...),
		[]byte("schema_version: 1\nrequest_id: x\nif_revision: r\n"),
	} {
		if _, err := decodeMachineRequest(mutated, machineCommand{Verb: "PATCH", Resource: "SETTINGS"}); err == nil {
			t.Fatalf("invalid envelope admitted: %q", mutated)
		}
	}
	boundary := bytes.Replace(good, []byte("request_id: noise-Ab1"), []byte("request_id: "+strings.Repeat("a", 32)), 1)
	boundary = bytes.Replace(boundary, []byte("if_revision: opaque-revision-1"), []byte("if_revision: "+strings.Repeat("r", 128)), 1)
	if _, err := decodeMachineRequest(boundary, machineCommand{Verb: "PATCH", Resource: "SETTINGS"}); err != nil {
		t.Fatalf("valid envelope boundary rejected: %v", err)
	}
}

func TestMachineSchemaErrorMessagesAreBounded(t *testing.T) {
	for _, body := range [][]byte{
		[]byte("schema_version: 1\nrequest_id: x\nif_revision: r\n" + strings.Repeat("a", 60_000) + ": value\nconfiguration: {}\n"),
		[]byte("value: \"" + strings.Repeat("a", 60_000)),
		bytes.Repeat([]byte{'a'}, 65_537),
	} {
		_, err := decodeMachineRequest(body, machineCommand{Verb: "PATCH", Resource: "SETTINGS"})
		if err == nil || len(err.Error()) > 256 {
			t.Fatalf("unbounded or missing error: %v", err)
		}
	}
}

func TestMachineSchemaSuppliedValuesDetachFromSourceBody(t *testing.T) {
	body := machineRequestFixture("modes: {allow: {CW: false}}\ndx_callsigns: [\"W1*\", \"K1*\"]\n", true)
	request, err := decodeMachineRequest(body, machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range body {
		body[i] = 'x'
	}
	got, err := request.apply(filter.Configuration{})
	if err != nil || !reflect.DeepEqual(got.Filters.Modes.Allow, map[string]bool{"CW": false}) || !reflect.DeepEqual(got.Filters.DXCallsigns, []string{"W1*", "K1*"}) || request.RequestID != "noise-Ab1" {
		t.Fatalf("decoded values retained mutable input: %+v %v", got, err)
	}
}

func FuzzMachineSchema(f *testing.F) {
	seeds := []struct {
		selector uint8
		body     []byte
	}{
		{0, machineRequestFixture(machineCompleteSettingsFixture, true)},
		{1, machineRequestFixture("modes: {allow: {CW: false}}\ndx_callsigns: [\"W1*\", \"W1*\"]\n", true)},
		{1, machineRequestFixture("dx_zones: {allow: {1: true, 01: false}}\n", true)},
		{1, machineRequestFixture("dx_callsigns: [&a \"W1*\", *a]\n", true)},
		{1, bytes.Replace(machineRequestFixture("dx_states: {allow: {CA: true, TX: false}}\nde_states: {block_all: true}\n", true), []byte("schema_version: 1"), []byte("schema_version: 2"), 1)},
		{4, bytes.Replace(machineRequestFixture(machineCompleteFilterFixture+"dx_states: {allow_all: true, block_all: false, allow: {}, block: {}}\nde_states: {allow_all: true, block_all: false, allow: {}, block: {}}\n", true), []byte("schema_version: 1"), []byte("schema_version: 2"), 1)},
		{1, minSNRMachineFixture("min_snr: {CW: 0, FT8: -10}\n", 3, true)},
		{1, minSNRMachineFixture("min_snr: {FT8: 0, FT8: -1}\n", 3, true)},
		{4, minSNRMachineFixture(machineCompleteFilterFixture+minSNRCompleteStateFixture+"min_snr: {CW: 0, FT8: -10}\n", 3, true)},
		{3, minSNRMachineFixture("filters:\n"+indentMachineFixture(machineCompleteFilterFixture+minSNRCompleteStateFixture+"min_snr: {CW: 0, FT8: -10}\n")+"settings:\n"+indentMachineFixture(machineCompleteSettingsFixture), 3, false)},
		{2, machineRequestFixture("settings: {noise_class: URBAN}\n", true)},
		{3, machineRequestFixture("filters:\n"+indentMachineFixture(machineCompleteFilterFixture)+"settings:\n"+indentMachineFixture(machineCompleteSettingsFixture), false)},
		{4, machineRequestFixture(machineCompleteFilterFixture, true)},
		{2, machineRequestFixture("filters: "+strings.Repeat("[", 256)+"x"+strings.Repeat("]", 256)+"\n", true)},
	}
	for _, seed := range seeds {
		f.Add(seed.selector, seed.body)
	}
	f.Fuzz(func(t *testing.T, selector uint8, body []byte) {
		if len(body) > 65_537 {
			t.Skip()
		}
		commands := [...]machineCommand{{Verb: "PUT", Resource: "SETTINGS"}, {Verb: "PATCH", Resource: "FILTER"}, {Verb: "PATCH", Resource: "CONFIG"}, {Verb: "VALIDATE", Resource: "CONFIG"}, {Verb: "PUT", Resource: "FILTER"}}
		command := commands[int(selector)%len(commands)]
		request, err := decodeMachineRequest(body, command)
		if err != nil {
			if len(err.Error()) > 256 {
				t.Fatal("schema error includes unbounded input")
			}
			return
		}
		if (request.SchemaVersion != 1 && request.SchemaVersion != 2 && request.SchemaVersion != 3) || len(request.RequestID) < 1 || len(request.RequestID) > 32 || len(request.IfRevision) > 128 || (command.Verb != "VALIDATE" && request.IfRevision == "") {
			t.Fatal("invalid envelope admitted")
		}
		before := filter.Configuration{Filters: filter.FilterConfiguration{MinSNR: map[string]int{"FT8": -10, "DORMANT": 0}, Bands: filter.StringRules{Allow: map[string]bool{"40m": true}, Block: map[string]bool{"80m": false}}, DXStates: filter.StringRules{Allow: map[string]bool{"CA": false}, Block: map[string]bool{"TX": true}}, DEStates: filter.StringRules{AllowAll: true, Block: map[string]bool{"NY": false}}, DXCallsigns: []string{"K2*"}}, Settings: filter.SettingsConfiguration{NoiseClass: "QUIET", Grid: "FN42", SolarSummaryMinutes: 30}}
		stateBefore := before.Clone()
		next, err := request.apply(before)
		if err != nil {
			t.Fatalf("decoded request could not apply: %v", err)
		}
		detached := next.Clone()
		for key := range detached.Filters.MinSNR {
			detached.Filters.MinSNR[key] = 42
			break
		}
		if !reflect.DeepEqual(before.Filters.Bands.Allow, map[string]bool{"40m": true}) || !reflect.DeepEqual(before.Filters.Bands.Block, map[string]bool{"80m": false}) || !reflect.DeepEqual(before.Filters.DXCallsigns, []string{"K2*"}) || before.Settings.NoiseClass != "QUIET" || before.Settings.Grid != "FN42" || before.Settings.SolarSummaryMinutes != 30 {
			t.Fatal("apply mutated previous configuration")
		}
		if !reflect.DeepEqual(before.Filters.MinSNR, stateBefore.Filters.MinSNR) {
			t.Fatal("apply mutated previous minima")
		}
		if !sameRules(before.Filters.DXStates, stateBefore.Filters.DXStates) || !sameRules(before.Filters.DEStates, stateBefore.Filters.DEStates) {
			t.Fatal("apply mutated previous state rules")
		}
	})
}
