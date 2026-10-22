package telnet

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func v5CoordinatorFixture(t *testing.T) (*Server, *Client, filter.V5Configuration) {
	t.Helper()
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	cfg := filter.V5Configuration{
		Filters: filter.V5FilterConfiguration{
			Rules:    []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND DXCONT=EU"}},
			AllowWWV: filter.DefaultBoolFalse, AllowWCY: filter.DefaultBoolTrue,
			AllowSelf: filter.DefaultBoolDefault, AllowToxic: filter.DefaultBoolTrue,
		},
		Settings: filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 15},
	}
	cfg, value, err := filter.PrepareV5Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.filter, c.configuredSettings, c.configurationInitialized = &value, cfg.Settings, true
	c.noiseClass, c.dialect = "QUIET", DialectGo
	c.setDedupePolicy(dedupePolicyFast)
	c.setSolarSummaryMinutes(15, time.Now())
	c.lastLoginUTC = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	c.recentIPs = []string{"192.0.2.1"}
	c.presetReference, err = filter.RuntimePresetReferenceFromV5(&filter.V5PresetReference{
		Name: "BASE", Baseline: &filter.V5SavedPreset{Configuration: cfg},
	})
	if err != nil {
		t.Fatal(err)
	}
	s.registerClient(c)
	v5CoordinatorRevision(t, c)
	return s, c, cfg
}

func v5CoordinatorRevision(t *testing.T, c *Client) string {
	t.Helper()
	release, err := c.server.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	token, err := c.v5ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func v5CoordinatorProposal(t *testing.T, c *Client, verb, resource string, configuration any) (machineCommand, v5MachineRequest) {
	t.Helper()
	body, err := yaml.Marshal(struct {
		Version       int    `yaml:"schema_version"`
		ID            string `yaml:"request_id"`
		Revision      string `yaml:"if_revision,omitempty"`
		Configuration any    `yaml:"configuration"`
	}{5, "edit-1", v5CoordinatorRevision(t, c), configuration})
	if err != nil {
		t.Fatal(err)
	}
	cmd := machineCommand{Verb: verb, Resource: resource}
	request, err := decodeV5MachineRequest(body, cmd)
	if err != nil {
		t.Fatalf("proposal decode: %v\n%s", err, body)
	}
	return cmd, request
}

func v5CoordinatorSeed(t *testing.T, c *Client, cfg filter.V5Configuration) []byte {
	t.Helper()
	release, err := c.server.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ref, err := filter.V5PresetReferenceFromRuntime(c.presetReference)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := filter.PrepareV5UserRecord(c.callsign, &filter.V5UserRecord{
		Configuration: cfg, Preset: ref, RecentIPs: c.recentIPs, LastLoginUTC: c.lastLoginUTC,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	return v5CoordinatorDisk(t, c)
}

func v5CoordinatorDisk(t *testing.T, c *Client) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(filter.UserDataDir, c.callsign+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func v5CoordinatorLibrary(t *testing.T, c *Client, cfg filter.V5Configuration) (string, []byte) {
	t.Helper()
	path := presetLibraryPathFor(c.callsign)
	data, err := filter.EncodeV5PresetCollection(&filter.V5PresetCollection{Presets: map[string]*filter.V5SavedPreset{
		"INDEPENDENT": {Configuration: cfg},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func v5CoordinatorSuccess(t *testing.T, response string, applied bool) map[string]any {
	t.Helper()
	doc := v5ReadbackDocument(t, response)
	result, ok := doc["result"].(map[string]any)
	if !ok || result["valid"] != true || result["applied"] != applied || result["persisted"] != applied || doc["error"] != nil {
		t.Fatalf("incorrect actual coordinator success: %s", response)
	}
	return doc
}

func TestV5MachineCoordinatorDurableCanonicalWriteAndReload(t *testing.T) {
	s, c, before := v5CoordinatorFixture(t)
	libraryPath, library := v5CoordinatorLibrary(t, c, before)
	pointer, programs := c.filter, c.filter.PreparedRules()
	baseline := c.presetReference.Baseline
	oldRules := programs.Definitions()
	initial := v5CoordinatorRevision(t, c)
	// The first machine write creates the destination, before any disconnect.
	cmd, request := v5CoordinatorProposal(t, c, "PUT", "FILTER", before.Filters)
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	if c.configurationRevision != 0 || v5CoordinatorRevision(t, c) != initial {
		t.Fatal("idempotent first write advanced revision")
	}
	// Authoritative timestamps survive; the current session IP remains first,
	// matching the existing preference-save merge contract.
	record, err := filter.LoadV5UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	fresh := time.Date(2026, 10, 21, 12, 0, 0, 123456789, time.UTC)
	record.LastLoginUTC, record.RecentIPs = fresh, []string{"192.0.2.99", "192.0.2.1"}
	payload, err := filter.PrepareV5UserRecord(c.callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	next := before.Clone()
	next.Filters.Rules = []filter.RuleDefinition{
		{Slot: 9, Action: filter.RuleReject, Expr: `comment=" MiXeD ""Q"" "`},
		{Slot: 0, Action: filter.RulePass, Expr: "band=20m and mode=ft8"},
	}
	next.Filters.IncludeBeacons, next.Filters.AllowSelf = filter.DefaultBoolFalse, filter.DefaultBoolTrue
	next.Settings = filter.SettingsConfiguration{Dialect: "", NoiseClass: "URBAN", DedupePolicy: "", SolarSummaryMinutes: 0}
	cmd, request = v5CoordinatorProposal(t, c, "PUT", "CONFIG", next)
	reply := v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	canonical, _, err := filter.PrepareV5Configuration(next)
	if err != nil {
		t.Fatal(err)
	}
	if c.filter != pointer || c.configurationRevision != 1 || reply["revision"] != v5CoordinatorRevision(t, c) ||
		!c.captureV5Configuration().Equal(canonical) || c.configuredSettings != next.Settings ||
		c.noiseClass != "URBAN" || c.getSolarSummaryMinutes() != 0 || c.dialect != DialectGo {
		t.Fatal("publication lost exact settings, stable pointer or single revision advance")
	}
	if !slices.Equal(programs.Definitions(), oldRules) || c.presetReference.Baseline != baseline ||
		!before.Equal(filter.V5ConfigurationFromFilter(&baseline.Filter, before.Settings)) {
		t.Fatal("write mutated old immutable programs or the applied baseline")
	}
	if after, err := os.ReadFile(libraryPath); err != nil || !bytes.Equal(library, after) {
		t.Fatal("machine config write changed the independent named library")
	}
	data := v5CoordinatorDisk(t, c)
	if !bytes.Contains(data, []byte("configuration_version: 5\nconfiguration:\n")) ||
		bytes.Contains(data, []byte("all_bands:")) || bytes.Contains(data, []byte("prepared")) {
		t.Fatalf("not explicit new-format disk data: %s", data)
	}
	loaded, err := filter.LoadV5UserRecord(c.callsign)
	if err != nil || !loaded.Configuration.Equal(canonical) || loaded.Preset == nil ||
		!loaded.Preset.Baseline.Configuration.Equal(before) || !loaded.LastLoginUTC.Equal(fresh) ||
		!slices.Equal(loaded.RecentIPs, []string{"192.0.2.1", "192.0.2.99"}) {
		t.Fatalf("immediate durable reload lost definitions/baseline/metadata: %+v, %v", loaded, err)
	}
	c.preserveRecordOnExit.Store(true) // Active v4 teardown is intentionally not S6b.
	s.unregisterClient(c)
	reconnected := configurationTestClient(s, c.callsign)
	_, runtime, err := filter.PrepareV5Configuration(loaded.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	reconnected.filter, reconnected.configuredSettings, reconnected.configurationInitialized = &runtime, loaded.Configuration.Settings, true
	reconnected.presetReference, err = filter.RuntimePresetReferenceFromV5(loaded.Preset)
	if err != nil {
		t.Fatal(err)
	}
	s.registerClient(reconnected)
	if v5CoordinatorRevision(t, reconnected) == reply["revision"] || !reconnected.captureV5Configuration().Equal(canonical) {
		t.Fatal("native reload retained old session token or lost saved data")
	}
}

func TestV5MachineCoordinatorPATCHOmissionEmptyAndIdempotence(t *testing.T) {
	s, c, before := v5CoordinatorFixture(t)
	v5CoordinatorSeed(t, c, before)
	clock := time.Now().Add(7 * time.Minute)
	c.solarNextSummaryAt = clock
	c.startReadPause(time.Now(), time.Minute)
	c.setDiagMode(diagModePath)
	pause, pointer, ref := c.readPauseUntilUnixNano.Load(), c.filter, c.presetReference
	for i, patch := range []map[string]any{
		{"allow_wwv": true},
		{"rules": []any{}},
		{}, // Must persist without installing defaults or rebuilding the runtime.
	} {
		set := c.filter.PreparedRules()
		cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", patch)
		v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
		if i == 0 && (c.filter.PreparedRules() != set || !slices.Equal(c.captureV5Configuration().Filters.Rules, before.Filters.Rules)) {
			t.Fatal("omitted rules replaced the immutable program")
		}
		if i >= 1 && len(c.captureV5Configuration().Filters.Rules) != 0 {
			t.Fatal("explicit [] did not remove all rules, or empty patch installed defaults")
		}
		if c.solarNextSummaryAt != clock || c.readPauseUntilUnixNano.Load() != pause ||
			c.getDiagMode() != diagModePath || c.filter != pointer || c.presetReference != ref {
			t.Fatal("unrelated patch changed clock/control/pointer/preset state")
		}
	}
	if c.configurationRevision != 2 {
		t.Fatalf("edits + idempotent write revision=%d, want 2", c.configurationRevision)
	}
	// A legal, out-of-band disk change is repaired even by an unchanged request.
	v5CoordinatorSeed(t, c, before)
	set := c.filter.PreparedRules()
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	record, err := filter.LoadV5UserRecord(c.callsign)
	if err != nil || len(record.Configuration.Filters.Rules) != 0 || c.configurationRevision != 2 || c.filter.PreparedRules() != set {
		t.Fatal("unchanged write did not repair disk without runtime reset")
	}
}

func TestV5MachineCoordinatorGETActualStatusAndCapabilities(t *testing.T) {
	s, c, cfg := v5CoordinatorFixture(t)
	for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
		t.Run(resource, func(t *testing.T) {
			doc := v5ReadbackDocument(t, s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: resource}))
			if len(doc["request_id"].(string)) != 32 || doc["revision"] != v5CoordinatorRevision(t, c) {
				t.Fatal("GET did not assign a nonce and current v5 revision")
			}
			status := doc["status"].(map[string]any)
			preset := status["preset"].(map[string]any)
			if preset["associated"] != true || preset["modified"] != false || preset["name"] != "BASE" ||
				status["min_snr"] != nil || status["comments"] != nil ||
				status["rules"].(map[string]any)["configured_count"] != 1 {
				t.Fatalf("actual status=%+v", status)
			}
			config := doc["configuration"].(map[string]any)
			if resource == "CAPABILITIES" && config["rule_limits"] == nil ||
				resource == "FILTER" && config["rules"] == nil ||
				resource == "SETTINGS" && config["dialect"] != cfg.Settings.Dialect {
				t.Fatalf("wrong resource=%+v", config)
			}
		})
	}
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{"rules": []filter.RuleDefinition{{Slot: 0, Action: filter.RuleReject, Expr: "MODE=FT8"}}})
	v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), true)
	doc := v5ReadbackDocument(t, s.prepareV5MachineReadback(c, machineCommand{Verb: "GET", Resource: "CONFIG", RequestID: "get-1"}))
	if doc["status"].(map[string]any)["preset"].(map[string]any)["modified"] != true {
		t.Fatal("preset comparison ignored the private rule carrier")
	}
}

func TestV5MachineCoordinatorVALIDATEHasNoEffects(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(fmt.Sprint(protected), func(t *testing.T) {
			s, c, cfg := v5CoordinatorFixture(t)
			disk := v5CoordinatorSeed(t, c, cfg)
			c.recordProtected = protected
			c.startReadPause(time.Now(), time.Minute)
			c.setDiagMode(diagModeMode)
			before := snapshotBooleanCommand(c)
			clock, pause, revision, generation := c.solarNextSummaryAt, c.readPauseUntilUnixNano.Load(), c.configurationRevision, c.historyGeneration
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return os.ErrPermission }
			cfg.Filters.Rules = []filter.RuleDefinition{{Slot: 7, Action: filter.RuleReject, Expr: "MODE=FT8"}}
			cfg.Settings.NoiseClass = "URBAN"
			cmd, request := v5CoordinatorProposal(t, c, "VALIDATE", "CONFIG", cfg)
			request.IfRevision = "stale-but-validation-has-no-precondition"
			v5CoordinatorSuccess(t, s.applyV5MachineRequest(c, cmd, request), false)
			before.assertUnchanged(t, c)
			if commits != 0 || c.configurationRevision != revision || c.historyGeneration != generation ||
				c.solarNextSummaryAt != clock || c.readPauseUntilUnixNano.Load() != pause || c.getDiagMode() != diagModeMode ||
				!bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
				t.Fatal("VALIDATE committed, published or mutated a revision/clock/control")
			}
		})
	}
}

func TestV5MachineCoordinatorRuntimePresetAdapterOwnsDefinitions(t *testing.T) {
	_, c, cfg := v5CoordinatorFixture(t)
	ref, err := filter.V5PresetReferenceFromRuntime(c.presetReference)
	if err != nil || ref.Baseline.ConfigurationVersion != 5 ||
		c.presetReference.Baseline.ConfigurationVersion != filter.CurrentConfigurationVersion ||
		!ref.Baseline.Configuration.Equal(cfg) {
		t.Fatalf("adapter marker or exact values lost: %+v %v", ref, err)
	}
	ref.Baseline.Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	if reflect.DeepEqual(ref.Baseline.Configuration.Filters.Rules, c.presetReference.Baseline.PreparedRules().Definitions()) {
		t.Fatal("adapter retained mutable definition aliases")
	}
	data, err := yaml.Marshal(c.presetReference.Baseline)
	if err != nil || !strings.Contains(string(data), "configuration_version: 5") || !strings.Contains(string(data), "rules:") {
		t.Fatalf("active runtime preset did not serialize v5 definitions: %v", err)
	}
	var restored filter.SavedPreset
	if err := yaml.Unmarshal(data, &restored); err != nil || !filter.ConfigurationFromPreset(&restored).V5().Equal(cfg) {
		t.Fatalf("active runtime preset did not restore exact v5 state: %v", err)
	}
}
