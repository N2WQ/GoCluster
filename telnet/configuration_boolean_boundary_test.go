package telnet

import (
	"bytes"
	"runtime"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func booleanBoundaryConfiguration(t *testing.T, expression string) (filter.V6Configuration, filter.Filter) {
	t.Helper()
	cfg := filter.V6Configuration{
		Filters: filter.V6FilterConfiguration{
			Rules:      []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: expression}},
			AllowToxic: filter.DefaultBoolFalse,
		},
		Settings: filter.SettingsConfiguration{
			Dialect: "go", Grid: "FN31", NoiseClass: "QUIET",
			DedupePolicy: "MED", PathMinObservationCount: 25,
		},
	}
	canonical, runtimeFilter, err := filter.PrepareV6Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, runtimeFilter
}

func TestBooleanPreparedStateCopyBoundaries(t *testing.T) {
	server := presetTestServer(t)
	cfg, runtimeFilter := booleanBoundaryConfiguration(t, "MODE=CW OR DXCONT=EU")
	client := configurationTestClient(server, "W6TEST-1")
	*client.filter = runtimeFilter
	client.configuredSettings, client.configurationInitialized = cfg.Settings, true
	client.grid, client.gridCell, client.gridCoarseCell = cfg.Settings.Grid, 101, 202
	client.noiseClass, client.pathMinObservationCount = cfg.Settings.NoiseClass, cfg.Settings.PathMinObservationCount
	client.setDedupePolicy(dedupePolicyMed)
	identity := client.filter
	originalRules := client.filter.PreparedRules()
	if originalRules == nil {
		t.Fatal("prepared configuration lost its rule program")
	}

	staged := newHumanPreferenceCandidate(client)
	captured := client.captureHistoryFilter()
	snapshot, err := client.captureConfiguration(0)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip := snapshot.Clone().FilterValue()
	for name, copied := range map[string]*filter.Filter{
		"staging": staged.filter, "history": captured.client.filter, "configuration": &roundTrip,
	} {
		if copied.PreparedRules() != originalRules {
			t.Fatalf("%s discarded or recompiled immutable rules", name)
		}
	}
	if staged.gridCell != 101 || staged.gridCoarseCell != 202 ||
		captured.client.gridCell != 101 || captured.client.gridCoarseCell != 202 {
		t.Fatal("detached boundary lost effective NEARBY cells")
	}
	staged.filter.SetToxicEnabled(true)
	if client.filter.ToxicEnabled() || captured.client.filter.ToxicEnabled() {
		t.Fatal("staging controls still borrow live pointers")
	}

	changedRules, err := originalRules.WithRule(filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8 AND DXCONT=EU"})
	if err != nil {
		t.Fatal(err)
	}
	changedConfig := cfg.Clone()
	changedConfig.Filters = changedConfig.Filters.WithRuleSet(changedRules)
	changedConfig.Filters.AllowToxic = filter.DefaultBoolTrue
	_, next, err := filter.PrepareV6Configuration(changedConfig)
	if err != nil {
		t.Fatal(err)
	}
	*staged.filter = next
	server.publishHumanPreference(client, staged, false)
	if client.filter != identity || client.filter.PreparedRules() != changedRules {
		t.Fatal("publication replaced filter identity or lost prepared rules")
	}
	probe := &spot.Spot{Mode: "CW", IsHuman: true}
	if !captured.client.filter.PreparedRules().Matches(probe, filter.EvaluationContext{PathClass: filter.PathClassInsufficient}) ||
		client.filter.PreparedRules().Matches(probe, filter.EvaluationContext{PathClass: filter.PathClassInsufficient}) {
		t.Fatal("publication changed an old snapshot or failed to activate new prepared state")
	}
	if !client.filter.ToxicEnabled() || captured.client.filter.ToxicEnabled() ||
		client.configuredSettings != cfg.Settings {
		t.Fatal("publication corrupted unrelated configured preferences")
	}
	if client.gridCell != pathreliability.CellID(101) ||
		client.gridCoarseCell != pathreliability.CellID(202) {
		t.Fatal("publication discarded NEARBY runtime state")
	}
}

func TestBooleanPreparedStateActiveV6SerializationBoundary(t *testing.T) {
	if filter.CurrentConfigurationVersion != 6 {
		t.Fatal("Boolean activation requires disk version 6")
	}
	cfg, runtimeFilter := booleanBoundaryConfiguration(t, "MODE=CW")
	original := filter.V6ConfigurationFromFilter(&runtimeFilter, cfg.Settings)
	withRules, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	originalFingerprint := original.Fingerprint()
	empty := cfg.Clone()
	empty.Filters = empty.Filters.WithRuleSet(nil)
	_, emptyRuntime, err := filter.PrepareV6Configuration(empty)
	if err != nil {
		t.Fatal(err)
	}
	withoutRules := filter.V6ConfigurationFromFilter(&emptyRuntime, cfg.Settings)
	withoutBytes, err := yaml.Marshal(withoutRules)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(withRules, withoutBytes) || originalFingerprint == withoutRules.Fingerprint() {
		t.Fatal("active v6 wire shape and digest must include public rule definitions")
	}
	if bytes.Contains(withRules, []byte("preparedRules")) || !bytes.Contains(withRules, []byte("MODE=CW")) ||
		bytes.Contains(withRules, []byte("all_bands")) {
		t.Fatal("v6 readback lost public rules or leaked compiled/retired category state")
	}
}

func TestBooleanPreparedStateConcurrentPublication(t *testing.T) {
	server := presetTestServer(t)
	cfg, first := booleanBoundaryConfiguration(t, "MODE=CW")
	nextConfig := cfg.Clone()
	nextConfig.Filters.Rules[0].Expr = "MODE=FT8"
	nextConfig.Filters.AllowToxic = filter.DefaultBoolTrue
	_, second, err := filter.PrepareV6Configuration(nextConfig)
	if err != nil {
		t.Fatal(err)
	}
	client := configurationTestClient(server, "W6TEST-1")
	*client.filter = first
	client.configuredSettings, client.configurationInitialized = cfg.Settings, true
	states := [2]Client{
		{filter: &first, configuredSettings: cfg.Settings, configurationInitialized: true},
		{filter: &second, configuredSettings: cfg.Settings, configurationInitialized: true},
	}
	originalPointer := client.filter
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			server.publishHumanPreference(client, &states[i%len(states)], false)
			runtime.Gosched()
		}
	}()
	defer func() { <-done }()
	for range 100 {
		snapshot := client.captureHistoryFilter()
		copied := snapshot.client.filter
		switch copied.PreparedRules() {
		case first.PreparedRules():
			if copied.ToxicEnabled() {
				t.Fatal("first program acquired the second configuration's control")
			}
		case second.PreparedRules():
			if !copied.ToxicEnabled() {
				t.Fatal("second program acquired the first configuration's control")
			}
		default:
			t.Fatal("snapshot discarded its prepared program")
		}
		if client.filter != originalPointer {
			t.Fatal("publication replaced the stable filter identity")
		}
		runtime.Gosched()
	}
}
