package telnet

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// Legacy GO fixtures remain literal version six. A CC owner witness instead
// carries both native banks and uses the explicit version-seven storage API.
func attachNativeCCTestBanks(t *testing.T, c *Client, modes []string) {
	t.Helper()
	configuration, _, runtime, err := filter.PrepareV7Configuration(filter.V7Configuration{
		Filters: filter.V7FilterBanks{
			GO: filter.V6FilterConfiguration{Rules: []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW"}}},
			CC: filter.CCConfiguration{Modes: modes, Sources: []string{"HUMAN", "SKIMMER"}},
		}, Settings: filter.SettingsConfiguration{Dialect: "cc", NoiseClass: "QUIET", DedupePolicy: "FAST"},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.dialect, c.filter, c.filterBanks = DialectCC, &runtime, &configuration.Filters
	c.configuredSettings, c.configurationInitialized, c.noiseClass = configuration.Settings, true, "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
}

func nativeCCTestCommittedSession(t *testing.T) (*Server, *Client) {
	t.Helper()
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	c := configurationTestClient(s, "W1ABC-1")
	attachNativeCCTestBanks(t, c, []string{"CW", "FT8"})
	requireFilterArgumentControl(t, s.filterEngine, c, "SET/FILTER BEACON/OFF")
	assertNativeCCTestSaved(t, c)
	return s, c
}

func assertNativeCCTestSaved(t *testing.T, c *Client) {
	t.Helper()
	live, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil || !record.Configuration.Equal(live) {
		t.Fatalf("native acknowledgement differs from its immediately saved profile: %v", err)
	}
}

func testNativeCCDiscoveryRefusal(t *testing.T) {
	t.Helper()
	s, c := nativeCCTestCommittedSession(t)
	for _, line := range []string{"PASS", "REJECT", "PASS RULE", "REJECT RULE 10 MODE=CW", "PASS MODE CW", "REJECT BAND ALL", "PASS NEARBY ON", "REJECT NEARBY OFF", "PASS NEARBY", "REJECT NEARBY", "SET/FILTER NEARBY ON", "UNSET/FILTER NEARBY OFF", "SET/FILTER NEARBY/ON", "SET/FILTER NEARBY/OFF"} {
		before, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		revision, err := c.configurationRevisionToken()
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
		disk := presetDiskBytes(t, path)
		response, handled := s.filterEngine.Handle(c, line)
		after, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		afterRevision, err := c.configurationRevisionToken()
		if err != nil {
			t.Fatal(err)
		}
		if !handled || strings.Contains(response, "HELP RULE") || !strings.Contains(response, "No changes applied.") || len(response) > 1024 || !before.Equal(after) || afterRevision != revision || !bytes.Equal(disk, presetDiskBytes(t, path)) {
			t.Fatalf("native discovery input changed state or lost bounded native guidance: %q => %q", line, response)
		}
	}
}

func testNativeCCHumanRefusalAndSaveIntent(t *testing.T) {
	t.Helper()
	s, c := nativeCCTestCommittedSession(t)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk := presetDiskBytes(t, path)
	attempts := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
	for _, line := range []string{"PASS BAND 20 NOTABAND", "PASS MODE CW NOTAMODE", "PASS DXCONT EU INVALID", "PASS RULE 1 COMMENT=é", "PASS RULE 1 BAND=NOTABAND", "RESET FILTER RULE 9"} {
		outcome := s.filterEngine.stageOwned(newHumanPreferenceCandidate(c), line)
		if !outcome.handled || outcome.publish || outcome.forceSolar {
			t.Fatalf("native refusal acquired publication intent: %s => %+v", line, outcome)
		}
		if _, handled := s.filterEngine.Handle(c, line); !handled || attempts != 0 {
			t.Fatalf("native refusal attempted persistence: %s", line)
		}
	}
	refused := passRuleTestFilter(t, "BAND=40M")
	if _, handled := s.runHumanPreferenceOwned(c, "fixture", func(staged *Client, _ string) humanCommandOutcome {
		staged.updateFilter(func(f *filter.Filter) { *f = *refused })
		return humanHandled("Refused after candidate mutation\n")
	}); !handled || attempts != 0 {
		t.Fatal("a refusing native candidate reached persistence")
	}
	after, err := c.captureV7Configuration()
	if err != nil || !before.Equal(after) || !bytes.Equal(disk, presetDiskBytes(t, path)) {
		t.Fatal("discarded native candidate changed live or saved banks")
	}
	for _, line := range []string{"DIALECT", "DIALECT LIST", "DIALECT CC", "SHOW DEDUPE"} {
		if strings.HasPrefix(line, "DIALECT") {
			s.handleDialectCommand(c, line)
		} else {
			s.handleDedupeCommand(c, line)
		}
	}
	if attempts != 0 {
		t.Fatal("native read-only/unchanged settings attempted a save")
	}
	outcome := s.filterEngine.stageOwned(newHumanPreferenceCandidate(c), "SET/FILTER DOC/PASS 291")
	if !outcome.handled || !outcome.publish {
		t.Fatal("native replacement lost explicit save intent")
	}
	s.filterEngine.Handle(c, "SET/FILTER DOC/PASS 291")
	s.filterEngine.Handle(c, "SET/FILTER DOC/PASS 291")
	if attempts != 2 {
		t.Fatal("repeated native replacement stopped saving")
	}
}

func testNativeCCNoFilterRetainsGridAndInactiveNearby(t *testing.T) {
	t.Helper()
	s, c := nativeCCTestCommittedSession(t)
	configuration, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	configuration.Filters.GO.Rules = []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "NEARBY AND DXCONT=EU"}}
	configuration.Filters.CC.Geography["DXCTY"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{230}}}
	configuration.Settings.Grid = "FN31"
	canonical, _, runtime, err := filter.PrepareV7Configuration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks, c.configuredSettings = &canonical.Filters, canonical.Settings
	*c.filter = runtime
	c.grid, c.gridCell, c.gridCoarseCell = "FN31", pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	pointer, fine, coarse := c.filter, c.gridCell, c.gridCoarseCell
	candidate := spot.NewSpot("K1ABC", "VE3XYZ", 14030, "CW")
	candidate.IsHuman = true
	if c.filter.Matches(candidate) {
		t.Fatal("native restrictive control did not hide the candidate")
	}
	response, handled := s.filterEngine.Handle(c, "SET/NOFILTER")
	if !handled || !strings.Contains(response, "restored") || !c.filter.Matches(candidate) {
		t.Fatalf("native NOFILTER did not restore operator defaults: %q", response)
	}
	after, err := c.captureV7Configuration()
	if err != nil || !(filter.V6Configuration{Filters: canonical.Filters.GO}).Equal(filter.V6Configuration{Filters: after.Filters.GO}) || c.filter != pointer || c.grid != "FN31" || c.gridCell != fine || c.gridCoarseCell != coarse {
		t.Fatal("native NOFILTER changed inactive NEARBY rules or GRID ownership")
	}
	assertNativeCCTestSaved(t, c)
}

func testNativeCCTaxonomyPublication(t *testing.T, ziutek bool, field string) {
	t.Helper()
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	input := "set/test_mode\nset/unknown_mode\n"
	if field == "EVENT" {
		input = "pass rule 0 EVENT=test_event\npass rule 1 EVENT=unknown_event\n"
	}
	c, _ := taxonomyInputReader(t, input, ziutek)
	c.server, c.callsign = s, "W1ABC-15"
	attachNativeCCTestBanks(t, c, []string{"CW"})
	if response, handled := s.handlePresetCommand(c, "SAVE PRESET BASE"); !handled || strings.Contains(response, "failed") {
		t.Fatalf("native taxonomy baseline setup failed: %q", response)
	}
	baseline, pointer := c.v7PresetReferences.CC, c.filter
	writes := 0
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error { writes++; return prepared.Commit() }
	for i := range 2 {
		before, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
		disk := presetDiskBytes(t, path)
		line, err := c.readCommandLine(128)
		if err != nil {
			t.Fatal(err)
		}
		response, handled := s.filterEngine.Handle(c, line)
		after, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		if !handled || c.filter != pointer || c.v7PresetReferences.CC != baseline {
			t.Fatal("native taxonomy routing replaced pointer/baseline ownership")
		}
		if field == "MODE" && i == 0 {
			if writes != 1 || strings.Contains(response, "No changes applied.") || strings.Contains(response, "warning") {
				t.Fatalf("operator-enabled native mode did not publish/save once: %q", response)
			}
			assertNativeCCTestSaved(t, c)
			for _, probe := range []struct {
				mode string
				want bool
			}{{"CW", true}, {"TEST_MODE", true}, {"UNKNOWN", false}} {
				if got := c.filter.Matches(spot.NewSpot("K1ABC", "W1XYZ", 14074, probe.mode)); got != probe.want {
					t.Fatalf("native configured mode %s: %t", probe.mode, got)
				}
			}
		} else if !strings.Contains(response, "No changes applied.") || !before.Equal(after) || !bytes.Equal(disk, presetDiskBytes(t, path)) || writes != map[string]int{"MODE": 1, "EVENT": 0}[field] {
			t.Fatalf("unknown/GO-only taxonomy input changed native state: %q", response)
		}
	}
}
