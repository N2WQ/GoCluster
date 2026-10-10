package telnet

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// Composed evidence exercises the real codec and dispatch owners, not the
// server accept/writer loops. The cumulative checkout fixture owns that claim.
type helpOwnerSession struct {
	server        *Server
	client        *Client
	writes, reads int
}

func newHelpOwnerSession(t *testing.T, dialect DialectName) *helpOwnerSession {
	t.Helper()
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	s.nowFn = func() time.Time { return time.Date(2026, 10, 10, 12, 7, 0, 0, time.UTC) }
	c := configurationTestClient(s, "W1ABC-18")
	c.dialect, c.noiseClass = dialect, "QUIET"
	c.filter.Reset()
	c.filter.AllBands, c.filter.Bands = false, map[string]bool{"20m": true}
	c.filter.AllModes, c.filter.Modes = false, map[string]bool{"CW": true}
	c.filter.Comments = []string{"CQ DX"}
	c.filter.MinSNR = map[string]int{"CW": 10}
	c.configuredSettings = filter.SettingsConfiguration{Dialect: string(dialect), NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 15}
	c.configurationInitialized = true
	c.setDedupePolicy(dedupePolicyFast)
	c.setSolarSummaryMinutes(15, s.now())
	c.setDiagMode(diagModeSource)
	s.clients = map[string]*Client{c.callsign: c}
	x := &helpOwnerSession{server: s, client: c}
	row := spot.NewSpot("K1ABC", "W2ABC", 14030, "CW")
	row.Comment, row.IsHuman = "CQ DX", true
	s.processor = commands.NewProcessor(nil, historyReaderFunc(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		x.reads++
		if req.Limit != 1 || req.Match == nil || !req.Match(row) {
			t.Error("SH history did not reach count-one matching archive predicate")
		}
		decoy := spot.NewSpot("K1ABC", "W2ABC", 7030, "CW")
		decoy.Comment, decoy.IsHuman = "CQ DX", true
		if req.Match != nil && req.Match(decoy) {
			t.Error("archive predicate accepted wrong-band decoy")
		}
		return archive.HistoryPage{Spots: []*spot.Spot{row}, Before: []byte("retained-position"), End: archive.HistoryBudgetReached}, nil
	}), nil, nil, nil, nil, commands.WithBuildInfo(commands.BuildInfo{Version: "help-owner-18"}))
	if response, handled := s.handlePresetCommand(c, "SAVE PRESET BASE"); !handled || response != "Saved preset BASE for W1ABC.\n" {
		t.Fatalf("initial real preset setup failed: %q", response)
	}
	if _, err := c.configurationRevisionToken(); err != nil {
		t.Fatal(err)
	}
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		x.writes++
		return prepared.Commit()
	}
	return x
}

func helpOwnerInput(t *testing.T, command string, ziutek bool) string {
	t.Helper()
	c, _ := taxonomyInputReader(t, strings.ToLower(command)+"\n", ziutek)
	line, err := c.readCommandLine(128)
	if err != nil || line != strings.ToUpper(command) {
		t.Fatalf("real codec changed or refused %q: %q / %v", command, line, err)
	}
	return line
}

func helpOwnerControl(t *testing.T, c *Client) string {
	t.Helper()
	select {
	case message := <-c.controlChan:
		if len(c.controlChan) != 0 {
			t.Fatal("owner queued more than one response")
		}
		return string(message.raw) + message.line
	default:
		t.Fatal("owner did not queue a response")
		return ""
	}
}

func helpOwnerFiles(t *testing.T) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(filter.UserDataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = data
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func (x *helpOwnerSession) assertSaved(t *testing.T) {
	t.Helper()
	c := x.client
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	saved := filter.ConfigurationFromFilter(&record.Filter, filter.SettingsConfiguration{
		Dialect: record.Dialect, Grid: record.Grid, NoiseClass: record.NoiseClass, DedupePolicy: record.DedupePolicy,
		PathMinObservationCount: record.PathMinObservationCount, SolarSummaryMinutes: record.SolarSummaryMinutes,
	})
	if !saved.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || record.Preset == nil || c.presetReference == nil ||
		record.Preset.Name != c.presetReference.Name || !filter.ConfigurationFromPreset(record.Preset.Baseline).Equal(filter.ConfigurationFromPreset(c.presetReference.Baseline)) {
		t.Fatal("owner acknowledgement did not match immediately saved configuration/association")
	}
}

func TestHelpRoutingRealReadbackSettingPresetAndHistoryOwners(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, ziutek := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ziutek=%t", dialect, ziutek), func(t *testing.T) {
				x := newHelpOwnerSession(t, dialect)
				s, c := x.server, x.client
				pointer := c.filter
				for _, command := range []string{"SHOW FILTER BAND", "SHOW/FILTER BAND", "SH/FILTER BAND"} {
					if !s.handleHumanReadback(c, helpOwnerInput(t, command, ziutek)) {
						t.Fatalf("real readback owner rejected %q", command)
					}
					response := helpOwnerControl(t, c)
					if !strings.Contains(response, "Bands\r\n  PASS: 20m\r\n  REJECT: NONE\r\n") {
						t.Fatalf("alias did not read actual selected band: %q", response)
					}
				}
				response, handled := s.handleSolarCommand(c, helpOwnerInput(t, "SET SOLAR 30", ziutek))
				if !handled || response != "Solar cadence: 30 minutes; unavailable on this server\n" || x.writes != 1 || c.configuredSettings.SolarSummaryMinutes != 30 {
					t.Fatalf("SOLAR owner did not publish/persist literal cadence: %q", response)
				}
				x.assertSaved(t)
				if response, handled = s.handlePresetCommand(c, helpOwnerInput(t, "SAVE PRESET SNAP", ziutek)); !handled || response != "Saved preset SNAP for W1ABC.\n" {
					t.Fatalf("real SAVE refused: %q", response)
				}
				stored, err := filter.LoadPreset(c.callsign, "SNAP")
				if err != nil || stored.SolarSummaryMinutes != 30 || stored.NoiseClass != "QUIET" {
					t.Fatalf("SAVE did not write selected snapshot: %+v / %v", stored, err)
				}
				if response, handled = s.handlePathSettingsCommand(c, "SET NOISE RURAL"); !handled || c.configuredSettings.NoiseClass != "RURAL" {
					t.Fatalf("LOAD positive control did not establish different live noise: %q", response)
				}
				if response, handled = s.handlePresetCommand(c, helpOwnerInput(t, "LOAD PRESET SNAP", ziutek)); !handled || !strings.HasPrefix(response, "Loaded preset SNAP; defaults saved for W1ABC-18.\n") || c.configuredSettings.NoiseClass != "QUIET" || c.configuredSettings.SolarSummaryMinutes != 30 {
					t.Fatalf("real LOAD did not restore saved selection: %q", response)
				}
				x.assertSaved(t)
				if response, handled = s.handlePresetCommand(c, "LIST PRESET"); !handled || response != "Saved presets for W1ABC (2/20):\nBASE\nSNAP\n" {
					t.Fatalf("real LIST did not expose durable names: %q", response)
				}
				if response, handled = s.handlePresetCommand(c, helpOwnerInput(t, "DELETE PRESET SNAP", ziutek)); !handled || response != "Deleted preset SNAP.\n" {
					t.Fatalf("real DELETE refused: %q", response)
				}
				if names, err := filter.ListPresets(c.callsign); err != nil || !slices.Equal(names, []string{"BASE"}) {
					t.Fatalf("DELETE acknowledgement hid unchanged library: %v / %v", names, err)
				}
				if c.filter != pointer {
					t.Fatal("owner replaced the live filter pointer")
				}
				if !s.handleHumanReadback(c, "SHOW SETTINGS") || !strings.Contains(helpOwnerControl(t, c), "Solar         30 minutes; unavailable on this server\r\n") {
					t.Fatal("SHOW SETTINGS did not reach effective/configured owner")
				}
				for _, command := range []string{"SH BUILD", "SH OWN"} {
					response = s.processor.ProcessCommandForClient(helpOwnerInput(t, command, ziutek), c.callsign, "", nil, string(dialect))
					if command == "SH BUILD" && !strings.Contains(response, "Build version: help-owner-18\n") || command == "SH OWN" && !strings.Contains(response, "W1ABC-18") {
						t.Fatalf("SH owner was not executable: %q -> %q", command, response)
					}
				}
				if !strings.Contains(runHistory(t, s, c, helpOwnerInput(t, "SH MYDX 1", ziutek)), "K1ABC") || x.reads != 1 || c.historySearch == nil {
					t.Fatal("SH MYDX did not reach archive/cursor owner")
				}
				if dispatch := s.handleMachineCommand(c, "GET YAML CONFIG SCHEMA 4 ID owner-18"); !dispatch.handled || dispatch.terminal {
					t.Fatal("GET argument header did not reach machine owner")
				}
				var document struct {
					SchemaVersion int                  `yaml:"schema_version"`
					RequestID     string               `yaml:"request_id"`
					Configuration filter.Configuration `yaml:"configuration"`
				}
				if err := yaml.Unmarshal([]byte(helpOwnerControl(t, c)), &document); err != nil || document.SchemaVersion != 4 || document.RequestID != "owner-18" || document.Configuration.Settings.SolarSummaryMinutes != 30 {
					t.Fatalf("actual machine readback did not preserve correlated setting: %+v / %v", document, err)
				}
				x.assertHelpHasNoEffects(t, ziutek)
			})
		}
	}
}

func (x *helpOwnerSession) assertHelpHasNoEffects(t *testing.T, ziutek bool) {
	t.Helper()
	s, c := x.server, x.client
	if _, err := c.configurationRevisionToken(); err != nil {
		t.Fatal(err)
	}
	before := captureHistoryCommentAliasState(t, c)
	files, runtime, next := helpOwnerFiles(t), c.pathSnapshot(), c.solarNextSummaryAt
	diag, writes, reads := c.getDiagMode(), x.writes, x.reads
	filterTopic, historyTopic, allowTopic := "SHOW FILTER", "SHOW DX", "PASS"
	if c.dialect == DialectCC {
		filterTopic, historyTopic, allowTopic = "SHOW/FILTER", "SHOW/DX", "SET/FILTER"
	}
	for _, tc := range []struct{ topic, canonical, content string }{
		{"SH BUILD", "SHOW BUILD", "Usage: SHOW BUILD"}, {"SH OWN", "SHOW OWN", "Usage: SHOW OWN"},
		{"SH MYDX 1", "SHOW MYDX", "SHOW MYDX NEXT <cursor>"}, {"SH DX 1", historyTopic, historyTopic + " NEXT <cursor>"},
		{"SHOW/FILTER BAND", filterTopic, filterTopic + " <category>"}, {"SH/FILTER COMMENT", "SHOW FILTER COMMENT", "SHOW FILTER COMMENT"},
		{"SHOW SETTINGS", "SHOW SETTINGS", "Usage: SHOW SETTINGS"}, {"PASS BAND 20", allowTopic, allowTopic + " <"},
		{"PASS MINSNR CW 10", "PASS MINSNR", "PASS MINSNR <mode[,mode...]|ALL>"},
		{"REJECT NEARBY OFF", "PASS NEARBY", "PASS NEARBY ON|OFF"},
		{"SET SOLAR 60", "SET SOLAR", "SET SOLAR <15|30|60|OFF>"},
		{"SAVE PRESET WOULD-WRITE", "SAVE PRESET", "SAVE PRESET <name>"},
		{"LOAD PRESET BASE", "LOAD PRESET", "LOAD PRESET <name>"}, {"DELETE PRESET BASE", "DELETE PRESET", "DELETE PRESET <name>"},
		{"GET YAML CONFIG SCHEMA 4 ID help-18", "GET YAML CONFIG", "GET YAML CONFIG [SCHEMA 2|3|4] [ID <id>]"},
	} {
		line := helpOwnerInput(t, "HELP "+tc.topic, ziutek)
		assertHelpOwnerInterceptorsDecline(t, s, c, line)
		response := s.processor.ProcessCommandForClient(line, c.callsign, "", nil, string(c.dialect))
		if !strings.HasPrefix(response, tc.canonical+" - ") || !strings.Contains(response, tc.content) {
			t.Fatalf("HELP failed specific owner route %q: %q", tc.topic, response)
		}
		before.assertOwners(t, c, x.writes-writes)
		after := captureHistoryCommentAliasState(t, c)
		if x.reads != reads || !reflect.DeepEqual(files, helpOwnerFiles(t)) || c.pathSnapshot() != runtime || !c.solarNextSummaryAt.Equal(next) || c.getDiagMode() != diag ||
			before.search != after.search || before.query != after.query || before.token != after.token || !bytes.Equal(before.position, after.position) ||
			before.unreadable != after.unreadable || before.hadMatches != after.hadMatches || before.generation != after.generation || before.closed != after.closed {
			t.Fatal("HELP changed runtime, cursor, archive calls or preset library bytes")
		}
	}
}

func assertHelpOwnerInterceptorsDecline(t *testing.T, s *Server, c *Client, line string) {
	t.Helper()
	if dispatch := s.handleMachineCommand(c, line); dispatch.handled || s.handleHumanReadback(c, line) {
		t.Fatal("HELP entered machine/readback owner")
	}
	for _, owner := range []func(*Client, string) (string, bool){
		s.handleReadPauseCommand, s.handleDialectCommand, s.handlePathSettingsCommand, s.handleDedupeCommand,
		s.handleDiagCommand, s.handleSolarCommand, s.handleShowPropCommand, s.handlePresetCommand, s.filterEngine.Handle,
	} {
		if response, handled := owner(c, line); handled || response != "" {
			t.Fatalf("HELP entered an intercepted command owner: %q", response)
		}
	}
	if handled, proceed := s.handleHistoryCommand(c, line); handled || !proceed {
		t.Fatal("HELP entered history/cursor owner")
	}
}

func TestHelpRoutingLoadedShortcutAliasesReachTheirRealOwners(t *testing.T) {
	// The loaded snapshot and owner tests are sequential; restore before any
	// other taxonomy consumer runs. Fixed ANN/NOFILTER overlap loaded aliases.
	const definition = "modes:\n" +
		"  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\n" +
		"  - name: TEST_MODE\n    filter_visible: true\n    cc_shortcut: true\n    variants: [MODE_ALIAS, ANN, NOFILTER]\n" +
		"  - name: DISABLED_MODE\n    filter_visible: true\n    cc_shortcut: false\n" +
		"events: []\n"
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	if err := os.WriteFile(path, []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
	x := newHelpOwnerSession(t, DialectCC)
	c, engine := x.client, x.server.filterEngine
	run := func(command string) {
		t.Helper()
		writes := x.writes
		response, handled := engine.Handle(c, command)
		if !handled || x.writes != writes+1 || response == "" || strings.Contains(response, "Usage:") || strings.Contains(response, "Unknown") || strings.Contains(response, "Invalid") {
			t.Fatalf("loaded shortcut did not reach/persist owner: %q -> %q", command, response)
		}
		x.assertSaved(t)
	}
	run("SET/MODE_ALIAS")
	row := spot.NewSpot("K1ABC", "W2ABC", 14030, "TEST_MODE")
	row.Comment, row.IsHuman = "CQ DX", true
	if !c.filter.Modes["TEST_MODE"] || !c.filter.Modes["CW"] || !c.filter.Matches(row) {
		t.Fatal("loaded alias did not select canonical mode and actual visibility")
	}
	run("SET/NOTEST_MODE")
	if !c.filter.BlockModes["TEST_MODE"] || c.filter.Matches(row) || !c.filter.Modes["CW"] {
		t.Fatal("negative loaded shortcut did not block canonical mode")
	}
	modes := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone().Filters.Modes
	run("SET/NOANN")
	run("SET/ANN")
	if !c.filter.AnnounceEnabled() || !reflect.DeepEqual(modes, filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Filters.Modes) {
		t.Fatal("fixed ANN owner lost to loaded mode alias")
	}
	run("SET/NOFILTER")
	if !c.filter.AllModes || len(c.filter.BlockModes) != 0 || len(c.filter.Comments) != 0 || !c.filter.AllBands {
		t.Fatal("fixed NOFILTER owner lost to loaded mode alias")
	}
	for _, command := range []string{"SET/DISABLED_MODE", "SET/NODISABLED_MODE", "SET/UNKNOWN_MODE", "SET/TEST_MODE EXTRA"} {
		before, files, writes := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone(), helpOwnerFiles(t), x.writes
		owners := captureHistoryCommentAliasState(t, c)
		response, handled := engine.Handle(c, command)
		if command == "SET/TEST_MODE EXTRA" && (!handled || response != "Usage: SET/TEST_MODE\nNo changes applied.\n") {
			t.Fatalf("execution accepted ignored shortcut tail: %q", response)
		}
		if command != "SET/TEST_MODE EXTRA" && (handled || response != "") {
			t.Fatalf("ineligible shortcut acquired an executable owner: %q -> %q", command, response)
		}
		if !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || writes != x.writes || !reflect.DeepEqual(files, helpOwnerFiles(t)) {
			t.Fatalf("ineligible shortcut had effects: %q -> %q", command, response)
		}
		owners.assertOwners(t, c, x.writes-writes)
	}
}
