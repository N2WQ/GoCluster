package telnet

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/commands"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

// Execute the rendered recipes, but keep expected spot outcomes literal and
// independent of their text. A synchronized presentation fixture is no oracle
// for the filters that a recipe actually creates.
func helpTaskCommandBlocks(text string) [][]string {
	var blocks [][]string
	var block []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		command := line != "" && line == strings.ToUpper(line) &&
			(strings.HasPrefix(line, "RESET FILTER") || strings.HasPrefix(line, "PASS RULE ") ||
				strings.HasPrefix(line, "REJECT RULE ") || strings.HasPrefix(line, "SET/") ||
				strings.HasPrefix(line, "SHOW FILTER") || strings.HasPrefix(line, "SHOW/FILTER") || line == "RESUME")
		if command {
			block = append(block, line)
		} else if len(block) != 0 {
			blocks = append(blocks, block)
			block = nil
		}
	}
	if len(block) != 0 {
		blocks = append(blocks, block)
	}
	return blocks
}

func helpTaskTestClient(t *testing.T, dialect DialectName) (*Server, *Client) {
	t.Helper()
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	s.processor = commands.NewProcessor(nil, nil, nil, nil, nil, nil)
	s.ctyLookup = func() *cty.CTYDatabase {
		return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{"K": {Prefix: "K", ADIF: 291}, "VE": {Prefix: "VE", ADIF: 1}}}
	}
	c := v7CurrentTestClient(t, s, "W1ABC-1")
	cfg := helpTaskConfiguration(t, c)
	cfg.Settings.Dialect, cfg.Settings.NoiseClass = string(dialect), "QUIET"
	cfg.Filters.GO.Rules = []filter.RuleDefinition{
		{Slot: 7, Action: filter.RulePass, Expr: "BAND=80M"},
		{Slot: 9, Action: filter.RuleReject, Expr: "DXCALL=K1*"},
	}
	cfg.Filters.GO.IncludeBeacons, cfg.Filters.GO.AllowSelf = filter.DefaultBoolFalse, filter.DefaultBoolFalse
	cfg.Filters.CC = filter.CCConfiguration{
		Modes: []string{"CW", "FT8"}, Sources: []string{"HUMAN"},
		Geography: map[string]filter.CCGeography{
			"DOC": {Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{14}},
				Bands: map[string]filter.CCCategory{"20": {Action: filter.CCPass, Countries: []int{230}}}},
			"DXCTY": {Global: filter.CCCategory{Action: filter.CCReject, Countries: []int{230}}},
		},
		DXBM:           filter.CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{"80-CW": true}},
		IncludeBeacons: filter.DefaultBoolFalse, AllowWWV: filter.DefaultBoolTrue,
		AllowWCY: filter.DefaultBoolDefault, AllowAnnounce: filter.DefaultBoolFalse,
		AllowSelf: filter.DefaultBoolFalse, AllowToxic: filter.DefaultBoolFalse,
	}
	canonical, goFilter, ccFilter, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = goFilter
	if dialect == DialectCC {
		*c.filter = ccFilter
	}
	c.filterBanks, c.dialect, c.configuredSettings, c.noiseClass = &canonical.Filters, dialect, canonical.Settings, "QUIET"
	if err := c.saveFilterOwned(); err != nil {
		t.Fatal(err)
	}
	return s, c
}

func helpTaskRun(t *testing.T, s *Server, c *Client, line string) string {
	t.Helper()
	if s.handleHumanReadback(c, line) {
		select {
		case message := <-c.controlChan:
			message.configurationReservation.Release()
			c.completeHumanReadback(message.readback, s.now())
			body := string(message.raw)
			if !strings.Contains(body, "Missed spots aren't replayed.") {
				t.Fatalf("%s lost complete readback: %q", line, body)
			}
			return body
		default:
			t.Fatalf("%s did not queue its readback", line)
		}
	}
	for _, owner := range []func(*Client, string) (string, bool){
		s.handleReadPauseCommand, s.handlePresetCommand, s.handlePathSettingsCommand, s.filterEngine.Handle,
	} {
		if body, handled := owner(c, line); handled {
			for _, bad := range []string{"Invalid", "Usage:", "Unknown", "failed", "unavailable", "not saved", "No changes applied"} {
				if strings.Contains(body, bad) {
					t.Fatalf("%q refused: %q", line, body)
				}
			}
			if strings.TrimSpace(body) == "" {
				t.Fatalf("%q returned an empty acknowledgement", line)
			}
			return body
		}
	}
	t.Fatalf("guide command has no owner: %s", line)
	return ""
}

func helpTaskConfiguration(t *testing.T, c *Client) filter.V7Configuration {
	t.Helper()
	cfg, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func helpTaskSaved(t *testing.T, c *Client, want filter.V7Configuration) {
	t.Helper()
	got := helpTaskConfiguration(t, c)
	record, err := filter.LoadV7UserRecord(c.callsign)
	if !got.Equal(want) || err != nil || !record.Configuration.Equal(want) {
		t.Fatalf("wrong selections or missing immediate save: %v; got=%+v want=%+v", err, got, want)
	}
}

func helpTaskRecipes(t *testing.T, s *Server, c *Client) [][]string {
	t.Helper()
	text := s.processor.ProcessCommandForClient("HELP FILTER EXAMPLES", c.callsign, "", nil, string(c.dialect))
	blocks := helpTaskCommandBlocks(text)
	if len(blocks) != 4 {
		t.Fatalf("guide must expose four complete recipes, got %d: %s", len(blocks), text)
	}
	show := "SHOW FILTER"
	if c.dialect == DialectCC {
		show = "SHOW/FILTER"
	}
	for _, block := range blocks {
		if len(block) < 3 || block[len(block)-2] != show || block[len(block)-1] != "RESUME" {
			t.Fatalf("guide recipe omits inspection or resume: %v", block)
		}
	}
	return blocks
}

func TestHelpFilterRecipesCurrentBanks(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for recipe := range 4 {
			t.Run(string(dialect)+"/"+strconv.Itoa(recipe+1), func(t *testing.T) {
				s, c := helpTaskTestClient(t, dialect)
				before := helpTaskConfiguration(t, c)
				for _, line := range helpTaskRecipes(t, s, c)[recipe] {
					helpTaskRun(t, s, c, line)
				}
				want := before.Clone()
				if dialect == DialectGo {
					want.Filters.GO.Rules = nil
					if recipe < 3 {
						actions := []filter.RuleAction{filter.RulePass, filter.RulePass, filter.RuleReject}
						expressions := []string{"BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN", "BAND=20M,40M AND MODE=CW AND SOURCE=SKIMMER", "MODE=FT8"}
						want.Filters.GO.Rules = []filter.RuleDefinition{{Slot: 0, Action: actions[recipe], Expr: expressions[recipe]}}
					}
				} else {
					want.Filters.CC = helpTaskExpectedCC(t, before.Filters.CC, recipe)
				}
				helpTaskSaved(t, c, want)
				if active, _, _ := c.readPauseStatus(s.now()); active {
					t.Fatal("RESUME left live spots paused")
				}
				helpTaskMatchVectors(t, c, recipe)
			})
		}
	}
}

func helpTaskExpectedCC(t *testing.T, before filter.CCConfiguration, recipe int) filter.CCConfiguration {
	t.Helper()
	want := before.Clone()
	switch recipe {
	case 0:
		want.DXBM = filter.CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{
			"20-CW": true, "20-RTTY": true, "20-SSB": true,
			"40-CW": true, "40-RTTY": true, "40-SSB": true,
		}}
	case 1:
		want.Geography["DOC"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{1, 291}}, Bands: map[string]filter.CCCategory{}}
	case 2:
		want.Modes = []string{"CW"}
	case 3:
		defaults, err := filter.CurrentDefaultCCConfiguration()
		if err != nil {
			t.Fatal(err)
		}
		want.Modes, want.Sources, want.Geography, want.DXBM = defaults.Modes, defaults.Sources, defaults.Geography, defaults.DXBM
	}
	return want
}

func helpTaskMatchVectors(t *testing.T, c *Client, recipe int) {
	t.Helper()
	for _, probe := range []struct {
		frequency float64
		mode      string
		human     bool
		country   int
		goWant    [4]bool
		ccWant    [3]bool
	}{
		{14025, "CW", true, 230, [4]bool{true, false, true, true}, [3]bool{true, false, false}},
		{7074, "FT8", true, 14, [4]bool{true, false, false, true}, [3]bool{true, false, false}},
		{14025, "CW", false, 230, [4]bool{false, true, true, true}, [3]bool{false, false, false}},
		{21025, "CW", true, 291, [4]bool{false, false, true, true}, [3]bool{false, false, false}},
		{3520, "CW", true, 291, [4]bool{false, false, true, true}, [3]bool{false, true, false}},
		{3520, "CW", true, 14, [4]bool{false, false, true, true}, [3]bool{false, false, true}},
		{3520, "FT8", true, 14, [4]bool{false, false, false, true}, [3]bool{false, false, false}},
	} {
		sp := spot.NewSpot("K1ABC", "N2ABC", probe.frequency, probe.mode)
		sp.IsHuman, sp.DEMetadata.ADIF, sp.DXMetadata.ADIF = probe.human, probe.country, 291
		want := probe.goWant[recipe]
		if c.dialect == DialectCC {
			if recipe == 3 {
				// The exact operator mode/source selections are checked above.
				// These ordinary CW reports witness cleared geography and bands.
				if !probe.human || probe.mode != "CW" {
					continue
				}
				want = true
			} else {
				want = probe.ccWant[recipe]
			}
		}
		if got := c.filter.Matches(sp); got != want {
			t.Fatalf("recipe %d probe %+v matched=%t, want=%t", recipe, probe, got, want)
		}
	}
}

func TestHelpTaskPagesHaveNoConfigurationEffects(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s, c := helpTaskTestClient(t, dialect)
			before, files := helpTaskConfiguration(t, c), helpOwnerFiles(t)
			for _, topic := range []string{"FILTERS", "FILTER EXAMPLES", "PRESETS"} {
				line := "HELP " + topic
				assertHelpOwnerInterceptorsDecline(t, s, c, line)
				text := s.processor.ProcessCommandForClient(line, c.callsign, "", nil, string(dialect))
				if text == "" || strings.HasPrefix(text, "Unknown help topic:") {
					t.Fatalf("guide not routed: %s", line)
				}
				if !before.Equal(helpTaskConfiguration(t, c)) || !reflect.DeepEqual(files, helpOwnerFiles(t)) {
					t.Fatalf("%s changed configuration or saved bytes", line)
				}
			}
		})
	}
}

func helpTaskPresetCommands(t *testing.T, s *Server, c *Client) map[string]string {
	t.Helper()
	text := s.processor.ProcessCommandForClient("HELP PRESETS", c.callsign, "", nil, string(c.dialect))
	result := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		command, _, _ := strings.Cut(line, "  ")
		for _, verb := range []string{"SAVE", "LIST", "LOAD", "DELETE"} {
			if strings.HasPrefix(command, verb+" PRESET") && command == strings.ToUpper(command) {
				result[command] = command
			}
		}
	}
	for _, line := range []string{"SAVE PRESET DAY", "LIST PRESET", "LOAD PRESET DAY", "DELETE PRESET DAY"} {
		if result[line] == "" {
			t.Fatalf("preset guide lost executable step %q", line)
		}
		for _, verb := range []string{"SAVE", "LOAD", "DELETE"} {
			result[verb+" PRESET NIGHT"] = strings.ReplaceAll(result[verb+" PRESET DAY"], "DAY", "NIGHT")
		}
	}
	return result
}

func TestHelpPresetGuideSwitchSharedSSIDAndRestore(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s, c := helpTaskTestClient(t, dialect)
			steps, recipes := helpTaskPresetCommands(t, s, c), helpTaskRecipes(t, s, c)
			for _, line := range recipes[0] {
				helpTaskRun(t, s, c, line)
			}
			day := helpTaskConfiguration(t, c)
			helpTaskRun(t, s, c, steps["SAVE PRESET DAY"])
			for _, line := range recipes[1] {
				helpTaskRun(t, s, c, line)
			}
			helpTaskRun(t, s, c, "SET NOISE URBAN")
			night := helpTaskConfiguration(t, c)
			helpTaskRun(t, s, c, steps["SAVE PRESET NIGHT"])
			listing := helpTaskRun(t, s, c, steps["LIST PRESET"])
			if !strings.Contains(listing, "DAY (") || !strings.Contains(listing, "NIGHT (") {
				t.Fatal("saved presets missing from LIST")
			}
			for _, restore := range []struct {
				name string
				want filter.V7Configuration
			}{{"DAY", day}, {"NIGHT", night}} {
				helpTaskRun(t, s, c, steps["LOAD PRESET "+restore.name])
				helpTaskSaved(t, c, restore.want)
			}
			helpTaskSharedPresetRestore(t, s, c, day, steps["LOAD PRESET DAY"])
			// Reusing DAY replaces the shared name, not the live selection.
			helpTaskRun(t, s, c, steps["SAVE PRESET DAY"])
			stored, err := filter.LoadV7Preset("W1ABC-2", "DAY")
			if err != nil || !stored.MatchesConfiguration(night) {
				t.Fatalf("SAVE did not overwrite the shared name: %v", err)
			}
			helpTaskRun(t, s, c, steps["DELETE PRESET NIGHT"])
			if _, err := filter.LoadV7Preset(c.callsign, "NIGHT"); err == nil {
				t.Fatal("DELETE left the named preset")
			}
			helpTaskSaved(t, c, night)
		})
	}
}

func helpTaskSharedPresetRestore(t *testing.T, s *Server, origin *Client, day filter.V7Configuration, command string) {
	t.Helper()
	other := v7CurrentTestClient(t, s, "W1ABC-2")
	opposite := "CC"
	if origin.dialect == DialectCC {
		opposite = "GO"
	}
	if body, handled := s.handleDialectCommand(other, "DIALECT "+opposite); !handled || strings.Contains(body, "failed") {
		t.Fatalf("cannot establish the other active style: %q", body)
	}
	before := helpTaskConfiguration(t, other)
	helpTaskRun(t, s, other, command)
	want := before.Clone()
	want.Settings = day.Settings
	if origin.dialect == DialectGo {
		want.Filters.GO = day.Filters.GO
	} else {
		want.Filters.CC = day.Filters.CC
	}
	if other.dialect != origin.dialect {
		t.Fatal("LOAD did not switch to the saved style")
	}
	helpTaskSaved(t, other, want)
	restored := configurationTestClient(s, other.callsign)
	if result := s.restoreV7ClientRecordOwned(restored, time.Now().UTC()); result.loadError != nil {
		t.Fatalf("future login could not restore loaded setup: %v", result.loadError)
	}
	if !helpTaskConfiguration(t, restored).Equal(want) || restored.dialect != origin.dialect {
		t.Fatal("future login lost loaded setup or saved style")
	}
}
