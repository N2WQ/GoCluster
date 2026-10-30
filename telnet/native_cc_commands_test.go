package telnet

import (
	"slices"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func nativeCCCommandFixture(t *testing.T) (*Client, *filterCommandEngine, *int) {
	t.Helper()
	config := filter.CCConfiguration{Modes: []string{"CW", "FT8", "UNKNOWN"}, Sources: []string{"HUMAN", "SKIMMER"},
		IncludeBeacons: filter.DefaultBoolFalse, AllowWWV: filter.DefaultBoolTrue,
		AllowWCY: filter.DefaultBoolDefault, AllowSelf: filter.DefaultBoolTrue,
		AllowAnnounce: filter.DefaultBoolFalse, AllowToxic: filter.DefaultBoolFalse}
	_, value, err := filter.NewCCFilter(config)
	if err != nil {
		t.Fatal(err)
	}
	lookups := new(int)
	database := &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
		"K": {Prefix: "K", ADIF: 291}, "VE": {Prefix: "VE", ADIF: 1},
		"ROTUMA": {Prefix: "3D2/R", ADIF: 460}, "W1ABC": {Prefix: "K", ADIF: 291},
	}}
	s := &Server{commandLineLimit: 128, ctyLookup: func() *cty.CTYDatabase { *lookups++; return database }}
	c := &Client{server: s, callsign: "N0CALL-7", dialect: DialectCC, filter: &value,
		grid: "FN31", noiseClass: "URBAN", configurationInitialized: true,
		configuredSettings: filter.SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN"}}
	return c, newFilterCommandEngine(), lookups
}

func nativeCCConfiguration(t *testing.T, c *Client) filter.CCConfiguration {
	t.Helper()
	config, exists := c.filter.NativeCCConfiguration()
	if !exists {
		t.Fatal("fixture lost native CC ownership")
	}
	return config
}

func assertNativeCCRefused(t *testing.T, c *Client, engine *filterCommandEngine, line string) {
	t.Helper()
	before, pointer, settings := nativeCCConfiguration(t, c), c.filter, c.configuredSettings
	grid, revision := c.grid, c.configurationRevision
	outcome := engine.stageNativeCCFilterCommand(c, line)
	if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "No changes applied.") || strings.Contains(outcome.response, "HELP RULE") {
		t.Fatalf("native refusal contract for %q: %+v", line, outcome)
	}
	if !before.Equal(nativeCCConfiguration(t, c)) || pointer != c.filter || settings != c.configuredSettings || grid != c.grid || revision != c.configurationRevision {
		t.Fatalf("refused native command mutated its candidate: %s", line)
	}
	assertBooleanHumanWidth(t, outcome.response)
}

func TestNativeCCGlobalGeographyGrammarAndCanonicalResolution(t *testing.T) {
	for _, test := range []struct {
		line, family string
		action       filter.CCAction
		countries    []int
		states       []string
		lookupCount  int
	}{
		{"SET/FILTER DOC/PASS K,VE", "DOC", filter.CCPass, []int{1, 291}, nil, 1},
		{"set/filter doc /reject 291 248", "DOC", filter.CCReject, []int{248, 291}, nil, 0},
		{"SET/FILTER DXCTY/PASS 3D2/R", "DXCTY", filter.CCPass, []int{460}, nil, 1},
		{"SET/FILTER K,VE/PASS", "DOC", filter.CCPass, []int{1, 291}, nil, 1},
		{"SET/FILTER 3D2/R/PASS", "DOC", filter.CCPass, []int{460}, nil, 1},
		{"SET/FILTER DOS/PASS ca,ON", "DOS", filter.CCPass, nil, []string{"CA", "ON"}, 0},
		{"SET/FILTER DXSTATE /REJECT QC TX", "DXSTATE", filter.CCReject, nil, []string{"QC", "TX"}, 0},
		{"SET/FILTER DOC/PASS K,291,K", "DOC", filter.CCPass, []int{291}, nil, 1},
	} {
		t.Run(test.line, func(t *testing.T) {
			c, engine, calls := nativeCCCommandFixture(t)
			before, pointer := nativeCCConfiguration(t, c), c.filter
			prior := *c.filter
			outcome := engine.stageNativeCCFilterCommand(c, test.line)
			if !outcome.handled || !outcome.publish || outcome.warning != humanWarningFilter || strings.Contains(outcome.response, "No changes") {
				t.Fatalf("accepted native command: %+v", outcome)
			}
			after := nativeCCConfiguration(t, c)
			row := after.Geography[test.family].Global
			if row.Action != test.action || !slices.Equal(row.Countries, test.countries) || !slices.Equal(row.States, test.states) || *calls != test.lookupCount || pointer != c.filter {
				t.Fatalf("wrong canonical category or resolver ownership: %+v calls=%d", row, *calls)
			}
			old, _ := prior.NativeCCConfiguration()
			if !old.Equal(before) || after.AllowSelf != before.AllowSelf || after.AllowToxic != before.AllowToxic {
				t.Fatal("staged edit changed prior immutable state or separate controls")
			}
			assertBooleanHumanWidth(t, outcome.response)
		})
	}
}

func TestNativeCCInvalidCommandsAreAtomicAndDoNotLookupNumericOverflow(t *testing.T) {
	c, engine, calls := nativeCCCommandFixture(t)
	for _, line := range []string{
		"PASS RULE 0 MODE=CW", "REJECT RULE 4 SOURCE=HUMAN", "RESET FILTER RULE ALL", "RESET FILTER",
		"SET/FILTER DOC/PASS", "SET/FILTER DOC/REJECT", "SET/FILTER DOC/OFF K", "SET/FILTER DOC/PASS K,NO-SUCH-LABEL",
		"SET/FILTER DOS/PASS CA,ZZ", "SET/FILTER DOC/PASS K,,VE", "SET/FILTER K,VE/PASS 291",
		"SET/FILTER DOC/PASS 0", "SET/FILTER DOC/PASS -291", "SET/FILTER DOC/PASS W1ABC",
		"SET/FILTER DOC20/PASS K", "SET/FILTER DOC/20/PASS K", "SET/FILTER AOC/PASS K",
		"SET/OWN", "SET/BOB", "SET/DX", "SET/NODX", "SET/TALK", "SET/WX", "SET/FILTER WOS/PASS CA",
		"SET/FT8 EXTRA", "SET/SKIMMER EXTRA", "SET/NOFILTER EXTRA", "UNSET/FT8 EXTRA", "UNSET/FILTER DOC/PASS K",
		"SET/FILTER DXBM/PASS HF", "SET/FILTER DXBM/PASS 20-FT8", "SET/FILTER DXBM/OFF 20",
		"SET/FILTER DOC/PASS " + strings.Repeat("291,", 40),
	} {
		assertNativeCCRefused(t, c, engine, line)
	}
	*calls = 0
	assertNativeCCRefused(t, c, engine, "SET/FILTER DOC/PASS 999999999999999999999999999")
	if *calls != 0 {
		t.Fatal("overflowing numeric ADIF fell through to CTY resolution")
	}
}

func TestNativeCCBooleanStageBoundaryAndControlShortcuts(t *testing.T) {
	c, engine, _ := nativeCCCommandFixture(t)
	for _, line := range []string{"PASS RULE 0 MODE=CW", "REJECT RULE 9 MODE=FT8", "RESET FILTER", "RESET FILTER RULE ALL", "RESET RULE 0"} {
		before := nativeCCConfiguration(t, c)
		outcome := engine.stageOwned(c, line)
		if !outcome.handled || outcome.publish || !before.Equal(nativeCCConfiguration(t, c)) || !strings.Contains(outcome.response, "No changes applied.") || strings.Contains(outcome.response, "HELP RULE") {
			t.Fatalf("native Boolean boundary leaked into GO: %s => %+v", line, outcome)
		}
	}
	for _, name := range []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "ANN", "SELF", "TOXIC"} {
		for _, form := range []struct {
			prefix string
			on     bool
		}{{"SET/", true}, {"SET/NO", false}, {"SET/", true}, {"UNSET/", false}} {
			before := nativeCCConfiguration(t, c)
			line := form.prefix + name
			outcome := engine.stageOwned(c, line)
			if !outcome.handled || !outcome.publish {
				t.Fatalf("native control shortcut lost its owner: %s => %+v", line, outcome)
			}
			after := nativeCCConfiguration(t, c)
			got := map[string]bool{"BEACON": c.filter.BeaconsEnabled(), "WWV": c.filter.WWVEnabled(), "WCY": c.filter.WCYEnabled(), "ANNOUNCE": c.filter.AnnounceEnabled(), "ANN": c.filter.AnnounceEnabled(), "SELF": c.filter.SelfEnabled(), "TOXIC": c.filter.ToxicEnabled()}[name]
			if got != form.on {
				t.Fatalf("native control shortcut reversed operation: %s", line)
			}
			before.IncludeBeacons, before.AllowWWV, before.AllowWCY = after.IncludeBeacons, after.AllowWWV, after.AllowWCY
			before.AllowAnnounce, before.AllowSelf, before.AllowToxic = after.AllowAnnounce, after.AllowSelf, after.AllowToxic
			if !before.Equal(after) {
				t.Fatal("control shortcut changed native criteria")
			}
			assertNativeCCRefused(t, c, engine, line+" EXTRA")
		}
	}
}

func TestNativeCCGlobalReplacementOffAndNoFilterKeepOtherOwners(t *testing.T) {
	c, engine, _ := nativeCCCommandFixture(t)
	config := nativeCCConfiguration(t, c)
	var err error
	config, err = config.WithGeography("DOC", "", filter.CCCategory{Action: filter.CCPass, Countries: []int{291}})
	if err != nil {
		t.Fatal(err)
	}
	config, _ = config.WithGeography("DOC", "20", filter.CCCategory{Action: filter.CCPass, Countries: []int{230}})
	config, _ = config.WithGeography("DOS", "", filter.CCCategory{Action: filter.CCReject, States: []string{"TX"}})
	config, _ = config.WithDXBM(filter.CCPass, []string{"20-CW"})
	_, value, err := filter.NewCCFilter(config)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	if outcome := engine.stageNativeCCFilterCommand(c, "SET/FILTER DOC/PASS 291"); !outcome.publish || len(nativeCCConfiguration(t, c).Geography["DOC"].Bands) != 0 {
		t.Fatal("same-list global command did not clear band exceptions")
	}
	if outcome := engine.stageNativeCCFilterCommand(c, "SET/FILTER DOC/OFF"); !outcome.publish {
		t.Fatal("category OFF refused")
	}
	before := nativeCCConfiguration(t, c)
	if before.Geography["DOC"].Global.Action != filter.CCOff || before.Geography["DOS"].Global.Action != filter.CCReject || !before.DXBM.Active {
		t.Fatal("category OFF changed another category")
	}
	defaults, err := filter.CurrentDefaultCCConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	settings := c.configuredSettings
	if outcome := engine.stageNativeCCFilterCommand(c, "SET/NOFILTER"); !outcome.publish {
		t.Fatalf("NOFILTER refused: %+v", outcome)
	}
	after := nativeCCConfiguration(t, c)
	if !slices.Equal(after.Modes, defaults.Modes) || !slices.Equal(after.Sources, defaults.Sources) || after.DXBM.Active || c.configuredSettings != settings {
		t.Fatal("NOFILTER did not install current operator criteria while preserving settings")
	}
	for _, category := range after.Geography {
		if category.Global.Action != filter.CCOff || len(category.Bands) != 0 {
			t.Fatal("NOFILTER retained geography restrictions")
		}
	}
	before.Modes, before.Sources, before.Geography, before.DXBM = after.Modes, after.Sources, after.Geography, after.DXBM
	if !before.Equal(after) {
		t.Fatal("NOFILTER reset a separate control")
	}
}

func TestNativeCCModeSourceAndDXBMStaging(t *testing.T) {
	c, engine, _ := nativeCCCommandFixture(t)
	initial := nativeCCConfiguration(t, c)
	for _, line := range []string{"SET/NOFT8", "UNSET/CW", "SET/CW", "UNSET/SKIMMER", "SET/SKIMMER", "SET/NOSKIMMER"} {
		outcome := engine.stageNativeCCFilterCommand(c, line)
		if !outcome.handled || !outcome.publish {
			t.Fatalf("configured shortcut refused: %s => %+v", line, outcome)
		}
	}
	after := nativeCCConfiguration(t, c)
	if slices.Contains(after.Modes, "FT8") || !slices.Contains(after.Modes, "CW") || slices.Contains(after.Sources, "SKIMMER") || after.AllowSelf != initial.AllowSelf {
		t.Fatal("mode/source shortcut changed another owner")
	}
	assertNativeCCRefused(t, c, engine, "SET/USB")
	for _, line := range []string{"SET/FILTER DXBM/PASS 20-CW", "SET/FILTER DXBM/PASS 40", "SET/FILTER DXBM/REJECT 40-RTTY"} {
		if outcome := engine.stageNativeCCFilterCommand(c, line); !outcome.publish {
			t.Fatalf("DXBM staging refused %s: %+v", line, outcome)
		}
	}
	for _, probe := range []struct {
		frequency float64
		want      bool
	}{{14025, true}, {14074, false}, {7020, true}, {7040, false}, {7100, true}, {21020, false}} {
		sp := &spot.Spot{Mode: "CW", ModeNorm: "CW", IsHuman: true, Frequency: probe.frequency}
		if actual := c.filter.Matches(sp); actual != probe.want {
			t.Fatalf("staged literal frequency %v: %t", probe.frequency, actual)
		}
	}
	final := nativeCCConfiguration(t, c)
	initial.Modes, initial.Sources, initial.DXBM = final.Modes, final.Sources, final.DXBM
	if c.dialect != DialectCC || !initial.Equal(final) {
		t.Fatal("native staging changed dialect or unrelated geography")
	}
}
