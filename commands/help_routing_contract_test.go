package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/spot"
)

// These heads come from the executable dispatch owners, not catalog iteration.
// Argument tails request usage help; they are never sent to mutation owners.
type helpRoutingCase struct {
	topic, canonical, usage string
}

func helpRoutingLiteralCases(dialect string) []helpRoutingCase {
	history, readback := "SHOW DX", "SHOW FILTER"
	if dialect == "cc" {
		history, readback = "SHOW/DX", "SHOW/FILTER"
	}
	return []helpRoutingCase{
		{"HELP", "HELP", "HELP [command]"}, {"H", "HELP", "HELP [command]"},
		{"H SHOW BUILD", "HELP", "HELP [command]"}, {"HELP SHOW BUILD", "HELP", "HELP [command]"},
		{"SHOW", "SHOW", "SHOW DXCC"}, {"SH", "SHOW", "SHOW DXCC"},
		{"DX", "DX", "DX <freq_khz> <callsign> [comment]"},
		{"DX 14030 K1ABC CQ", "DX", "DX <freq_khz> <callsign> [comment]"},
		{"DX K1ABC 14030 CQ", "DX", "DX <callsign> <freq_khz> [comment]"},
		{"SH BUILD", "SHOW BUILD", "SHOW BUILD"}, {"SHOW BUILD", "SHOW BUILD", "SHOW BUILD"},
		{"SH OWN", "SHOW OWN", "SHOW OWN"}, {"SHOW OWN", "SHOW OWN", "SHOW OWN"},
		{"SH DXCC DL", "SHOW DXCC", "SHOW DXCC <prefix|callsign>"},
		{"SH DXCC", "SHOW DXCC", "SHOW DXCC <prefix|callsign>"},
		{"SHOW DXCC DL", "SHOW DXCC", "SHOW DXCC <prefix|callsign>"},
		{"SH MYDX", "SHOW MYDX", "SHOW MYDX [count]"},
		{"SH MYDX 5 MODE CW COMMENT CQ DX", "SHOW MYDX", "SHOW MYDX NEXT <cursor>"},
		{"SHOW MYDX K1ABC 5", "SHOW MYDX", "SHOW MYDX <prefix|callsign> [count]"},
		{"SH DX", history, history + " [count]"},
		{"SH DX 5 BAND 20 MODE CW", history, history + " NEXT <cursor>"},
		{"SHOW DX 5", history, history + " [count]"},
		{"SHOW PROP FN31 20 CW", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SH PROP FN31 20 CW", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SHOW/PROP FN31", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SH/PROP FN31", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SH PROP", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SHOW/PROP", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"SH/PROP", "SHOW PROP", "SHOW PROP <call|prefix|grid> [band] [mode]"},
		{"WHOSPOTSME 20", "WHOSPOTSME", "WHOSPOTSME [band]"},
		{"SHOW FILTER FULL", readback, readback + " FULL"},
		{"SHOW/FILTER RULE", readback, readback + " RULE"},
		{"SH/FILTER FULL", readback, readback + " FULL"},
		{"SHOW FILTER COMMENT", "SHOW FILTER COMMENT", "Usage: HELP RULE"},
		{"SHOW/FILTER COMMENT", "SHOW FILTER COMMENT", "Usage: HELP RULE"},
		{"SH/FILTER COMMENT", "SHOW FILTER COMMENT", "Usage: HELP RULE"},
		{"SHOW SETTINGS", "SHOW SETTINGS", "Usage: SHOW SETTINGS"},
		{"RULE", "RULE", "Usage: HELP RULE"},
		{"PASS", "PASS", "PASS RULE <0..9> <expression>"}, {"REJECT", "REJECT", "REJECT RULE <0..9> <expression>"},
		{"PASS RULE 0 MODE=CW", "PASS RULE", "PASS RULE <0..9> <expression>"},
		{"REJECT RULE 9 SNR<10", "REJECT RULE", "REJECT RULE <0..9> <expression>"},
		{"PASS BAND 20", "PASS", "HELP RULE"}, {"REJECT MODE FT8", "REJECT", "HELP RULE"},
		{"PASS PC93 ALL", "PASS", "PC93 aliases ANNOUNCE."}, {"REJECT SELF", "REJECT", "SELF"},
		{"PASS COMMENT CQ DX", "PASS COMMENT", "Usage: HELP RULE"},
		{"REJECT COMMENT QRT", "REJECT COMMENT", "Usage: HELP RULE"},
		{"REMOVE PASS COMMENT CQ DX", "REMOVE PASS COMMENT", "Usage: HELP RULE"},
		{"REMOVE REJECT COMMENT QRT", "REMOVE REJECT COMMENT", "Usage: HELP RULE"},
		{"RESET FILTER COMMENT PASS", "RESET FILTER COMMENT", "Usage: HELP RULE"},
		{"PASS MINSNR CW 10", "PASS MINSNR", "Usage: HELP RULE"},
		{"REJECT MINSNR CW NONE", "REJECT MINSNR", "Usage: HELP RULE"},
		{"PASS NEARBY ON", "PASS NEARBY", "PASS NEARBY ON|OFF"},
		{"REJECT NEARBY OFF", "PASS NEARBY", "PASS NEARBY ON|OFF"},
		{"RESET FILTER", "RESET FILTER", "RESET FILTER"},
		{"RESET FILTER RULE ALL", "RESET FILTER RULE", "RESET FILTER RULE <0..9|ALL>"},
		{"SET GRID FN31", "SET GRID", "SET GRID <locator>"},
		{"SET NOISE RURAL", "SET NOISE", "SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>"},
		{"SET PATHSAMPLES DEFAULT", "SET PATHSAMPLES", "SET PATHSAMPLES <count|DEFAULT>"},
		{"SET DEDUPE FAST", "SET DEDUPE", "SET DEDUPE <FAST|MED|SLOW>"},
		{"SHOW DEDUPE", "SHOW DEDUPE", "SHOW DEDUPE"},
		{"SET DIAG CONFIDENCE", "SET DIAG", "SET DIAG <OFF|DEDUPE|SOURCE|CONF|PATH|MODE>"},
		{"SET SOLAR 30", "SET SOLAR", "SET SOLAR <15|30|60|OFF>"},
		{"DIALECT CLASSIC", "DIALECT", "DIALECT <go|cc>"},
		{"SAVE PRESET CONTEST", "SAVE PRESET", "SAVE PRESET <name>"},
		{"LOAD PRESET CONTEST", "LOAD PRESET", "LOAD PRESET <name>"},
		{"DELETE PRESET CONTEST", "DELETE PRESET", "DELETE PRESET <name>"},
		{"LIST PRESET", "LIST PRESET", "LIST PRESET"},
		{"PAUSE 30", "PAUSE", "PAUSE [seconds 1-300]"},
		{"SHOW HOLD", "SHOW HOLD", "SHOW HOLD"}, {"RESUME", "RESUME", "RESUME"},
		{"BYE", "BYE", "BYE"}, {"QUIT", "BYE", "BYE"}, {"EXIT", "BYE", "BYE"},
	}
}

func assertHelpRouting(t testing.TB, p *Processor, dialect string, tc helpRoutingCase) {
	t.Helper()
	response := p.ProcessCommandForClient("HELP "+tc.topic, "W1ABC-18", "", nil, dialect)
	if !strings.HasPrefix(response, tc.canonical+" - ") || !strings.Contains(response, tc.usage) {
		t.Fatalf("%s HELP %q did not select %s / %q: %q", dialect, tc.topic, tc.canonical, tc.usage, response)
	}
	for _, line := range strings.Split(response, "\n") {
		if len(line) > 78 {
			t.Fatalf("%s HELP %q exceeds 78 bytes: %q", dialect, tc.topic, line)
		}
	}
}

func TestHelpRoutingDispatchDerivedFamilies(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, tc := range helpRoutingLiteralCases(dialect) {
			t.Run(dialect+"/"+tc.topic, func(t *testing.T) {
				assertHelpRouting(t, p, dialect, tc)
				assertHelpRouting(t, p, dialect, helpRoutingCase{tc.canonical, tc.canonical, tc.usage})
				assertHelpRouting(t, p, dialect, helpRoutingCase{"  " + strings.ToLower(tc.topic) + "  ", tc.canonical, tc.usage})
			})
		}
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
			topic := "GET YAML " + resource
			assertHelpRouting(t, p, dialect, helpRoutingCase{topic, topic, "Usage: " + topic + " SCHEMA 5 [ID <id>]"})
			assertHelpRouting(t, p, dialect, helpRoutingCase{topic + " SCHEMA 5 ID Read-1", topic, topic + " SCHEMA 5 [ID <id>]"})
		}
		for _, verb := range []string{"PUT", "PATCH"} {
			for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
				topic := verb + " YAML " + resource
				assertHelpRouting(t, p, dialect, helpRoutingCase{topic, topic, "Usage: " + topic})
			}
		}
		assertHelpRouting(t, p, dialect, helpRoutingCase{"VALIDATE YAML CONFIG", "VALIDATE YAML CONFIG", "Usage: VALIDATE YAML CONFIG"})
	}
	for _, topic := range []string{"SHOW/DX 5", "SH/DX 5", "SH/DX NEXT H100000000000000000000000000000000"} {
		assertHelpRouting(t, p, "cc", helpRoutingCase{topic, "SHOW/DX", "SHOW/DX NEXT <cursor>"})
	}
	for _, tc := range []helpRoutingCase{
		{"SET/FILTER BEACON/ON", "SET/FILTER", "SET/FILTER <switch> [ALL]"},
		{"UNSET/FILTER BEACON", "UNSET/FILTER", "UNSET/FILTER <switch> [ALL]"},
		{"SET/FILTER DXBM/REJECT 20", "SET/FILTER", "HELP RULE"},
		{"SET/NOFILTER", "SET/NOFILTER", "Usage: HELP RULE"},
	} {
		assertHelpRouting(t, p, "cc", tc)
	}
}

func TestHelpRoutingUnknownHeadsAndDialectRestrictions(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{
			"SH FILTER", "SH SETTINGS", "SH DEDUPE", "SH HOLD", "SHOW/MYDX", "SH/MYDX",
			"SHOW BUILDX", "SH BUILDX", "SHOW DXCCX DL", "SHOW MYDXX", "SHOW FILTERX", "SHOW SETTINGSX",
			"WHOSPOTSMEX", "RESUMED", "DIALECTX", "SET SOLARX 30", "SET GRIDX FN31", "SET PATHSAMPLESX DEFAULT",
			"SAVE PRESETX CON", "GET YAML CONFIGX", "SET/NOFILTERX", "SHOW/PROPX FN31", "SH/PROPX FN31",
		} {
			want := "Unknown help topic: " + topic + "\nType HELP for available commands.\n"
			if got := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, dialect); got != want {
				t.Fatalf("%s unsupported HELP %q resolved: %q", dialect, topic, got)
			}
		}
	}
	for _, topic := range []string{"SHOW/DX", "SH/DX", "SHOW/DX 5", "SH/DX 5", "SET/ANN", "SET/FILTER BAND 20"} {
		if got := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, "go"); !strings.HasPrefix(got, "Unknown help topic: ") {
			t.Fatalf("GO HELP advertised a CC-only head %q: %q", topic, got)
		}
	}
}

func TestHelpRoutingReverseAliasMetadata(t *testing.T) {
	// The literal executable-head allowlist must not grow when catalog aliases
	// grow. New metadata therefore requires evidence from a dispatch owner.
	common := map[string]string{
		"H": "HELP", "SH": "SHOW", "QUIT": "BYE", "EXIT": "BYE", "REJECT NEARBY": "PASS NEARBY",
		"SH BUILD": "SHOW BUILD", "SH OWN": "SHOW OWN", "SH DXCC": "SHOW DXCC", "SH MYDX": "SHOW MYDX",
		"SH PROP": "SHOW PROP", "SHOW/PROP": "SHOW PROP", "SH/PROP": "SHOW PROP",
		"SHOW/FILTER COMMENT": "SHOW FILTER COMMENT", "SH/FILTER COMMENT": "SHOW FILTER COMMENT",
	}
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		allowed := make(map[string]string, len(common)+8)
		for alias, target := range common {
			allowed[alias] = target
		}
		allowed["NEARBY"] = "PASS NEARBY" // Existing educational topic, not an executable head.
		if dialect == "cc" {
			allowed["SHOW DX"], allowed["SH DX"], allowed["SH/DX"] = "SHOW/DX", "SHOW/DX", "SHOW/DX"
			allowed["SHOW FILTER"], allowed["SH/FILTER"] = "SHOW/FILTER", "SHOW/FILTER"
			allowed["SHOW/FILTER"] = "SHOW/FILTER"
		} else {
			allowed["SH DX"] = "SHOW DX"
			allowed["SHOW/FILTER"], allowed["SH/FILTER"] = "SHOW FILTER", "SHOW FILTER"
		}
		catalog := buildHelpCatalog(dialect, DedupeHelpConfig{}, WhoSpotsMeHelpConfig{})
		for alias, target := range catalog.aliases {
			want, exists := allowed[alias]
			if !exists || target != want {
				t.Fatalf("%s catalog alias %q -> %q has no literal owner contract (want %q)", dialect, alias, target, want)
			}
			assertHelpRouting(t, p, dialect, helpRoutingCase{alias, want, "Usage:"})
		}
	}
}

func TestHelpRoutingVisibleAliases(t *testing.T) {
	// Routing alone cannot prove users can discover an accepted alias. Require
	// the independently inventoried aliases in the rendered canonical entry.
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		readback, history := "SHOW FILTER", "SHOW DX"
		filterAliases, historyAliases := "SHOW/FILTER, SH/FILTER", "SH DX"
		if dialect == "cc" {
			readback, history = "SHOW/FILTER", "SHOW/DX"
			filterAliases, historyAliases = "SHOW FILTER, SH/FILTER", "SH/DX, SHOW DX, SH DX"
		}
		for _, tc := range []struct{ topic, aliases string }{
			{"HELP", "H"}, {"SHOW", "SH"}, {"SHOW BUILD", "SH BUILD"},
			{"SHOW OWN", "SH OWN"}, {"SHOW DXCC", "SH DXCC"}, {"SHOW MYDX", "SH MYDX"},
			{"PASS NEARBY", "REJECT NEARBY"}, {readback, filterAliases}, {history, historyAliases},
		} {
			response := p.ProcessCommandForClient("HELP "+tc.topic, "W1ABC", "", nil, dialect)
			if !strings.Contains(response, "\nAliases: "+tc.aliases+"\n") {
				t.Fatalf("%s HELP %q hid executable aliases %q: %q", dialect, tc.topic, tc.aliases, response)
			}
		}
	}
}

func TestHelpRoutingLoadedShortcutModesAndFixedPrecedence(t *testing.T) {
	// Taxonomy is process-global: load once, use sequentially, restore on exit.
	const definition = "modes:\n" +
		"  - name: TEST_MODE\n    filter_visible: true\n    default_filter_allowed: true\n    cc_shortcut: true\n    variants: [MODE_ALIAS, ANN, NOFILTER]\n" +
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
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, mode := range []string{"TEST_MODE", "MODE_ALIAS"} {
		for _, prefix := range []string{"SET/", "SET/NO"} {
			canonical := "SET/<MODE>"
			if prefix == "SET/NO" {
				canonical = "SET/NO<MODE>"
			}
			assertHelpRouting(t, p, "cc", helpRoutingCase{prefix + mode, canonical, "Usage: HELP RULE"})
			assertHelpRouting(t, p, "cc", helpRoutingCase{prefix + mode + " EXTRA", canonical, "Usage: HELP RULE"})
			if got := p.ProcessCommandForClient("HELP "+prefix+mode, "W1ABC", "", nil, "go"); !strings.HasPrefix(got, "Unknown help topic: ") {
				t.Fatalf("GO HELP advertised loaded CC shortcut %q: %q", prefix+mode, got)
			}
		}
	}
	for _, topic := range []string{"SET/DISABLED_MODE", "SET/NODISABLED_MODE", "SET/UNKNOWN_MODE", "SET/NOUNKNOWN_MODE"} {
		if got := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, "cc"); !strings.HasPrefix(got, "Unknown help topic: ") {
			t.Fatalf("ineligible loaded shortcut %q resolved: %q", topic, got)
		}
	}
	assertHelpRouting(t, p, "cc", helpRoutingCase{"SET/ANN", "SET/ANN", "Alias of PASS ANNOUNCE."})
	assertHelpRouting(t, p, "cc", helpRoutingCase{"SET/NOANN", "SET/NOANN", "Alias of REJECT ANNOUNCE."})
	assertHelpRouting(t, p, "cc", helpRoutingCase{"SET/NOFILTER", "SET/NOFILTER", "Usage: HELP RULE"})
}

func FuzzHelpRoutingWholeTokenBoundaries(f *testing.F) {
	for i := uint8(0); i < 7; i++ {
		f.Add(i, true, uint8('X'), false)
		f.Add(i, false, uint8('Z'), true)
	}
	// These independent fixed heads have no generic parent that accepts their
	// collided spelling. The fuzz oracle never consults catalog/normalizer data.
	heads := []helpRoutingCase{
		{"SHOW BUILD", "SHOW BUILD", "SHOW BUILD"}, {"SH DXCC", "SHOW DXCC", "SHOW DXCC <prefix|callsign>"},
		{"SET SOLAR", "SET SOLAR", "SET SOLAR <15|30|60|OFF>"}, {"WHOSPOTSME", "WHOSPOTSME", "WHOSPOTSME [band]"},
		{"DIALECT", "DIALECT", "DIALECT <go|cc>"}, {"RESUME", "RESUME", "RESUME"},
		{"GET YAML CONFIG", "GET YAML CONFIG", "GET YAML CONFIG SCHEMA 5 [ID <id>]"},
	}
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	f.Fuzz(func(t *testing.T, index uint8, separated bool, value uint8, cc bool) {
		tc := heads[int(index)%len(heads)]
		dialect := "go"
		if cc {
			dialect = "cc"
		}
		tail := string(rune('A' + value%26))
		if separated {
			tc.topic += " " + tail
			assertHelpRouting(t, p, dialect, tc)
			return
		}
		topic := tc.topic + tail
		want := "Unknown help topic: " + topic + "\nType HELP for available commands.\n"
		if got := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, dialect); got != want {
			t.Fatalf("character collision resolved: %q -> %q", topic, got)
		}
	})
}
