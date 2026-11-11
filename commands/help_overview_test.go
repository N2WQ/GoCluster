package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpOverviewPresentation(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(PathGlyphHelpConfig{Enabled: true, High: ">", Medium: "=", Low: "<", Unlikely: "-", Insufficient: " ", Closed: "#"}))
	for _, dialect := range []string{"go", "cc"} {
		t.Run(dialect, func(t *testing.T) {
			got := p.ProcessCommandForClient("HELP", "N0CALL", "", nil, dialect)
			want, err := os.ReadFile(filepath.Join("testdata", "help-"+dialect+".txt"))
			if err != nil {
				t.Fatal(err)
			}
			if got != strings.ReplaceAll(string(want), "\r\n", "\n") {
				t.Fatalf("%s HELP differs from the reviewed presentation fixture", dialect)
			}
			previous := -1
			for _, section := range []string{"Try these commands:", "Call confidence:", "Path reliability:", "What would you like to do?", "For help with one command, type HELP before its name:"} {
				index := strings.Index(got, section)
				if index <= previous {
					t.Fatalf("missing or out-of-order section %q", section)
				}
				previous = index
			}
			if dialect == "go" && strings.Contains(got, "YAML") || strings.Contains(got, "Bucket p50") || strings.Contains(got, "Confidence glyphs:") {
				t.Fatal("overview includes displaced machine/scientific reference")
			}
		})
	}
}

func TestHelpCatalogDiscoverabilityAndWidths(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		catalog := buildHelpCatalog(dialect, DedupeHelpConfig{}, WhoSpotsMeHelpConfig{})
		overview := p.ProcessCommandForClient("HELP", "N0CALL", "", nil, dialect)
		index := p.ProcessCommandForClient("HELP COMMANDS", "N0CALL", "", nil, dialect)
		for _, topic := range catalog.order {
			text := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			if strings.HasPrefix(text, "Unknown help topic:") {
				t.Fatalf("%s advertised topic %s does not resolve", dialect, topic)
			}
			assertHelpWidth(t, text)
			// All detailed topics remain reachable through the command index.
			if !strings.Contains(index, topic) {
				t.Fatalf("%s command index omits catalog command %s", dialect, topic)
			}
		}
		for _, topic := range []string{"SPOTS", "FILTERS", "SETTINGS", "PRESETS", "SYMBOLS", "TROUBLE", "COMMANDS"} {
			text := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			if strings.HasPrefix(text, "Unknown help topic:") || !strings.Contains(overview, "HELP "+topic) {
				t.Fatalf("reference %s is not reachable/advertised", topic)
			}
			assertHelpWidth(t, text)
		}
		assertHelpWidth(t, overview)
	}
}

func TestBooleanRuleHelpCanonicalCatalog(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	fields := []string{"BAND", "MODE", "SOURCE", "EVENT", "FREQ", "SNR", "COMMENT", "DXCALL", "DECALL", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2", "CONF", "PATH"}
	for _, dialect := range []string{"go", "cc"} {
		text := p.ProcessCommandForClient("HELP RULE", "N0CALL", "", nil, dialect)
		assertHelpWidth(t, text)
		if dialect == "cc" {
			if !strings.Contains(text, "GO dialect only") || !strings.Contains(text, "HELP SET/FILTER") || strings.Contains(text, "PASS RULE 0") {
				t.Fatal("native CC HELP RULE advertised Boolean edits")
			}
			continue
		}
		flat := strings.Join(strings.Fields(text), " ")
		for _, want := range []string{
			"PASS without SOURCE allows humans and skimmers.",
			"SOURCE follows the same Boolean logic as other fields.",
			"Users moving from old filters get this rule too.",
			"They keep their settings.",
		} {
			if !strings.Contains(flat, want) {
				t.Fatalf("%s HELP RULE omitted source/onboarding policy %q", dialect, want)
			}
		}
		for _, field := range fields {
			if !strings.Contains(flat, field) {
				t.Fatalf("%s HELP RULE omitted %s", dialect, field)
			}
		}
		if !strings.Contains(text, `COMMENT="CQ ""DX""  !;=()[]"`) {
			t.Fatalf("%s HELP RULE changed quoted literal bytes", dialect)
		}
		for _, topic := range []string{"PASS", "REJECT", "FILTERS"} {
			text := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			if !strings.Contains(text, "HELP RULE") || strings.Contains(text, "Fields:") {
				t.Fatalf("%s HELP %s must reference, not duplicate, the canonical field catalog", dialect, topic)
			}
		}
	}
}

func TestHelpNavigationNormalization(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"spots", "filters", "settings", "presets", "symbols", "trouble", "commands", "pass", "reject"} {
			want := p.ProcessCommandForClient("HELP "+strings.ToUpper(topic), "N0CALL", "", nil, dialect)
			got := p.ProcessCommandForClient("  hElP   "+topic+"  ", "N0CALL", "", nil, dialect)
			if got != want || strings.HasPrefix(got, "Unknown help topic:") {
				t.Fatalf("%s topic %s normalization failed", dialect, topic)
			}
		}
		if got := p.ProcessCommandForClient("HELP MADEUP", "N0CALL", "", nil, dialect); got != "Unknown help topic: MADEUP\nType HELP for available commands.\n" {
			t.Fatalf("unknown topic response changed: %q", got)
		}
	}
	got := p.ProcessCommandForClient("H", "N0CALL", "", nil, "classic")
	if got != p.ProcessCommandForClient("HELP", "N0CALL", "", nil, "go") || !strings.HasPrefix(got, "Try these commands:") {
		t.Fatal("default dialect or H alias changed")
	}
}

func TestHelpReferenceStartupValues(t *testing.T) {
	for _, glyphs := range []PathGlyphHelpConfig{
		{Enabled: true, High: ">", Medium: "=", Low: "<", Unlikely: "-", Insufficient: " ", Closed: "!"},
		{Enabled: false, High: ">", Medium: "=", Low: "<", Unlikely: "-", Insufficient: " ", Closed: "!"},
		{Enabled: true, High: ">", Medium: "=", Low: "<", Unlikely: "-", Insufficient: " "},
	} {
		p := NewProcessor(nil, nil, nil, nil, nil, nil,
			WithPathGlyphHelp(glyphs), WithWhoSpotsMeHelp(WhoSpotsMeHelpConfig{Configured: true, WindowMinutes: 17}))
		for _, dialect := range []string{"go", "cc"} {
			text := p.ProcessCommandForClient("HELP SYMBOLS", "N0CALL", "", nil, dialect)
			if strings.Contains(text, "Path reliability glyphs:") != (glyphs.Enabled && glyphs.Closed != "") {
				t.Fatal("path legend availability changed")
			}
			if !strings.Contains(text, "Confidence glyphs:") {
				t.Fatal("confidence legend lost when path display unavailable")
			}
			for _, note := range []string{"strongest ?/S/P/V letter", "corrected call gets its own result", "DIAG CONF shows current support", "CONF filters use the retained"} {
				if !strings.Contains(strings.Join(strings.Fields(text), " "), note) {
					t.Fatalf("HELP SYMBOLS lost stabilizer contract: %s", note)
				}
			}
			assertHelpWidth(t, text)
			who := p.ProcessCommandForClient("HELP WHOSPOTSME", "N0CALL", "", nil, dialect)
			if !strings.Contains(who, "last 17 minutes") {
				t.Fatalf("configured reporting window lost: %q", who)
			}
		}
	}
}

func assertHelpWidth(t *testing.T, text string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if len(line) > helpMaxWidth {
			t.Fatalf("HELP line exceeds %d bytes: %q", helpMaxWidth, line)
		}
	}
}

// These checks complement fixtures: copied output cannot hide a dead menu entry,
// a lost narrow catalog topic, or a path legend using the wrong startup mapping.
func TestHelpTaskNavigationAndIndex(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		catalog := buildHelpCatalog(dialect, DedupeHelpConfig{}, WhoSpotsMeHelpConfig{})
		index := p.ProcessCommandForClient("HELP COMMANDS", "N0CALL", "", nil, dialect)
		if index != p.ProcessCommandForClient("HELP COMMANDS", "N0CALL", "", nil, dialect) {
			t.Fatal("command index order is unstable")
		}
		for topic := range catalog.entries {
			found := false
			for _, line := range strings.Split(index, "\n") {
				if strings.HasPrefix(line, "  "+topic+" ") {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s index lost exact topic %s", dialect, topic)
			}
		}
		for _, topic := range []string{"DX", "SHOW SETTINGS", "SAVE PRESET", "LOAD PRESET", "SHOW HOLD", "RESUME", "GET YAML CONFIG", "BYE"} {
			if !strings.Contains(index, "  "+topic+" ") {
				t.Fatalf("index lost required command %s", topic)
			}
		}
		for _, topic := range []string{"SPOTS", "SETTINGS", "PRESETS", "TROUBLE"} {
			page := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			assertHelpWidth(t, page)
			// A stronger common limit keeps both descriptions and instructions short.
			for _, line := range strings.Split(page, "\n") {
				if len(strings.Fields(line)) >= 20 {
					t.Fatalf("long task sentence: %q", line)
				}
				if strings.HasPrefix(line, "  HELP ") {
					target := strings.TrimPrefix(line, "  ")
					if strings.HasPrefix(p.ProcessCommandForClient(target, "N0CALL", "", nil, dialect), "Unknown help topic:") {
						t.Fatalf("dead task link %s", target)
					}
				}
			}
		}
		trouble := p.ProcessCommandForClient("HELP TROUBLE", "N0CALL", "", nil, dialect)
		if !strings.Contains(trouble, "SHOW HOLD") || !strings.Contains(trouble, "RESUME") || strings.Index(trouble, "SHOW HOLD") > strings.Index(trouble, "RESUME") || strings.Contains(trouble, "RESET FILTER") {
			t.Fatal("troubleshooting lost safe check-first order")
		}
		overview := p.ProcessCommandForClient("HELP", "N0CALL", "", nil, dialect)
		for _, forbidden := range []string{"GoCluster help", "BYE", "HELP WEB", "guarantee contact", "Path reliability:"} {
			if strings.Contains(overview, forbidden) {
				t.Fatalf("unexpected overview content %q", forbidden)
			}
		}
		for _, alias := range []string{"H", "h"} {
			if p.ProcessCommandForClient(alias, "N0CALL", "", nil, dialect) != overview {
				t.Fatal("HELP alias changed")
			}
		}
	}
}

func TestHelpOverviewConfiguredSymbols(t *testing.T) {
	for _, dialect := range []string{"go", "cc"} {
		cfg := PathGlyphHelpConfig{Enabled: true, High: "A", Medium: "M", Low: "L", Unlikely: "U", Insufficient: "?", Closed: "X"}
		p := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(cfg))
		text := p.ProcessCommandForClient("HELP", "N0CALL", "", nil, dialect)
		for _, want := range []string{"A Favorable", "M Workable", "L Weak", "U Poor", "X Closed", `"?" means not enough recent reports.`} {
			if !strings.Contains(text, want) {
				t.Fatalf("custom symbol missing %q", want)
			}
		}
		if strings.Contains(text, "A blank") {
			t.Fatal("nonblank insufficient symbol described as blank")
		}
		assertHelpWidth(t, text)
		disabledConfig := cfg
		disabledConfig.Enabled = false
		for _, disabled := range []PathGlyphHelpConfig{{}, disabledConfig, {Enabled: true, High: "A"}} {
			p := NewProcessor(nil, nil, nil, nil, nil, nil, WithPathGlyphHelp(disabled))
			if strings.Contains(p.ProcessCommandForClient("HELP", "N0CALL", "", nil, dialect), "Path reliability:") {
				t.Fatal("unavailable path legend shown")
			}
		}
	}
}
