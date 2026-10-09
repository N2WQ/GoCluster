package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpOverviewPresentation(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
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
			for _, section := range []string{"Getting started:", "Reading and posting spots:", "Changing filters:", "What you can filter:", "A complete recipe:", "Filter rules to remember:", "Comment rules:", "Nearby filtering:", "Preferences and propagation:", "Saving and loading presets:", "Pausing live spots:", "Diagnostics:", "More help:", "Other commands:", "Syntax used in detailed help:"} {
				index := strings.Index(got, section)
				if index <= previous {
					t.Fatalf("missing or out-of-order section %q", section)
				}
				previous = index
			}
			if strings.Contains(got, "YAML") || strings.Contains(got, "Bucket p50") || strings.Contains(got, "Confidence glyphs:") {
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
		for _, topic := range catalog.order {
			text := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			if strings.HasPrefix(text, "Unknown help topic:") {
				t.Fatalf("%s advertised topic %s does not resolve", dialect, topic)
			}
			assertHelpWidth(t, text)
			// Only machine YAML topics are deliberately hidden. Checking the
			// full ordered catalog catches future human commands omitted by layout edits.
			if !strings.Contains(topic, " YAML ") && !strings.Contains(overview, topic) {
				t.Fatalf("%s overview omits catalog command %s", dialect, topic)
			}
		}
		for _, topic := range []string{"FILTERS", "SYMBOLS"} {
			text := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, dialect)
			if strings.HasPrefix(text, "Unknown help topic:") || !strings.Contains(overview, "HELP "+topic) {
				t.Fatalf("reference %s is not reachable/advertised", topic)
			}
			assertHelpWidth(t, text)
		}
		assertHelpWidth(t, overview)
		for alias := range catalog.aliases {
			text := p.ProcessCommandForClient("HELP "+alias, "N0CALL", "", nil, dialect)
			if strings.HasPrefix(text, "Unknown help topic:") {
				t.Fatalf("%s alias %s no longer resolves", dialect, alias)
			}
		}
	}
}

func TestConcreteFilterHelpEveryCategory(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	// This inventory is intentionally separate from the renderer's rows.
	categories := []string{"BAND", "MODE", "SOURCE", "EVENT", "COMMENT", "DXCALL", "DECALL", "DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE", "DXGRID2", "DEGRID2", "CONFIDENCE", "PATH", "MINSNR"}
	for _, dialect := range []string{"go", "cc"} {
		for _, verb := range []string{"PASS", "REJECT"} {
			text := p.ProcessCommandForClient("HELP "+verb, "N0CALL", "", nil, dialect)
			for _, category := range categories {
				prefix := verb
				if dialect == "cc" && category != "COMMENT" && category != "MINSNR" {
					prefix = "SET/FILTER"
					if verb == "REJECT" {
						prefix = "UNSET/FILTER"
					}
				}
				if !strings.Contains(text, prefix+" "+category+" ") {
					t.Fatalf("%s HELP %s lacks a %s example", dialect, verb, category)
				}
			}
			for _, want := range []string{"Supported bands:", "Supported modes:", "Supported events:", "Continents:", "State/province mailing-address codes:", "Only a leading or trailing *", "SOURCE takes one value", "other filters still apply"} {
				if !strings.Contains(text, want) {
					t.Fatalf("%s HELP %s lacks %q", dialect, verb, want)
				}
			}
		}
	}
}

func TestHelpNavigationNormalization(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"filters", "symbols", "pass", "reject"} {
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
	if p.ProcessCommandForClient("H", "", "", nil, "classic") != p.ProcessCommandForClient("HELP", "", "", nil, "go") {
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
