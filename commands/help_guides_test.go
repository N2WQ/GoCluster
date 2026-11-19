package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Reviewed screens protect presentation; navigation and executable recipe tests
// separately prove the links, command outcomes and replacement boundaries.
func TestHelpGuidePresentation(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, page := range []struct{ topic, fixture string }{
			{"FILTERS", "help-filters-" + dialect + ".txt"},
			{"FILTER EXAMPLES", "help-filter-examples-" + dialect + ".txt"},
			{"PRESETS", "help-presets.txt"},
		} {
			t.Run(dialect+"/"+page.topic, func(t *testing.T) {
				got := p.ProcessCommandForClient("HELP "+page.topic, "N0CALL", "", nil, dialect)
				want, err := os.ReadFile(filepath.Join("testdata", page.fixture))
				if err != nil {
					t.Fatal(err)
				}
				if got != strings.ReplaceAll(string(want), "\r\n", "\n") {
					t.Fatalf("guide differs from the reviewed screen: %q", got)
				}
				if page.topic == "FILTERS" {
					for _, alias := range []string{"HELP FILTER", " help  FiLtEr "} {
						if p.ProcessCommandForClient(alias, "N0CALL", "", nil, dialect) != got {
							t.Fatalf("%q must open the same %s filter guide", alias, dialect)
						}
					}
				}
				assertHelpWidth(t, got)
				assertHelpGuideWordLimits(t, got)
			})
		}
	}
}

func assertHelpGuideWordLimits(t *testing.T, text string) {
	t.Helper()
	// Wrapped prose is counted as a whole, so wrapping cannot conceal long
	// descriptions or instructions. The stronger instruction cap covers both.
	var prose []string
	check := func() {
		for _, sentence := range strings.Split(strings.Join(prose, " "), ".") {
			if len(strings.Fields(sentence)) >= 20 {
				t.Fatalf("guide instruction has 20 or more words: %q", sentence)
			}
		}
		prose = prose[:0]
	}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasSuffix(line, ":") || helpGuideCommand(line) {
			check()
			continue
		}
		if strings.Contains(strings.TrimSpace(line), "   ") {
			check()
			_, description, _ := strings.Cut(strings.TrimSpace(line), "   ")
			if len(strings.Fields(description)) >= 25 {
				t.Fatalf("guide description has 25 or more words: %q", description)
			}
			continue
		}
		prose = append(prose, strings.TrimSpace(line))
	}
	check()
}

func helpGuideCommand(line string) bool {
	// A command occupies its own two-space-indented line. Menu descriptions
	// are counted separately, rather than mistaken for executable arguments.
	if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "   ") {
		return false
	}
	for _, prefix := range []string{"HELP ", "SHOW ", "SHOW/", "RESUME", "RESET ", "PASS RULE ", "REJECT RULE ", "SET/", "SAVE ", "LIST ", "LOAD ", "DELETE "} {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return !strings.Contains(strings.TrimSpace(line), "   ")
		}
	}
	return false
}

func TestHelpGuidesReachCore(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		guide := p.ProcessCommandForClient("HELP FILTERS", "N0CALL", "", nil, dialect)
		_, readback := helpTaskCommands(dialect)
		if !strings.HasPrefix(guide, readback+" shows your active ") {
			t.Fatal("filter guide delays its first useful command")
		}
		examples := p.ProcessCommandForClient("HELP FILTER EXAMPLES", "N0CALL", "", nil, dialect)
		if strings.HasPrefix(examples, "Try a filter setup:") {
			t.Fatal("examples should start directly with the first recipe")
		}
		index := p.ProcessCommandForClient("HELP COMMANDS", "N0CALL", "", nil, dialect)
		if !strings.Contains(index, "  FILTER EXAMPLES ") || !strings.Contains(guide, "HELP FILTER EXAMPLES") {
			t.Fatal("new examples are not discoverable")
		}
		if dialect == "cc" && !strings.Contains(examples, "Requires the FT8 command to be enabled on this cluster.") {
			t.Fatal("CC examples omit the operator-owned shortcut boundary")
		}
	}
}
