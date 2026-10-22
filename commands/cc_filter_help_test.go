package commands

import (
	"slices"
	"strings"
	"testing"
)

// Read rendered controls independently of the catalog builder.
func ccHelpGroup(t *testing.T, text, heading string) []string {
	t.Helper()
	_, remainder, found := strings.Cut(text, heading+"\n")
	if !found {
		t.Fatalf("missing rendered group %q", heading)
	}
	var result []string
	for _, line := range strings.Split(remainder, "\n") {
		if !strings.HasPrefix(line, "  ") {
			break
		}
		for _, name := range strings.Split(strings.TrimSpace(line), ",") {
			result = append(result, strings.TrimSpace(name))
		}
	}
	return result
}

func TestCCFilterHelpGrammar(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	wantSwitches := []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"}
	for _, verb := range []string{"SET/FILTER", "UNSET/FILTER"} {
		text := p.ProcessCommandForClient("HELP "+verb, "W1ABC", "", nil, "cc")
		assertHelpWidth(t, text)
		if !slices.Equal(ccHelpGroup(t, text, "Switches:"), wantSwitches) {
			t.Fatalf("%s advertises incomplete or unsupported CC groups", verb)
		}
		flat := strings.Join(strings.Fields(text), " ")
		for _, want := range []string{
			"Usage: " + verb + " <switch> [ALL]", verb + " NEARBY ON|OFF",
			"NEARBY follows its explicit ON/OFF value with either verb",
			"Category lists, MINSNR, DXBM and NOFILTER are refused, not translated.",
			"Use PASS RULE or REJECT RULE in either dialect; see HELP RULE.",
			"UNSET/FILTER does not accept inline /ON or /OFF.", "PC93 aliases ANNOUNCE",
		} {
			if !strings.Contains(flat, want) {
				t.Fatalf("%s loses qualified grammar %q", verb, want)
			}
		}
		for _, forbidden := range []string{"CC list types:", verb + " <list-type>", verb + " MINSNR ", verb + " DXBM ", "SET/FILTER NOFILTER/ON"} {
			if strings.Contains(flat, forbidden) {
				t.Fatalf("%s retains misleading blanket grammar %q", verb, forbidden)
			}
		}
		if verb == "SET/FILTER" {
			for _, want := range []string{"SET/FILTER <switch>/ON or /OFF", "No inline flags for NEARBY.", "Example: SET/FILTER BEACON/OFF"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("SET reference loses supported or excluded form %q", want)
				}
			}
		}
	}
	text := p.ProcessCommandForClient("HELP FILTERS", "W1ABC", "", nil, "cc")
	assertHelpWidth(t, text)
	if !strings.Contains(text, "HELP RULE") || strings.Contains(text, "CC list types:") || strings.Contains(text, "SET/<MODE>") {
		t.Fatal("FILTERS reference advertises legacy category or mode mutation")
	}
}
