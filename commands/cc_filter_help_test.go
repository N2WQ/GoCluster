package commands

import (
	"slices"
	"strings"
	"testing"
)

// Read rendered groups independently of the catalog's category builder. A
// missing category or COMMENT advertised as a CC list must fail even when the
// help topic itself resolves and its examples contain plausible words.
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
	wantLists := []string{"BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL", "DXGRID2", "DEGRID2", "DXCONT", "DECONT",
		"DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE", "CONFIDENCE", "PATH"}
	wantSwitches := []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"}
	for _, verb := range []string{"SET/FILTER", "UNSET/FILTER"} {
		text := p.ProcessCommandForClient("HELP "+verb, "W1ABC", "", nil, "cc")
		assertHelpWidth(t, text)
		if !slices.Equal(ccHelpGroup(t, text, "CC list types:"), wantLists) ||
			!slices.Equal(ccHelpGroup(t, text, "Switches:"), wantSwitches) {
			t.Fatalf("%s advertises incomplete or unsupported CC groups", verb)
		}
		flat := strings.Join(strings.Fields(text), " ")
		clear := "ALL"
		if verb == "UNSET/FILTER" {
			clear = "NONE"
		}
		for _, want := range []string{
			"Usage: " + verb + " <list-type> <values>",
			verb + " MINSNR <modes|ALL> <integer dB|" + clear + ">",
			"Clear MINSNR with " + verb + " MINSNR <modes|ALL> " + clear,
			verb + " NEARBY ON|OFF", verb + " <switch> [ALL]", verb + " DXBM <band-codes>",
			"SOURCE takes one HUMAN, SKIMMER or ALL", "NEARBY follows its explicit ON/OFF value with either verb",
			"SET/FILTER COMMENT and UNSET/FILTER COMMENT are unsupported",
			"UNSET/FILTER does not accept inline /ON or /OFF, or NOFILTER",
			"DXBM takes numeric band codes, not ALL", "PC93 aliases ANNOUNCE",
		} {
			if !strings.Contains(flat, want) {
				t.Fatalf("%s loses qualified grammar %q", verb, want)
			}
		}
		for _, forbidden := range []string{verb + " <type> <list>", "SET/FILTER <type>/ON", "SET/FILTER <type>/OFF", "Same semantics as"} {
			if strings.Contains(flat, forbidden) {
				t.Fatalf("%s retains misleading blanket grammar %q", verb, forbidden)
			}
		}
		if verb == "SET/FILTER" {
			for _, want := range []string{"SET/FILTER <list-type>/ON or /OFF", "SET/FILTER <switch>/ON or /OFF",
				"SET/FILTER NOFILTER/ON", "NOFILTER/OFF is unsupported", "No inline flags for COMMENT, MINSNR, NEARBY or DXBM",
				"SET/FILTER DXBM PASS|REJECT <band-codes>", "SET/FILTER DXBM/PASS <band-codes>", "SET/FILTER DXBM/REJECT <band-codes>"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("SET reference loses supported or excluded form %q", want)
				}
			}
		}
	}
	text := p.ProcessCommandForClient("HELP FILTERS", "W1ABC", "", nil, "cc")
	assertHelpWidth(t, text)
	if strings.Count(text, "CC list types:\n") != 2 ||
		!slices.Equal(ccHelpGroup(t, text, "CC list types:"), wantLists) ||
		strings.Contains(text, "SET/FILTER <type>/ON") || strings.Contains(text, "SET/FILTER <type>/OFF") {
		t.Fatal("FILTERS reference diverges from both CC verb contracts")
	}
}
