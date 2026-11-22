package commands

import (
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func loadHelpExampleTaxonomy(t *testing.T) {
	t.Helper()
	taxonomy, err := spot.LoadTaxonomyFile(filepath.Join("..", "data", "config", "spot_taxonomy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
}

func TestHelpArchiveExamplesParse(t *testing.T) {
	loadHelpExampleTaxonomy(t)
	db := historyCanonicalCTY(t)
	p := NewProcessor(nil, nil, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	for _, tc := range []struct{ dialect, topic string }{{"go", "SHOW DX"}, {"cc", "SHOW/DX"}} {
		page := p.ProcessCommandForClient("HELP "+tc.topic, "N0CALL", "", nil, tc.dialect)
		examples, _, found := strings.Cut(page, "\n\n")
		if !found || !strings.HasPrefix(examples, "Example: "+tc.topic+" 10 - ") ||
			!strings.Contains(page, "Type "+tc.topic+" NEXT to search older spots.") ||
			!strings.Contains(page, tc.topic+" NEXT [cursor]") {
			t.Fatalf("%s archive help lost the example-led introduction or continuation guidance", tc.dialect)
		}
		lines := strings.Split(examples, "\n")
		if len(lines) != 5 {
			t.Fatalf("%s archive introduction lost an example: %q", tc.dialect, examples)
		}
		for _, line := range lines[:4] {
			text := strings.TrimSpace(strings.TrimPrefix(line, "Example: "))
			command, _, _ := strings.Cut(text, " - ")
			command, _, _ = strings.Cut(command, "   ")
			parsed, handled, parseErr := p.ParseHistoryCommand(command, tc.dialect)
			if !handled || parseErr != "" || parsed.Next || parsed.Query.count != 10 {
				t.Fatalf("%s Help example %q: handled=%t error=%q query=%+v", tc.dialect, command, handled, parseErr, parsed)
			}
		}
		parsed, handled, parseErr := p.ParseHistoryCommand(tc.topic+" NEXT", tc.dialect)
		if !handled || parseErr != "" || !parsed.Next || parsed.Token != "" {
			t.Fatalf("%s Help NEXT example doesn't parse without a cursor: %+v %q", tc.dialect, parsed, parseErr)
		}
	}
}

func TestHelpRuleExamplesAndDeliverySwitches(t *testing.T) {
	loadHelpExampleTaxonomy(t)
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	table := "Delivery switches (GO and CC)\n" +
		"  Switch     Turn on          Turn off\n" +
		"  WWV        PASS WWV         REJECT WWV\n" +
		"  WCY        PASS WCY         REJECT WCY\n" +
		"  ANNOUNCE   PASS ANNOUNCE    REJECT ANNOUNCE\n" +
		"  BEACON     PASS BEACON      REJECT BEACON\n"
	for _, verb := range []string{"PASS", "REJECT"} {
		topic := verb + " RULE"
		goRule := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, "go")
		_, example, found := strings.Cut(goRule, "\nExample: "+topic+" ")
		if !found || !strings.HasPrefix(goRule, topic+" <0-9> <expression>\n") {
			t.Fatalf("GO Help %s doesn't lead with its example: %q", topic, goRule)
		}
		example, _, _ = strings.Cut(example, "\n")
		slotText, expression, _ := strings.Cut(example, " ")
		slot, err := strconv.Atoi(slotText)
		if err != nil {
			t.Fatal(err)
		}
		_, err = filter.PrepareRuleSet([]filter.RuleDefinition{
			{Slot: slot, Action: filter.RuleAction(strings.ToLower(verb)), Expr: expression},
		})
		if err != nil {
			t.Fatalf("%s example cannot be compiled: %v", topic, err)
		}
		ccRule := p.ProcessCommandForClient("HELP "+topic, "N0CALL", "", nil, "cc")
		if !strings.HasPrefix(ccRule, "Numbered PASS and REJECT rules are for GO. In CC, see HELP SET/FILTER.\n") ||
			strings.Contains(ccRule, "\nExample: ") {
			t.Fatalf("CC Help %s doesn't point to native filters: %q", topic, ccRule)
		}
		for _, dialect := range []string{"go", "cc"} {
			page := p.ProcessCommandForClient("HELP "+verb, "N0CALL", "", nil, dialect)
			if !strings.HasPrefix(page, "Spot rules (GO only)\n") || !strings.Contains(page, table) ||
				!strings.Contains(page, "SELF separately controls live spots of your own call.") {
				t.Fatalf("%s Help %s doesn't distinguish spot rules from delivery switches", dialect, verb)
			}
		}
	}
}

// Literal wording controls complement the executable admission and recipe
// tests. Catalog enumeration and synchronized golden files are not the oracle.
func TestHelpWordingDXAdmissionAndOutcome(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		response := p.ProcessCommandForClient("HELP DX", "W1ABC-18", "", nil, dialect)
		assertHelpWidth(t, response)
		flat := strings.Join(strings.Fields(response), " ")
		for _, required := range []string{
			"DX <freq_khz> <callsign> [comment]", "DX <callsign> <freq_khz> [comment]",
			"Frequency is in kHz (e.g., 7001.0).",
			"Use plain decimal numbers; exponent notation is not accepted.",
			"Values in gaps between bands remain accepted.",
			"Comment: ASCII letters, digits, spaces, and / # @ - . , * ? _.",
			"Other punctuation and non-ASCII characters are rejected.",
			"The whole command has a node-configured byte limit (default 128).",
			"Mode/report may be parsed from the comment.",
			"Rejects invalid callsigns and CTY-unknown DX when CTY is enabled.",
			"Requires a valid logged-in callsign.",
			"Logins ending TEST require a CTY-known baseline callsign.",
			"When enabled, station reputation admission is shared across numeric SSIDs.",
			"Posting admission may respond Spot rejected.",
			"Spot queued confirms intake; filtering and processing determine delivery.",
			"A missing or full input queue refuses the spot with an explicit response.",
		} {
			if !strings.Contains(flat, required) {
				t.Fatalf("%s HELP DX lacks admission/outcome contract %q: %q", dialect, required, response)
			}
		}
		for _, unsupported := range []string{"free text", "guarantees delivery", "bypasses reputation", "SSID has its own allowance", "FCC license required to post"} {
			if strings.Contains(flat, unsupported) {
				t.Fatalf("%s HELP DX invented admission/delivery claim %q", dialect, unsupported)
			}
		}
	}
}

func helpWordingStateCodes() []string {
	// 60 FCC mailing codes include territories and military addresses; the
	// other 13 are Canadian provinces/territories, including NT, NU and YT.
	return strings.Fields("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT")
}

func helpWordingExtractMailingCodes(t *testing.T, response string) []string {
	t.Helper()
	const heading = "DX/DESTATE:"
	if strings.Count(response, heading) != 1 {
		t.Fatalf("missing or duplicate mailing-code section: %q", response)
	}
	_, tail, _ := strings.Cut(response, heading)
	var rows []string
	for _, row := range strings.Split(tail, "\n") {
		if strings.HasPrefix(strings.TrimSpace(row), "CONF:") {
			break
		}
		rows = append(rows, row)
	}
	return strings.FieldsFunc(strings.Join(rows, " "), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\r' })
}

func TestHelpWordingRegisteredMailingVocabulary(t *testing.T) {
	want := helpWordingStateCodes()
	if len(want) != 73 || !slices.Equal(spot.StateCodes(), want) {
		t.Fatalf("shared state owner differs from the literal 73 registered-mailing codes: %v", spot.StateCodes())
	}
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go"} {
		for _, topic := range []string{"RULE"} {
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			if codes := helpWordingExtractMailingCodes(t, response); !slices.Equal(codes, want) {
				t.Fatalf("%s HELP %s omitted, duplicated or changed mailing codes: %v", dialect, topic, codes)
			}
		}
		response := p.ProcessCommandForClient("HELP RULE", "W1ABC-18", "", nil, dialect)
		if !strings.Contains(strings.Join(strings.Fields(response), " "), "DXSTATE/DESTATE use US/Canadian registered mailing codes. Missing values are unknown.") ||
			strings.Contains(response, "show FCC mailing-address codes") {
			t.Fatalf("%s readback help attributes Canadian data to FCC: %q", dialect, response)
		}
	}
}

func TestHelpWordingBooleanReplacementAndExclusiveRecipe(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go"} {
		for _, topic := range []string{"FILTERS"} {
			response := p.ProcessCommandForClient(strings.TrimSpace("HELP "+topic), "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			flat := strings.Join(strings.Fields(response), " ")
			for _, required := range []string{
				"If you have PASS rules, a spot must match at least one of them.",
				"A matching REJECT rule blocks a spot, even if a PASS rule matches.",
				"Adding a PASS rule can let in more spots. Edit a rule to narrow it.",
				"HELP FILTER EXAMPLES",
			} {
				if !strings.Contains(flat, required) {
					t.Fatalf("%s HELP %s omits Boolean qualification %q", dialect, topic, required)
				}
			}
			for _, misleading := range []string{
				"Values you do not name remain unchanged.",
				"Allowed selections do not generally replace your existing selections.",
				"List is comma or space separated; use ALL to allow all.",
				"List is comma or space separated; use ALL to block all.",
				"RESET MODE",
			} {
				if strings.Contains(flat, misleading) {
					t.Fatalf("%s HELP %s retains unsupported/generalized wording %q", dialect, topic, misleading)
				}
			}
		}
		examples := p.ProcessCommandForClient("HELP FILTER EXAMPLES", "W1ABC-18", "", nil, dialect)
		warning := "Removes all your GO rules. Your other settings stay unchanged."
		reset := strings.Index(examples, "  RESET FILTER RULE ALL\n")
		if !strings.Contains(examples, warning) || strings.Index(examples, warning) > reset ||
			!strings.Contains(examples, "  RESET FILTER RULE ALL\n  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN\n  SHOW FILTER\n  RESUME\n") ||
			!strings.Contains(examples, "This clears your rules. It does not restore the starting rule.") {
			t.Fatal("GO examples lost the warning, complete recipe, reset boundary or resume")
		}
	}
}

func TestHelpWordingMachineSchema6LegacyAnd7Complete(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
			topic := "GET YAML " + resource
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, dialect)
			flat := strings.Join(strings.Fields(response), " ")
			for _, want := range []string{topic + " SCHEMA 6 [ID <id>]", topic + " SCHEMA 7 [ID <id>]", "Schema 6 is a legacy view", "complete both-bank backup", "65,536 bytes", "262,144 bytes"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("%s %s omitted %q", dialect, topic, want)
				}
			}
			if strings.Contains(flat, "SCHEMA 2|3|4") || strings.Contains(flat, "SCHEMA 5") || strings.Contains(flat, "Default schema 1") {
				t.Fatalf("%s %s advertises an obsolete machine schema", dialect, topic)
			}
		}
	}
}
