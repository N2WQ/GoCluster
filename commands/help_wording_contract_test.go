package commands

import (
	"slices"
	"strings"
	"testing"

	"dxcluster/spot"
)

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
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"RULE"} {
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			if codes := helpWordingExtractMailingCodes(t, response); !slices.Equal(codes, want) {
				t.Fatalf("%s HELP %s omitted, duplicated or changed mailing codes: %v", dialect, topic, codes)
			}
		}
		response := p.ProcessCommandForClient("HELP RULE", "W1ABC-18", "", nil, dialect)
		if !strings.Contains(strings.Join(strings.Fields(response), " "), "DXSTATE/DESTATE use US/Canadian registered mailing codes; missing values are unknown.") ||
			strings.Contains(response, "show FCC mailing-address codes") {
			t.Fatalf("%s readback help attributes Canadian data to FCC: %q", dialect, response)
		}
	}
}

func TestHelpWordingBooleanReplacementAndExclusiveRecipe(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"", "FILTERS"} {
			response := p.ProcessCommandForClient(strings.TrimSpace("HELP "+topic), "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			flat := strings.Join(strings.Fields(response), " ")
			for _, required := range []string{
				"Slots 0..9 identify rules, not priorities. Each edit replaces a full rule.",
				"Any PASS can qualify; any matching REJECT vetoes it.",
				"No active PASS means unrestricted except for matching REJECT rules.",
				"Reset does not reinstall default slot 0.",
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
			if !strings.Contains(response, "  RESET FILTER RULE ALL\n  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8\n") {
				t.Fatalf("%s HELP %s lost the full-replacement exclusive recipe", dialect, topic)
			}
		}
	}
}

func TestHelpWordingMachineSchema5Only(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
			topic := "GET YAML " + resource
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC", "", nil, dialect)
			flat := strings.Join(strings.Fields(response), " ")
			for _, want := range []string{topic + " SCHEMA 5 [ID <id>]", "Explicit SCHEMA 5 is required", "older schemas are refused", "No old machine-schema migration"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("%s %s omitted %q", dialect, topic, want)
				}
			}
			if strings.Contains(flat, "SCHEMA 2|3|4") || strings.Contains(flat, "Default schema 1") {
				t.Fatalf("%s %s advertises an obsolete machine schema", dialect, topic)
			}
		}
	}
}
