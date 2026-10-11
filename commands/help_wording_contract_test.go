package commands

import (
	"os"
	"path/filepath"
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
	const heading = "US/Canada mailing codes (states, provinces, territories):\n"
	if strings.Count(response, heading) != 1 {
		t.Fatalf("missing or duplicate mailing-code section: %q", response)
	}
	_, tail, _ := strings.Cut(response, heading)
	var rows []string
	for _, row := range strings.Split(tail, "\n") {
		if strings.TrimSpace(row) == "" {
			break
		}
		rows = append(rows, row)
	}
	return strings.FieldsFunc(strings.Join(rows, " "), func(r rune) bool { return r == ',' || r == ' ' })
}

func TestHelpWordingRegisteredMailingVocabulary(t *testing.T) {
	want := helpWordingStateCodes()
	if len(want) != 73 || !slices.Equal(spot.StateCodes(), want) {
		t.Fatalf("shared state owner differs from the literal 73 registered-mailing codes: %v", spot.StateCodes())
	}
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"FILTERS", "PASS", "REJECT"} {
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			if codes := helpWordingExtractMailingCodes(t, response); !slices.Equal(codes, want) {
				t.Fatalf("%s HELP %s omitted, duplicated or changed mailing codes: %v", dialect, topic, codes)
			}
		}
		for _, topic := range []string{"", "FILTERS", "PASS", "REJECT"} {
			response := p.ProcessCommandForClient(strings.TrimSpace("HELP "+topic), "W1ABC-18", "", nil, dialect)
			flat := strings.Join(strings.Fields(response), " ")
			for _, description := range []string{
				"DXSTATE Spotted station's US/Canadian mailing code.",
				"DESTATE Spotter's US/Canadian mailing code.",
			} {
				if !strings.Contains(flat, description) {
					t.Fatalf("%s HELP %s misdescribed the shared mailing vocabulary: %q", dialect, topic, response)
				}
			}
		}
		readback := "SHOW FILTER"
		if dialect == "cc" {
			readback = "SHOW/FILTER"
		}
		response := p.ProcessCommandForClient("HELP "+readback, "W1ABC-18", "", nil, dialect)
		if !strings.Contains(strings.Join(strings.Fields(response), " "), "DESTATE/DXSTATE show US/Canadian registered mailing codes; empty means unknown.") ||
			strings.Contains(response, "show FCC mailing-address codes") {
			t.Fatalf("%s readback help attributes Canadian data to FCC: %q", dialect, response)
		}
	}
}

func TestHelpWordingModeSpecificVisibilityAndExclusiveRecipe(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"", "PASS", "REJECT"} {
			response := p.ProcessCommandForClient(strings.TrimSpace("HELP "+topic), "W1ABC-18", "", nil, dialect)
			assertHelpWidth(t, response)
			flat := strings.Join(strings.Fields(response), " ")
			for _, required := range []string{
				"Filter categories have their own selection rules; see HELP PASS/REJECT.",
				"MODE preserves visibility for every mode you do not name.",
			} {
				if !strings.Contains(flat, required) {
					t.Fatalf("%s HELP %s omits MODE-specific qualification %q", dialect, topic, required)
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
			block, allow := "REJECT MODE ALL", "PASS MODE CW,FT8"
			if dialect == "cc" {
				block, allow = "UNSET/FILTER MODE ALL", "SET/FILTER MODE CW,FT8"
			}
			if !strings.Contains(response, "  "+block+"\n  "+allow+"\n") {
				t.Fatalf("%s HELP %s lost the block-before-pass exclusive MODE recipe", dialect, topic)
			}
		}
	}
}

func TestHelpWordingModeOpeningDocumentsExecutableCommand(t *testing.T) {
	// Decision/troubleshooting records may retain an explicit correction note.
	// Reject their original executable claims, not the historical command name.
	for _, tc := range []struct{ path, unsupportedRecipe string }{
		{"telnet/README.md", "RESET MODE"},
		{"docs/decisions/ADR-0052-region-aware-mode-inference-and-unknown-filter-token.md", "PASS MODE ALL and RESET MODE continue to allow blank-mode spots."},
		{"docs/decisions/ADR-0272-mode-visibility-deltas.md", "PASS MODE ALL or RESET MODE opens MODE explicitly."},
		{"docs/troubleshooting/TSR-0053-mode-delta-whitelist-transition.md", "PASS MODE ALL or RESET MODE explicitly opens the MODE filter."},
	} {
		data, err := os.ReadFile(filepath.Join("..", filepath.FromSlash(tc.path)))
		if err != nil {
			t.Fatal(err)
		}
		flat := strings.Join(strings.Fields(strings.ReplaceAll(string(data), "`", "")), " ")
		if strings.Contains(flat, tc.unsupportedRecipe) || !strings.Contains(flat, "PASS MODE ALL") {
			t.Fatalf("%s must teach PASS MODE ALL without its unsupported executable recipe %q", tc.path, tc.unsupportedRecipe)
		}
	}
}
