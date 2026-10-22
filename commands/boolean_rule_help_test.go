package commands

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestBooleanRuleHelpSharedReference(t *testing.T) {
	text := strings.Join(BooleanRuleHelpLines(), "\n")
	for _, required := range []string{
		"NOT then AND then OR", "UNLIKELY does not include CLOSED",
		"including human spots", "SOURCE=SKIMMER", "YAML schema 5",
		"To narrow defaults, edit or remove slot 0.",
		"Editing a slot replaces both", "Slot IDs are not priorities",
		"No rules means no personal restriction", "RESET FILTER does not reinstall that rule",
		"Emptied rules are ignored.", "Other conditions still apply.",
		"Saved rules stay unchanged.", "Phrases match anywhere in a comment.",
		"Matching ignores ASCII letter case.",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("shared Boolean reference omitted %q", required)
		}

	}
	for _, values := range [][]string{spot.SupportedBandNames(), filter.SupportedModes(), spot.StateCodes()} {
		for _, value := range values {
			if !strings.Contains(text, value) {
				t.Fatalf("shared reference omitted configured value %q", value)
			}
		}
	}
}

func TestBooleanRuleHelpReadableLayout(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		t.Run(dialect, func(t *testing.T) {
			text := p.ProcessCommandForClient("HELP RULE", "N0CALL", "", nil, dialect)
			assertHelpWidth(t, text)
			previous := -1
			for _, heading := range []string{
				"Commands:", "Editing and resetting:", "Starting rules:",
				"How rules combine:", "Report sources:", "Writing expressions:",
				"Text and comments:", "Missing values and special cases:",
				"NEARBY and separate controls:", "Fields and values:",
				"Examples:", "Limits:", "Supported values:",
			} {
				index := strings.Index(text, "\n\n  "+heading+"\n")
				if index <= previous {
					t.Fatalf("missing, unspaced or out-of-order heading %q", heading)
				}
				previous = index
			}
			for _, example := range []string{
				`COMMENT="CQ ""DX""  !;=()[]"`,
				"PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN",
				`REJECT RULE 9 SNR<-15 OR COMMENT="test beacon"`,
				"PASS RULE 2 NOT (DXCONT=EU OR BAND=20M)",
			} {
				if !strings.Contains(text, "\n  "+example+"\n") {
					t.Fatalf("literal example was changed or wrapped: %q", example)
				}
			}
		})
	}
}
