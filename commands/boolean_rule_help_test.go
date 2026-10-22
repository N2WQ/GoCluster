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
		"edit or remove ordinary default slot 0",
		"setting it replaces both", "Slot IDs are not priorities",
		"No rules means no personal restriction", "RESET FILTER does not reinstall that rule",
		"Emptied rules are ignored; non-geography conditions survive",
		"retaining full saved rules", "ASCII case-insensitive literal substring",
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
