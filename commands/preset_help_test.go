package commands

import (
	"strings"
	"testing"
)

func TestNamedPresetHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		list := p.ProcessCommandForClient("HELP", "N2WQ", "", nil, dialect)
		for _, verb := range []string{"SAVE", "LIST", "LOAD", "DELETE"} {
			topic := verb + " PRESET"
			if !strings.Contains(list, topic+" - ") {
				t.Fatalf("%s HELP omits %s", dialect, topic)
			}
			response := p.ProcessCommandForClient("HELP "+topic, "N2WQ", "", nil, dialect)
			for _, want := range []string{"Usage: " + topic, "numeric SSIDs", "case-insensitive", "uppercase", "20 presets, 256 KiB", "Login/IP history"} {
				if !strings.Contains(response, want) {
					t.Fatalf("%s %s missing %q: %s", dialect, topic, want, response)
				}
			}
			for _, line := range strings.Split(response, "\n") {
				if len(line) > 78 {
					t.Fatalf("HELP line exceeds 78 characters: %q", line)
				}
			}
			obsolete := verb + " FILTER"
			if strings.Contains(list, obsolete+" - ") {
				t.Fatalf("%s HELP advertises obsolete %s", dialect, obsolete)
			}
			response = p.ProcessCommandForClient("HELP "+obsolete, "N2WQ", "", nil, dialect)
			if strings.Contains(response, "Usage: "+obsolete) {
				t.Fatalf("%s HELP accepts obsolete %s", dialect, obsolete)
			}
		}
		for _, topic := range []string{"SHOW FILTER", "RESET FILTER"} {
			response := p.ProcessCommandForClient("HELP "+topic, "N2WQ", "", nil, dialect)
			if !strings.Contains(response, "Usage:") || !strings.Contains(response, "filter state") && !strings.Contains(response, "Reset filters") {
				t.Fatalf("%s existing %s HELP changed: %s", dialect, topic, response)
			}
		}
	}
}
