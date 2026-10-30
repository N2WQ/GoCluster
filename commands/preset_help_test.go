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
			if !strings.Contains(list, topic) {
				t.Fatalf("%s HELP omits %s", dialect, topic)
			}
			response := p.ProcessCommandForClient("HELP "+topic, "N2WQ", "", nil, dialect)
			for _, want := range []string{"Usage: " + topic, "numeric SSIDs", "case-insensitive", "uppercase", "20 presets, 256 KiB", "Login/IP history"} {
				if !strings.Contains(response, want) {
					t.Fatalf("%s %s missing %q: %s", dialect, topic, want, response)
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
			if dialect == "cc" {
				want := "Inspect native CC state."
				if topic == "RESET FILTER" {
					want = "GO dialect only."
				}
				if !strings.Contains(response, want) || !strings.Contains(response, "Usage:") {
					t.Fatalf("cc %s HELP missing %q: %s", topic, want, response)
				}
				continue
			}
			if !strings.Contains(response, "Usage:") || !strings.Contains(response, "filter state") && !strings.Contains(response, "Reset filters") {
				t.Fatalf("%s existing %s HELP changed: %s", dialect, topic, response)
			}
		}
	}
}
