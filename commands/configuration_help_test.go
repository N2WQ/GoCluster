package commands

import (
	"strings"
	"testing"
)

func TestConfigurationCommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"SHOW SETTINGS", "GET YAML FILTER", "GET YAML SETTINGS", "GET YAML CONFIG", "GET YAML CAPABILITIES", "PUT YAML FILTER", "PUT YAML SETTINGS", "PUT YAML CONFIG", "PATCH YAML FILTER", "PATCH YAML SETTINGS", "PATCH YAML CONFIG", "VALIDATE YAML CONFIG"} {
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC-1", "", nil, dialect)
			if !strings.Contains(response, "Usage: "+topic) {
				t.Fatalf("%s %s missing usage: %q", dialect, topic, response)
			}
			for _, line := range strings.Split(response, "\n") {
				if len(line) > 78 {
					t.Fatalf("%s help exceeds 78 bytes: %q", topic, line)
				}
			}
		}
		response := p.ProcessCommandForClient("HELP SHOW FILTER", "W1ABC-1", "", nil, dialect)
		for _, want := range []string{"FULL", "<category>", "65,536", "always pause", "wraps finite selections", "grid and zone lists use counts"} {
			if !strings.Contains(response, want) {
				t.Fatalf("readback help omits %q: %s", want, response)
			}
		}
	}
}
