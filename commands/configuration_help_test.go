package commands

import (
	"strings"
	"testing"
)

func TestConfigurationCommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		response := p.ProcessCommandForClient("HELP SHOW FILTER", "W1ABC-1", "", nil, dialect)
		for _, want := range []string{"FULL", "<category>", "65,536", "always pause", "wraps finite selections", "grid and zone lists use counts", "PASS/REJECT", "ON/OFF", "GET YAML FILTER"} {
			if !strings.Contains(response, want) {
				t.Fatalf("readback help omits %q: %s", want, response)
			}
		}
	}
}
