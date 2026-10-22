package commands

import (
	"strings"
	"testing"
)

func TestConfigurationCommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		response := p.ProcessCommandForClient("HELP SHOW FILTER", "W1ABC-1", "", nil, dialect)
		for _, want := range []string{"FULL", "RULE", "65,536", "always pause", "saved rules", "NEARBY", "Slot IDs", "ON/OFF", "GET YAML FILTER SCHEMA 5"} {
			if !strings.Contains(response, want) {
				t.Fatalf("readback help omits %q: %s", want, response)
			}
		}
		for _, unsupported := range []string{"<category>", "MINSNR shows", "FULL/category", "schema 1 is unchanged"} {
			if strings.Contains(response, unsupported) {
				t.Fatalf("readback help retains %q: %s", unsupported, response)
			}
		}
	}
}
