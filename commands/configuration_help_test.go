package commands

import (
	"strings"
	"testing"
)

func TestConfigurationCommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		response := p.ProcessCommandForClient("HELP SHOW FILTER", "W1ABC-1", "", nil, dialect)
		if dialect == "cc" {
			for _, want := range []string{"native CC state", "DOC|DOS|DXCTY|DXSTATE", "INHERIT", "active allow-all", "SCHEMA 7", "RESUME"} {
				if !strings.Contains(response, want) {
					t.Fatalf("native CC readback help omits %q: %s", want, response)
				}
			}
			continue
		}
		for _, want := range []string{"FULL", "RULE", "65,536", "always pause", "canonical numbered rules", "NEARBY", "Slot IDs", "ON/OFF", "GET YAML FILTER SCHEMA 6"} {
			if !strings.Contains(response, want) {
				t.Fatalf("readback help omits %q: %s", want, response)
			}
		}
		for _, unsupported := range []string{"<category>", "MINSNR shows", "FULL/category", "schema 1 is unchanged", "effective NEARBY expressions", "NEARBY omits geography", "emptied rules"} {
			if strings.Contains(response, unsupported) {
				t.Fatalf("readback help retains %q: %s", unsupported, response)
			}
		}
	}
}

func TestNativeCC11CommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		index := p.ProcessCommandForClient("HELP COMMANDS", "W1ABC-1", "", nil, dialect)
		for _, topic := range []string{"SET/VE7CC", "UNSET/VE7CC"} {
			response := p.ProcessCommandForClient("HELP "+topic, "W1ABC-1", "", nil, dialect)
			if !strings.HasPrefix(response, topic+" - ") || !strings.Contains(index, topic) {
				t.Fatalf("%s native selector help is not reachable: %s", dialect, response)
			}
			flat := strings.Join(strings.Fields(response), " ")
			for _, want := range []string{"after login", "new connection", "not saved", "raw comments", "CWSPEED", "DIAG", "65,536", "busy", "ADIF", "WPXLOC", "unqualified"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("%s %s help omits %q: %s", dialect, topic, want, response)
				}
			}
			assertHelpWidth(t, response)
		}
	}
}
