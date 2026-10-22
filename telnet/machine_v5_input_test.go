package telnet

import (
	"strings"
	"testing"
)

func TestV5MachineHeaderMatrix(t *testing.T) {
	for _, verb := range []string{"GET", "PUT", "PATCH", "VALIDATE"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES", "UNKNOWN"} {
			t.Run(verb+"/"+resource, func(t *testing.T) {
				line := verb + " YAML " + resource
				if verb == "GET" {
					line += " SCHEMA 5 ID MiXeD-01"
				}
				command, handled, err := parseV5MachineHeader(line)
				allowed := resource == "CONFIG" ||
					verb != "VALIDATE" && (resource == "FILTER" || resource == "SETTINGS") ||
					verb == "GET" && resource == "CAPABILITIES"
				if !handled || (err == nil) != allowed || command.Verb != verb || command.Resource != resource {
					t.Fatalf("command=%+v handled=%v err=%v", command, handled, err)
				}
				if allowed && verb == "GET" && (command.SchemaVersion != 5 || command.RequestID != "MiXeD-01") {
					t.Fatalf("GET metadata changed: %+v", command)
				}
				if allowed && verb != "GET" && command.SchemaVersion != 0 {
					t.Fatalf("write version must come from body: %+v", command)
				}
			})
		}
	}
	for _, line := range []string{
		"GET", "PUT YAML", "GET JSON CONFIG SCHEMA 5", "GET YAML CONFIG",
		"GET YAML CONFIG SCHEMA", "GET YAML CONFIG ID abc", "GET YAML CONFIG SCHEMA 4",
		"GET YAML CONFIG SCHEMA 05", "GET YAML CONFIG SCHEMA +5",
		"GET YAML CONFIG SCHEMA 5 ID", "GET YAML CONFIG SCHEMA 5 ID a_b",
		"GET YAML CONFIG SCHEMA 5 ID é", "GET YAML CONFIG SCHEMA 5 ID " + strings.Repeat("x", 33),
		"GET YAML CONFIG SCHEMA 5 ID abc extra", "PUT YAML FILTER SCHEMA 5", "PATCH YAML FILTER ID abc",
	} {
		if _, handled, err := parseV5MachineHeader(line); !handled || err == nil {
			t.Fatalf("accepted invalid header %q: handled=%v err=%v", line, handled, err)
		}
	}
	for _, line := range []string{"SHOW FILTER", "PASS RULE 0 MODE=CW", "", "GETTING YAML CONFIG"} {
		if _, handled, err := parseV5MachineHeader(line); handled || err != nil {
			t.Fatalf("claimed human command %q", line)
		}
	}
	for _, line := range []string{"get yaml filter schema 5", " GET YAML CONFIG SCHEMA 5 ID - ", "GET YAML SETTINGS SCHEMA 5 ID " + strings.Repeat("A", 32)} {
		if _, handled, err := parseV5MachineHeader(line); !handled || err != nil {
			t.Fatalf("rejected valid header %q: %v", line, err)
		}
	}
}

func TestV5MachineHeaderRefusesOldSchemas(t *testing.T) {
	for _, version := range []string{"1", "2", "3", "4", "6"} {
		if _, handled, err := parseV5MachineHeader("GET YAML CONFIG SCHEMA " + version); !handled || err == nil {
			t.Fatalf("accepted unsupported schema %s", version)
		}
	}
}
