package telnet

import (
	"reflect"
	"strings"
	"testing"
)

func TestV7MachineHeaderSelectsOnlyExplicitLargeUploads(t *testing.T) {
	for _, verb := range []string{"GET", "PUT", "PATCH", "VALIDATE"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES", "UNKNOWN"} {
			t.Run(verb+"/"+resource, func(t *testing.T) {
				line := verb + " YAML " + resource + " SCHEMA 7"
				if verb == "GET" {
					line += " ID MiXeD-01"
				}
				command, handled, err := parseV7MachineHeader(line)
				allowed := resource == "CONFIG" ||
					verb != "VALIDATE" && (resource == "FILTER" || resource == "SETTINGS") ||
					verb == "GET" && resource == "CAPABILITIES"
				if !handled || (err == nil) != allowed || command.Verb != verb || command.Resource != resource {
					t.Fatalf("command=%+v handled=%v err=%v", command, handled, err)
				}
				if allowed && command.SchemaVersion != 7 {
					t.Fatalf("explicit upload/readback did not select schema 7: %+v", command)
				}
				if allowed && verb == "GET" && command.RequestID != "MiXeD-01" {
					t.Fatalf("request identifier changed: %+v", command)
				}
			})
		}
	}
	for _, line := range []string{
		"GET YAML CONFIG SCHEMA 7 ID", "GET YAML CONFIG SCHEMA 7 ID a_b",
		"GET YAML CONFIG SCHEMA 7 ID " + strings.Repeat("a", 33),
		"GET YAML CONFIG SCHEMA 7 ID a extra", "GET JSON CONFIG SCHEMA 7",
		"PUT YAML FILTER SCHEMA 7 ID a", "PATCH YAML SETTINGS SCHEMA 7 extra",
		"VALIDATE YAML CONFIG SCHEMA 7 ID a",
	} {
		if _, handled, err := parseV7MachineHeader(line); !handled || err == nil {
			t.Fatalf("accepted malformed schema-7 header %q: handled=%v err=%v", line, handled, err)
		}
	}
	for _, line := range []string{
		"get yaml config schema 7 id MiXeD-01", "put yaml filter schema 7",
		" patch YAML settings SCHEMA 7 ", "validate yaml config schema 7",
	} {
		if command, handled, err := parseV7MachineHeader(line); !handled || err != nil || command.SchemaVersion != 7 {
			t.Fatalf("rejected valid explicit header %q: %+v handled=%v err=%v", line, command, handled, err)
		}
	}
}

func TestV7MachineHeaderPreservesLegacySelectionAndErrors(t *testing.T) {
	for _, line := range []string{
		"PUT YAML FILTER", "PATCH YAML SETTINGS", "VALIDATE YAML CONFIG",
		"GET YAML CONFIG SCHEMA 6 ID MiXeD-01", "GET YAML CAPABILITIES SCHEMA 6",
		"GET YAML CONFIG", "GET YAML CONFIG SCHEMA 07", "GET YAML CONFIG SCHEMA +7",
		"PUT YAML CONFIG SCHEMA 6", "PATCH YAML CONFIG SCHEMA 8", "PUT YAML CONFIG SCHEMA",
		"SHOW FILTER", "PASS RULE 0 MODE=CW", "", "GETTING YAML CONFIG SCHEMA 7",
	} {
		want, wantHandled, wantErr := parseV6MachineHeader(line)
		got, gotHandled, gotErr := parseV7MachineHeader(line)
		if !reflect.DeepEqual(got, want) || gotHandled != wantHandled ||
			(gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
			t.Fatalf("legacy selection changed for %q: got=%+v/%v/%v want=%+v/%v/%v", line, got, gotHandled, gotErr, want, wantHandled, wantErr)
		}
	}
}
