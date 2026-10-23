package telnet

import (
	"fmt"
	"strings"
	"testing"
)

const machineCompleteSettingsFixture = `dialect: GO
grid: ""
noise_class: QUIET
dedupe_policy: FAST
path_min_observation_count: 0
solar_summary_minutes: 0
`

func machineRequestFixture(configuration string, revision bool) []byte {
	header := "schema_version: 5\nrequest_id: noise-Ab1\n"
	if revision {
		header += "if_revision: opaque-revision-1\n"
	}
	return []byte(header + "configuration:\n" + indentMachineFixture(configuration))
}

func indentMachineFixture(value string) string {
	return "  " + strings.ReplaceAll(strings.TrimSuffix(value, "\n"), "\n", "\n  ") + "\n"
}

func TestMachineActiveSchemaRefusesOldVersions(t *testing.T) {
	for version := 1; version < 5; version++ {
		body := fmt.Sprintf("schema_version: %d\nrequest_id: old-1\nif_revision: rev-1\nconfiguration: {noise_class: URBAN}\n", version)
		if _, err := decodeV5MachineRequest([]byte(body), machineCommand{Verb: "PATCH", Resource: "SETTINGS"}, nil); err == nil ||
			!strings.Contains(err.Error(), "schema version 5") {
			t.Fatalf("schema %d was accepted or failed for an unrelated reason: %v", version, err)
		}
	}
}
