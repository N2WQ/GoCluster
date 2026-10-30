package telnet

import (
	"errors"
	"os"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func bankedV6Proposal(t *testing.T, c *Client, verb, resource, configuration string) (machineCommand, v6MachineRequest) {
	t.Helper()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(v6MachineRequestFixture(configuration, verb != "VALIDATE")), "opaque-revision-1", revision, 1)
	cmd := machineCommand{Verb: verb, Resource: resource}
	r, err := decodeV6MachineRequest([]byte(body), cmd, nil)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, r
}

func bankedV6SelectGO(t *testing.T, c *Client) {
	t.Helper()
	cfg, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Settings.Dialect = "go"
	canonical, runtime, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks = &canonical.Filters
	c.filter = &runtime
	c.dialect = DialectGo
	c.configuredSettings = canonical.Settings
}

func TestV6BankedCCReadbackRestrictionsAndHonestMetadata(t *testing.T) {
	s, c := v7MachineTestClient(t)
	cfg, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	preset, err := filter.V7PresetFromConfiguration(cfg, "cc")
	if err != nil {
		t.Fatal(err)
	}
	c.v7PresetReferences.CC = &filter.V7PresetReference{Name: "NATIVE", Baseline: preset}
	for _, resource := range []string{"FILTER", "CONFIG", "SETTINGS", "CAPABILITIES"} {
		result := s.handleV6MachineCommand(c, "GET YAML "+resource+" SCHEMA 6 ID legacy-1")
		if !result.handled || result.terminal {
			t.Fatalf("%s dispatch=%+v", resource, result)
		}
		msg := <-c.controlChan
		response := string(msg.raw)
		msg.raw = nil
		msg.configurationReservation.Release()
		if !strings.Contains(response, "schema_version: 6\r\n") || strings.Contains(response, "rules:") || strings.Contains(response, "rule_fields:") || strings.Contains(response, "baseline:") || strings.Contains(response, "modes:") {
			t.Fatalf("%s fabricated/exposed inactive GO or native filter state: %q", resource, response)
		}
		if resource == "FILTER" || resource == "CONFIG" {
			if !strings.Contains(response, "code: schema7_required") {
				t.Fatalf("%s was not refused: %q", resource, response)
			}
		} else if !strings.Contains(response, "compatibility_warning:") || !strings.Contains(response, "name: NATIVE") {
			t.Fatalf("%s omitted warning or active native association: %q", resource, response)
		}
	}
}

func TestV6BankedCCRefusesFilterConfigAndValidation(t *testing.T) {
	s, c := v7MachineTestClient(t)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	for _, tc := range []struct{ verb, resource, body string }{
		{"PATCH", "FILTER", "rules: []\n"},
		{"PATCH", "CONFIG", "filters: {rules: []}\n"},
	} {
		cmd, r := bankedV6Proposal(t, c, tc.verb, tc.resource, tc.body)
		response := s.applyBankedV6MachineRequest(c, cmd, r)
		if !strings.Contains(response, "code: schema7_required") {
			t.Fatalf("%s %s did not refuse CC: %q", tc.verb, tc.resource, response)
		}
	}
	legacy := filter.V6Configuration{Filters: before.Filters.GO, Settings: before.Settings}
	encoded, err := yaml.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	cmd, r := bankedV6Proposal(t, c, "VALIDATE", "CONFIG", string(encoded))
	if response := s.applyBankedV6MachineRequest(c, cmd, r); !strings.Contains(response, "code: schema7_required") {
		t.Fatal(response)
	}
	after, err := c.captureV7Configuration()
	if err != nil || !after.Equal(before) || commits != 0 {
		t.Fatalf("refused legacy operation changed state or committed: err=%v commits=%d", err, commits)
	}
}

func TestV6BankedSettingsSwitchPersistsBothBanksAsSeven(t *testing.T) {
	s, c := v7MachineTestClient(t)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	cmd, r := bankedV6Proposal(t, c, "PATCH", "SETTINGS", "dialect: go\n")
	response := s.applyBankedV6MachineRequest(c, cmd, r)
	if !strings.Contains(response, "schema_version: 6") || !strings.Contains(response, "persisted: true") || strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	after, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	if c.dialect != DialectGo || !after.Filters.CC.Equal(before.Filters.CC) || !(filter.V6Configuration{Filters: after.Filters.GO}).Equal(filter.V6Configuration{Filters: before.Filters.GO}) {
		t.Fatal("legacy SETTINGS lost a bank or failed selection")
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil || record.ConfigurationVersion != 7 || !record.Configuration.Equal(after) {
		t.Fatalf("legacy SETTINGS did not commit complete v7 profile: err=%v", err)
	}
	result := s.handleV6MachineCommand(c, "GET YAML CONFIG SCHEMA 6 ID go-view-1")
	if !result.handled || result.terminal {
		t.Fatalf("GO view failed: %+v", result)
	}
	msg := <-c.controlChan
	frame := string(msg.raw)
	msg.raw = nil
	msg.configurationReservation.Release()
	if !strings.Contains(frame, "rules:") || !strings.Contains(frame, "compatibility_warning:") || strings.Contains(frame, "geography:") {
		t.Fatalf("GO legacy view lost GO or exposed CC: %q", frame)
	}
}

func TestV6BankedGOWritesPreserveCCAndRequireGOConfigResult(t *testing.T) {
	s, c := v7MachineTestClient(t)
	bankedV6SelectGO(t, c)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	cmd, r := bankedV6Proposal(t, c, "PATCH", "CONFIG", "settings: {dialect: cc}\n")
	if response := s.applyBankedV6MachineRequest(c, cmd, r); !strings.Contains(response, "code: schema7_required") {
		t.Fatalf("legacy CONFIG selected CC: %q", response)
	}
	cmd, r = bankedV6Proposal(t, c, "PATCH", "FILTER", "rules: []\nallow_self: true\n")
	if response := s.applyBankedV6MachineRequest(c, cmd, r); !strings.Contains(response, "persisted: true") || strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	after, err := c.captureV7Configuration()
	if err != nil || len(after.Filters.GO.Rules) != 0 || after.Filters.GO.AllowSelf != filter.DefaultBoolTrue || !after.Filters.CC.Equal(before.Filters.CC) {
		t.Fatalf("legacy GO filter update lost native bank: err=%v", err)
	}
}

func TestV6BankedCommitFailureAndInactiveRevisionAreAtomic(t *testing.T) {
	s, c := v7MachineTestClient(t)
	bankedV6SelectGO(t, c)
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	cmd, r := bankedV6Proposal(t, c, "PATCH", "FILTER", "allow_self: true\n")
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	if response := s.applyBankedV6MachineRequest(c, cmd, r); !strings.Contains(response, "code: persistence_failed") {
		t.Fatal(response)
	}
	after, err := c.captureV7Configuration()
	if err != nil || !after.Equal(before) {
		t.Fatal("legacy failed commit changed live preferences")
	}
	s.commitConfigurationFn = nil
	copy := c.filterBanks.CC.Clone()
	copy.Modes = []string{"FT4"}
	banks := *c.filterBanks
	banks.CC = copy
	c.filterBanks = &banks
	if response := s.applyBankedV6MachineRequest(c, cmd, r); !strings.Contains(response, "code: revision_conflict") {
		t.Fatalf("inactive CC did not participate in token: %q", response)
	}
	entries, err := os.ReadDir(filter.UserDataDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed/conflicting legacy request wrote data: entries=%d err=%v", len(entries), err)
	}
}

func TestV6BankedReceiverRetainsLegacyLimit(t *testing.T) {
	s := &Server{}
	c, conn := newYAMLTestClient("---\nschema_version: 6\nvalue: " + strings.Repeat("x", 70_000) + "\n...\nRESUME\n")
	c.server = s
	initialInput, initialOutput := configurationTransferBudgetStats()
	_, terminal, err := s.receiveBankedV6MachineRequest(c, machineCommand{Verb: "PATCH", Resource: "SETTINGS"})
	if !terminal || !errors.Is(err, errYAMLUploadTooLarge) || !conn.closed.Load() {
		t.Fatalf("legacy receive limit widened: terminal=%v err=%v", terminal, err)
	}
	assertConfigurationTransferStats(t, initialInput, initialOutput)
}
