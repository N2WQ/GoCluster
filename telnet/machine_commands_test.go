package telnet

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func decodeProposalTest(t *testing.T, c *Client, verb, resource, config string) (machineCommand, v5MachineRequest) {
	t.Helper()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command := machineCommand{Verb: verb, Resource: resource}
	body := fmt.Sprintf("schema_version: 5\nrequest_id: edit-1\nif_revision: %s\nconfiguration:\n%s", revision, config)
	request, err := decodeV5MachineRequest([]byte(body), command, nil)
	if err != nil {
		t.Fatalf("decode literal proposal: %v", err)
	}
	return command, request
}

func completeValidationConfigurationTest() string {
	return "filters:\n" + indentMachineFixture(`rules: []
nearby_enabled: false
include_beacons: DEFAULT
allow_wwv: DEFAULT
allow_wcy: DEFAULT
allow_announce: DEFAULT
allow_self: DEFAULT
allow_toxic: DEFAULT
`) + "settings:\n" + indentMachineFixture(strings.Replace(machineCompleteSettingsFixture, "GO", "go", 1))
}

func TestMachineYAMLPreparationBoundAndCancellation(t *testing.T) {
	s := &Server{shutdown: make(chan struct{})}
	var releases []func()
	for range 4 {
		release, err := s.acquireYAMLPreparation(configurationTestClient(s, "W1ABC"))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if cap(s.yamlPreparation) != 4 || len(s.yamlPreparation) != 4 {
		t.Fatal("preparation permits are not fixed at four")
	}
	waiting := configurationTestClient(s, "W1ABC-1")
	close(waiting.done)
	if release, err := s.acquireYAMLPreparation(waiting); err == nil || release != nil {
		t.Fatal("canceled preparation waiter acquired a permit")
	}
	for _, release := range releases {
		release()
	}
	for _, body := range []string{"[broken", "schema_version: 5\nrequest_id: edit-1\nif_revision: rev-1\nconfiguration: {noise_class: URBAN}\n"} {
		_, _ = s.prepareV5MachineRequest(configurationTestClient(s, "W1ABC-1"), []byte(body), machineCommand{Verb: "PATCH", Resource: "SETTINGS"})
		if len(s.yamlPreparation) != 0 {
			t.Fatal("decoder retained its preparation permit")
		}
	}
}

func TestMachineUnchangedPUTRepairsDurabilityWithoutRuntimeReset(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter = passRuleTestFilter(t, "BAND=20M")
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 15}
	c.configurationInitialized, c.noiseClass = true, "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	c.setSolarSummaryMinutes(15, time.Now())
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	response, _ := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
	if response != fmt.Sprintf("Noise class set to URBAN (warning: failed to persist: %v)\n", os.ErrPermission) ||
		c.noiseClass != "URBAN" || c.configuredSettings.NoiseClass != "URBAN" {
		t.Fatalf("accepted temporary settings edit lost its inline warning/live value: %q", response)
	}
	s.commitConfigurationFn = nil
	c.setDiagMode(diagModePath)
	c.startReadPause(time.Now(), time.Minute)
	c.solarNextSummaryAt = time.Now().Add(time.Minute)
	tick, pause, diag, programs := c.solarNextSummaryAt, c.readPauseUntilUnixNano.Load(), c.getDiagMode(), c.filter.PreparedRules()
	revision, _ := c.configurationRevisionToken()
	command, request := decodeProposalTest(t, c, "PUT", "SETTINGS", "  dialect: go\n  grid: \"\"\n  noise_class: URBAN\n  dedupe_policy: FAST\n  path_min_observation_count: 0\n  solar_summary_minutes: 15\n")
	response = s.applyV5MachineRequest(c, command, request)
	if !strings.Contains(response, "persisted: true") || strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	afterRevision, _ := c.configurationRevisionToken()
	if afterRevision != revision || c.filter.PreparedRules() != programs || c.solarNextSummaryAt != tick ||
		c.readPauseUntilUnixNano.Load() != pause || c.getDiagMode() != diag {
		t.Fatal("durability repair reset immutable rules, revision or temporary runtime state")
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	data := presetDiskBytes(t, path)
	if !bytes.Contains(data, []byte("noise_class: URBAN")) || !bytes.Contains(data, []byte("configuration_version: 5")) {
		t.Fatal("unchanged PUT did not repair durable v5 preferences")
	}
	next := configurationTestClient(s, c.callsign)
	if _, err := s.restoreAndRegisterClient(next, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil ||
		next.configuredSettings.NoiseClass != "URBAN" {
		t.Fatalf("reconnect lost durability repair: %v", err)
	}
	if reconnected, _ := next.configurationRevisionToken(); reconnected == revision {
		t.Fatal("reconnect retained the old session revision")
	}
}
