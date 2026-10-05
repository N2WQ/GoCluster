package telnet

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func decodeProposalTest(t *testing.T, c *Client, verb, resource, config string) (machineCommand, machineRequest) {
	t.Helper()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command := machineCommand{Verb: verb, Resource: resource}
	body := fmt.Sprintf("schema_version: 1\nrequest_id: edit-1\nif_revision: %s\nconfiguration:\n%s", revision, config)
	request, err := decodeMachineRequest([]byte(body), command)
	if err != nil {
		t.Fatalf("decode literal proposal: %v", err)
	}
	return command, request
}

func TestMachineWriteFailureLeavesLiveAndDiskUnchanged(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}
	c.configurationInitialized = true
	c.noiseClass = "QUIET"
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	command, request := decodeProposalTest(t, c, "PATCH", "SETTINGS", "  noise_class: URBAN\n")
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"stale", "unavailable", "persistence", "protected", "padded_grid", "padded_noise"} {
		t.Run(failure, func(t *testing.T) {
			proposal := request
			s.saveConfigurationFn = nil
			c.recordProtected = false
			s.dedupeSlowEnabled = true
			switch failure {
			case "stale":
				proposal.IfRevision = "old-session-0"
			case "unavailable":
				s.dedupeSlowEnabled = false
				_, proposal = decodeProposalTest(t, c, "PATCH", "SETTINGS", "  dedupe_policy: SLOW\n")
			case "persistence":
				s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error { return os.ErrPermission }
			case "protected":
				c.recordProtected = true
			case "padded_grid":
				_, proposal = decodeProposalTest(t, c, "PATCH", "SETTINGS", "  grid: \" FN31 \"\n")
			case "padded_noise":
				_, proposal = decodeProposalTest(t, c, "PATCH", "SETTINGS", "  noise_class: \" URBAN \"\n")
			}
			response := s.applyMachineRequest(c, command, proposal)
			if !strings.Contains(response, "error:") || strings.Contains(response, "persisted: true") {
				t.Fatalf("failed write response=%q", response)
			}
			if c.configuredSettings.NoiseClass != "QUIET" || c.noiseClass != "QUIET" || c.getDedupePolicy() == dedupePolicySlow {
				t.Fatal("failed write changed live preferences")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed write changed literal durable bytes")
			}
			afterRevision, err := c.configurationRevisionToken()
			if err != nil || afterRevision != revision {
				t.Fatal("failed write advanced revision")
			}
		})
	}
}

func TestMachineUnchangedPUTRepairsDurabilityWithoutRuntimeReset(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST", SolarSummaryMinutes: 15}
	c.configurationInitialized = true
	c.noiseClass = "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	c.setSolarSummaryMinutes(15, time.Now())
	c.filter.Bands["20m"] = false
	priorBands := c.filter.Bands
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error { return os.ErrPermission }
	response, _ := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
	if !strings.Contains(response, "warning: failed to persist") {
		t.Fatalf("setter=%q", response)
	}
	s.saveConfigurationFn = nil
	c.setDiagMode(diagModePath)
	deadline := time.Now().Add(time.Minute)
	c.startReadPause(time.Now(), time.Minute)
	c.solarNextSummaryAt = deadline
	beforeTick, beforePause, beforeDiag := c.solarNextSummaryAt, c.readPauseUntilUnixNano.Load(), c.getDiagMode()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command, request := decodeProposalTest(t, c, "PUT", "SETTINGS", "  dialect: go\n  grid: \"\"\n  noise_class: URBAN\n  dedupe_policy: FAST\n  path_min_observation_count: 0\n  solar_summary_minutes: 15\n")
	response = s.applyMachineRequest(c, command, request)
	if !strings.Contains(response, "persisted: true") || strings.Contains(response, "error:") {
		t.Fatalf("unchanged PUT=%q", response)
	}
	afterRevision, err := c.configurationRevisionToken()
	if err != nil || afterRevision != revision {
		t.Fatal("unchanged PUT advanced revision")
	}
	if c.solarNextSummaryAt != beforeTick || c.readPauseUntilUnixNano.Load() != beforePause || c.getDiagMode() != beforeDiag {
		t.Fatal("unchanged PUT reset temporary runtime state")
	}
	priorBands["20m"] = true
	if !c.filter.Bands["20m"] {
		t.Fatal("unchanged PUT replaced a live filter collection to repair persistence")
	}
	priorBands["20m"] = false
	data, err := os.ReadFile(filepath.Join(filter.UserDataDir, c.callsign+".yaml"))
	if err != nil || !strings.Contains(string(data), "noise_class: URBAN") {
		t.Fatalf("durable repair=%q, %v", data, err)
	}
	next := configurationTestClient(s, c.callsign)
	_, err = s.restoreAndRegisterClient(next, time.Now().UTC(), time.Now().Add(time.Minute))
	if err != nil || next.configuredSettings.NoiseClass != "URBAN" {
		t.Fatalf("repair after reconnect=%+v, %v", next.configuredSettings, err)
	}
	newRevision, err := next.configurationRevisionToken()
	if err != nil || newRevision == revision {
		t.Fatal("reconnect retained old session revision")
	}
}

func TestMachineRevisionOrderAndReversal(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.DXCallsigns = []string{"W1*", "K1*"}
	c.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: &filter.SavedPreset{Filter: *filter.NewFilter(), Dialect: "go", DedupePolicy: "FAST"}}
	c.presetReference.Baseline.DXCallsigns = []string{"W1*", "K1*"}
	c.setDedupePolicy(dedupePolicyFast)
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command, request := decodeProposalTest(t, c, "PATCH", "FILTER", "  dx_callsigns: [\"K1*\", \"W1*\"]\n")
	response := s.applyMachineRequest(c, command, request)
	if strings.Contains(response, "error:") {
		t.Fatalf("order-only write=%q", response)
	}
	after, err := c.configurationRevisionToken()
	if err != nil || after != revision || strings.Join(c.filter.DXCallsigns, ",") != "K1*,W1*" {
		t.Fatal("order-only write lost supplied order or advanced revision")
	}
	command, request = decodeProposalTest(t, c, "PATCH", "FILTER", "  dx_callsigns: [\"K1*\"]\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	modifiedRevision, err := c.configurationRevisionToken()
	if err != nil || modifiedRevision == revision {
		t.Fatal("semantic edit did not advance revision")
	}
	command, request = decodeProposalTest(t, c, "PATCH", "FILTER", "  dx_callsigns: [\"K1*\", \"W1*\"]\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	revertedRevision, err := c.configurationRevisionToken()
	if err != nil || revertedRevision == revision || revertedRevision == modifiedRevision {
		t.Fatal("reversal revived an old revision")
	}
	current, err := c.captureConfiguration(maxYAMLBytes)
	if err != nil || !current.Equal(filter.ConfigurationFromPreset(c.presetReference.Baseline)) {
		t.Fatal("reversal did not match preserved baseline")
	}
}

func TestMachineOrderOnlyPATCHPublishesAndPersistsEveryPatternList(t *testing.T) {
	for _, tc := range []struct {
		field   string
		diskKey string
	}{
		{field: "dx_callsigns", diskKey: "callsigns"},
		{field: "block_dx_callsigns", diskKey: "block_callsigns"},
		{field: "de_callsigns", diskKey: "decallsigns"},
		{field: "block_de_callsigns", diskKey: "block_decallsigns"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			var patterns *[]string
			switch tc.field {
			case "dx_callsigns":
				patterns = &c.filter.DXCallsigns
			case "block_dx_callsigns":
				patterns = &c.filter.BlockDXCallsigns
			case "de_callsigns":
				patterns = &c.filter.DECallsigns
			case "block_de_callsigns":
				patterns = &c.filter.BlockDECallsigns
			}
			*patterns = []string{"W1*", "K1*"}
			revision, err := c.configurationRevisionToken()
			if err != nil {
				t.Fatal(err)
			}
			command, request := decodeProposalTest(t, c, "PATCH", "FILTER", "  "+tc.field+": [\"K1*\", \"W1*\"]\n")
			if response := s.applyMachineRequest(c, command, request); !strings.Contains(response, "persisted: true") || strings.Contains(response, "error:") {
				t.Fatal(response)
			}
			if strings.Join(*patterns, ",") != "K1*,W1*" {
				t.Fatal("order-only write skipped runtime publication")
			}
			if after, err := c.configurationRevisionToken(); err != nil || after != revision {
				t.Fatal("order-only write changed the semantic revision")
			}
			var record map[string]any
			data := presetDiskBytes(t, filepath.Join(filter.UserDataDir, c.callsign+".yaml"))
			if err := yaml.Unmarshal(data, &record); err != nil || !reflect.DeepEqual(record[tc.diskKey], []any{"K1*", "W1*"}) {
				t.Fatalf("disk lost supplied order: %v, %v", record[tc.diskKey], err)
			}
		})
	}
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
	if len(s.yamlPreparation) != 0 {
		t.Fatal("preparation permit was retained")
	}
	// Success and decode error must both release the permit before returning.
	for _, body := range []string{"[broken", "schema_version: 1\nrequest_id: edit-1\nif_revision: rev-1\nconfiguration: {noise_class: URBAN}\n"} {
		_, _ = s.prepareMachineRequest(configurationTestClient(s, "W1ABC-1"), []byte(body), machineCommand{Verb: "PATCH", Resource: "SETTINGS"})
		if len(s.yamlPreparation) != 0 {
			t.Fatal("decoder retained its preparation permit")
		}
	}
}

// These field names and values are a literal client contract, independent of
// the implementation's serializer and configuration normalizer.
func completeValidationConfigurationTest() string {
	text := "filters:\n"
	for _, name := range []string{"bands", "modes", "sources", "events", "confidence", "path_classes", "dx_continents", "de_continents", "dx_zones", "de_zones", "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc"} {
		text += "  " + name + ": {allow_all: false, block_all: false, allow: {}, block: {}}\n"
	}
	return text + `  dx_callsigns: ["K1*", "W1*"]
  block_dx_callsigns: []
  de_callsigns: []
  block_de_callsigns: []
  include_beacons: DEFAULT
  allow_wwv: DEFAULT
  allow_wcy: DEFAULT
  allow_announce: DEFAULT
  allow_self: DEFAULT
  allow_toxic: DEFAULT
  nearby_enabled: false
settings:
  dialect: go
  grid: ""
  noise_class: QUIET
  dedupe_policy: FAST
  path_min_observation_count: 0
  solar_summary_minutes: 0
`
}

func TestMachineVALIDATEHasNoLiveDurableOrSessionEffects(t *testing.T) {
	s := presetTestServer(t)
	c, before, _ := publishTestClient(t, s)
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	durable, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	tick, snapshot, pointer, diag := c.solarNextSummaryAt, c.filter.NearbySnapshot, c.filter, c.getDiagMode()
	pause := readbackPauseTestState{until: c.readPauseUntilUnixNano.Load(), cutoff: c.readPauseDiscardBefore.Load(),
		count: c.readPauseSuppressed.Load(), epoch: c.readPauseEpoch, pending: c.readPausePending.Load(), closed: c.readPauseClosed}
	persistCalls := 0
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		persistCalls++
		return os.ErrPermission
	}
	for _, behavior := range []string{"valid", "protected", "unavailable", "oversized"} {
		t.Run(behavior, func(t *testing.T) {
			c.recordProtected = behavior == "protected"
			s.dedupeSlowEnabled = false
			configuration := completeValidationConfigurationTest()
			switch behavior {
			case "unavailable":
				configuration = strings.Replace(configuration, "dedupe_policy: FAST", "dedupe_policy: SLOW", 1)
			case "oversized":
				configuration = strings.Replace(configuration, `dx_callsigns: ["K1*", "W1*"]`, "dx_callsigns: ["+strings.Repeat("X", 63000)+"]", 1)
			}
			command := machineCommand{Verb: "VALIDATE", Resource: "CONFIG"}
			request, err := decodeMachineRequest(machineRequestFixture(configuration, false), command)
			if err != nil {
				t.Fatal(err)
			}
			response := s.applyMachineRequest(c, command, request)
			if behavior == "valid" || behavior == "protected" {
				if !strings.Contains(response, "valid: true\r\n  applied: false\r\n  persisted: false") || strings.Contains(response, "error:") {
					t.Fatalf("validation result=%q", response)
				}
			} else if !strings.Contains(response, "error:") {
				t.Fatalf("invalid proposal admitted: %q", response)
			}
			if behavior == "oversized" && !strings.Contains(response, "code: response_too_large") {
				t.Fatalf("oversized validation did not enforce the final CONFIG budget: %q", response)
			}
			after, err := c.captureConfiguration(maxYAMLBytes)
			if err != nil || !reflect.DeepEqual(before, after) || c.filter != pointer || c.filter.NearbySnapshot != snapshot || c.solarNextSummaryAt != tick || c.getDiagMode() != diag {
				t.Fatal("VALIDATE changed live configuration or temporary session state")
			}
			assertReadbackPauseState(t, c, pause)
			currentRevision, err := c.configurationRevisionToken()
			if err != nil || currentRevision != revision || persistCalls != 0 {
				t.Fatal("VALIDATE changed revision or attempted persistence")
			}
			data, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, durable) {
				t.Fatal("VALIDATE changed literal durable bytes")
			}
		})
	}
}

func TestMachinePATCHCanReduceOversizedHumanConfiguration(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	oversizedPattern := strings.Repeat("X", 70000)
	c.filter.DXCallsigns = []string{oversizedPattern}
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}
	c.configurationInitialized = true
	c.noiseClass = "QUIET"
	c.setDedupePolicy(dedupePolicyFast)
	baseline, err := c.captureConfiguration(filter.MaxPresetBytes)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := baseline.Preset()
	if err != nil {
		t.Fatal(err)
	}
	c.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: saved}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command, request := decodeProposalTest(t, c, "PATCH", "SETTINGS", "  noise_class: URBAN\n")
	response := s.applyMachineRequest(c, command, request)
	if !strings.Contains(response, "code: response_too_large") || c.configuredSettings.NoiseClass != "QUIET" {
		t.Fatalf("oversized unrelated update=%q", response)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, data) {
		t.Fatal("rejected unrelated PATCH changed durable bytes")
	}
	currentRevision, err := c.configurationRevisionToken()
	if err != nil || currentRevision != revision {
		t.Fatal("rejected oversized result advanced revision")
	}
	command, request = decodeProposalTest(t, c, "PATCH", "FILTER", "  dx_callsigns: [\"K1*\", \"W1*\"]\n")
	response = s.applyMachineRequest(c, command, request)
	if strings.Contains(response, "error:") || !strings.Contains(response, "persisted: true") || strings.Join(c.filter.DXCallsigns, ",") != "K1*,W1*" || c.presetReference.Name != "CONTEST" {
		t.Fatalf("reducing PATCH=%q", response)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect disk fields directly, avoiding the production record decoder.
	var disk map[string]any
	if err := yaml.Unmarshal(data, &disk); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disk["callsigns"], []any{"K1*", "W1*"}) {
		t.Fatalf("disk pattern order=%v", disk["callsigns"])
	}
	preset, ok := disk["preset"].(map[string]any)
	if !ok || preset["name"] != "CONTEST" {
		t.Fatal("reducing PATCH lost durable preset association")
	}
	baselineDisk, ok := preset["baseline"].(map[string]any)
	if !ok || !reflect.DeepEqual(baselineDisk["callsigns"], []any{oversizedPattern}) {
		t.Fatal("reducing PATCH overwrote the saved baseline")
	}
	currentRevision, err = c.configurationRevisionToken()
	if err != nil || currentRevision == revision {
		t.Fatal("reducing PATCH did not advance configuration revision")
	}
	readback, err := s.renderYAMLReadback(c, "CONFIG", strings.Repeat("1", 32), currentRevision)
	if err != nil || len(readback) > 65536 || !strings.HasSuffix(readback, "...\r\n") || !strings.Contains(readback, "modified: true") {
		t.Fatalf("new configuration lacks complete bounded readback: length=%d error=%v", len(readback), err)
	}
}
