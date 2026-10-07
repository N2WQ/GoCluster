package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/uls"
	"gopkg.in/yaml.v3"
)

func stateProposal(t *testing.T, c *Client, version int, verb, resource, content string) (machineCommand, machineRequest) {
	t.Helper()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	command := machineCommand{Verb: verb, Resource: resource}
	body := fmt.Sprintf("schema_version: %d\nrequest_id: states-1\nif_revision: %s\nconfiguration:\n%s", version, revision, content)
	request, err := decodeMachineRequest([]byte(body), command)
	if err != nil {
		t.Fatalf("literal proposal: %v", err)
	}
	return command, request
}

func installExactStateFixture(c *Client) {
	c.filter.DXStates = map[string]bool{"CA": true, "TX": false}
	c.filter.BlockDXStates = map[string]bool{"NY": false}
	c.filter.AllDXStates = false
	c.filter.DEStates = map[string]bool{}
	c.filter.BlockDEStates = map[string]bool{"AA": false}
	c.filter.BlockAllDEStates, c.filter.AllDEStates = true, false
}

func TestStateMachineV1ProjectionAndPreservation(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	installExactStateFixture(c)
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	for _, resource := range []string{"FILTER", "CONFIG"} {
		got, err := s.renderYAMLReadback(c, resource, "states-1", "session-0")
		if err != nil || strings.Contains(got, "dx_states:") || strings.Contains(got, "de_states:") || !strings.Contains(got, "schema_version: 1\r\n") {
			t.Fatalf("v1 projection: %v %s", err, got)
		}
	}
	// An old complete FILTER document explicitly lists all original fields.
	fixture := strings.ReplaceAll(machineCompleteFilterFixture, "K1?", "K1*")
	fixture = "  " + strings.ReplaceAll(strings.TrimSuffix(fixture, "\n"), "\n", "\n  ") + "\n"
	command, request := stateProposal(t, c, 1, "PUT", "FILTER", fixture)
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	command, request = stateProposal(t, c, 1, "PATCH", "FILTER", "  bands: {allow_all: true, allow: {}}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	after := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	if !reflect.DeepEqual(before.Filters.DXStates, after.Filters.DXStates) || !reflect.DeepEqual(before.Filters.DEStates, after.Filters.DEStates) {
		t.Fatal("v1 write reset hidden exact state")
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !reflect.DeepEqual(record.DXStates, before.Filters.DXStates.Allow) || !record.BlockAllDEStates || record.BlockDXStates["NY"] {
		t.Fatalf("v1 did not persist full state: %v", err)
	}
	command, stale := stateProposal(t, c, 1, "PATCH", "SETTINGS", "  noise_class: URBAN\n")
	newFilterCommandEngine().Handle(c, "PASS DXSTATE TX")
	response := s.applyMachineRequest(c, command, stale)
	if !strings.Contains(response, "code: revision_conflict") || !strings.Contains(response, "schema_version: 1\r\n") {
		t.Fatalf("hidden edit escaped revision: %s", response)
	}
	_, err = decodeMachineRequest([]byte("schema_version: 1\nrequest_id: states-1\nif_revision: rev-1\nconfiguration: {dx_states: {allow: {CA: true}}}\n"), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err == nil {
		t.Fatal("v1 accepted new state field")
	}
}

func TestStateMachineV2Contract(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	for _, line := range []string{"GET YAML FILTER SCHEMA 2", "get yaml config schema 2 id States-Ab1", "GET YAML CAPABILITIES SCHEMA 2"} {
		command, handled, err := parseMachineHeader(line)
		if err != nil || !handled || command.SchemaVersion != 2 {
			t.Fatalf("header: %q %v", line, err)
		}
		response := s.prepareMachineReadback(c, command)
		if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "dx_states") {
			t.Fatal(response)
		}
	}
	for _, line := range []string{"GET YAML FILTER SCHEMA 1", "GET YAML FILTER SCHEMA 3", "GET YAML FILTER ID abc SCHEMA 2", "PUT YAML FILTER SCHEMA 2"} {
		if _, _, err := parseMachineHeader(line); err == nil {
			t.Fatalf("invalid header admitted: %q", line)
		}
	}
	for _, version := range []int{1, 2} {
		response, err := s.renderYAMLReadbackVersion(c, "CAPABILITIES", "caps-1", "session-0", version)
		if err != nil || !strings.Contains(response, "schema_versions:\r\n    - 1\r\n    - 2\r\n") {
			t.Fatalf("capabilities: %v %s", err, response)
		}
		if strings.Contains(response, "dx_states") != (version == 2) || strings.Contains(response, "states:") != (version == 2) {
			t.Fatal("capabilities leaked or omitted state")
		}
	}
	for _, field := range []string{"dx_states", "de_states"} {
		fixture := machineCompleteFilterFixture + "dx_states: {allow_all: true, block_all: false, allow: {}, block: {}}\nde_states: {allow_all: true, block_all: false, allow: {}, block: {}}\n"
		body := bytes.Replace(machineRequestFixture(fixture, true), []byte("schema_version: 1"), []byte("schema_version: 2"), 1)
		if _, err := decodeMachineRequest(body, machineCommand{Verb: "PUT", Resource: "FILTER"}); err != nil {
			t.Fatal(err)
		}
		line := field + ": {allow_all: true, block_all: false, allow: {}, block: {}}\n"
		body = bytes.Replace(body, []byte("  "+line), nil, 1)
		if _, err := decodeMachineRequest(body, machineCommand{Verb: "PUT", Resource: "FILTER"}); err == nil {
			t.Fatalf("v2 complete PUT omitted %s", field)
		}
	}
	command, request := stateProposal(t, c, 2, "PATCH", "FILTER", "  dx_states: {allow: {CA: true, TX: false}, allow_all: false}\n  de_states: {block: {NY: true}}\n")
	response := s.applyMachineRequest(c, command, request)
	if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "persisted: true") || !c.filter.DXStates["CA"] || !c.filter.BlockDEStates["NY"] {
		t.Fatal(response)
	}
	command, request = stateProposal(t, c, 2, "PATCH", "FILTER", "  dx_states: {allow: {TX: true}}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || len(c.filter.DXStates) != 1 || !c.filter.DXStates["TX"] || !c.filter.BlockDEStates["NY"] {
		t.Fatalf("v2 PATCH did not replace/preserve: %s", response)
	}
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	for _, key := range []string{"ca", "AB", "UNKNOWN"} {
		command, request = stateProposal(t, c, 2, "PATCH", "FILTER", "  dx_states: {allow: {"+key+": true}}\n")
		response := s.applyMachineRequest(c, command, request)
		if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "error:") || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
			t.Fatalf("invalid canonical state mutated: %s", response)
		}
	}
}

func TestStateMachineV2FramedSessions(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			_, conn, reader, done := startMachineTestSession(t, transport)
			defer closeHandshakeTranscriptSession(t, conn, done)
			write := func(message string) {
				t.Helper()
				if _, err := io.WriteString(conn, message); err != nil {
					t.Fatal(err)
				}
			}
			write("GET YAML FILTER SCHEMA 2 ID States-Ab1\r\n")
			response := readMachineTestFrame(t, reader)
			if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "request_id: States-Ab1\r\n") || !strings.Contains(response, "dx_states:") {
				t.Fatal(response)
			}
			revision := machineTestRevision(t, response)
			write(fmt.Sprintf("PATCH YAML FILTER\r\n---\r\nschema_version: 2\r\nrequest_id: states-edit\r\nif_revision: %s\r\nconfiguration:\r\n  dx_states: {allow_all: false, allow: {CA: true}}\r\n  de_states: {block: {NY: true}}\r\n...\r\n", revision))
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "persisted: true\r\n") {
				t.Fatal(response)
			}
			revision = machineTestRevision(t, response)
			for _, invalid := range []string{"dx_states: null", "dx_states: {allow: {CA: null}}", "dx_states: {allow: {CA: true, CA: false}}", "unknown_state_field: true"} {
				write(fmt.Sprintf("PATCH YAML FILTER\r\n---\r\nschema_version: 2\r\nrequest_id: states-bad\r\nif_revision: %s\r\nconfiguration: {%s}\r\n...\r\n", revision, invalid))
				response = readMachineTestFrame(t, reader)
				if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "code: invalid_document\r\n") {
					t.Fatal(response)
				}
			}
			write("GET YAML FILTER ID After-1\r\n")
			response = readMachineTestFrame(t, reader)
			if strings.Contains(response, "dx_states") || machineTestRevision(t, response) != revision || !strings.Contains(response, "schema_version: 1\r\n") {
				t.Fatal("schema 2 created session negotiation or invalid upload changed revision")
			}
			write("GET YAML FILTER SCHEMA 2 ID After-2\r\n")
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "CA: true") || !strings.Contains(response, "NY: true") || machineTestRevision(t, response) != revision {
				t.Fatal(response)
			}
			record, err := filter.LoadUserRecord("W1ABC-1")
			if err != nil || !record.DXStates["CA"] || !record.BlockDEStates["NY"] {
				t.Fatal("framed state write was not persisted")
			}
		})
	}
}

func TestStateMachineV2ValidateCompleteReadOnly(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	newFilterCommandEngine().Handle(c, "PASS DXSTATE TX")
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	revision, _ := c.configurationRevisionToken()
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fixture := strings.ReplaceAll(machineCompleteFilterFixture, "K1?", "K1*") + "dx_states: {allow_all: false, block_all: false, allow: {CA: true}, block: {}}\nde_states: {allow_all: true, block_all: false, allow: {}, block: {NY: true}}\n"
	settings := "dialect: go\ngrid: \"\"\nnoise_class: QUIET\ndedupe_policy: FAST\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n"
	configuration := "filters:\n" + indentMachineFixture(fixture) + "settings:\n" + indentMachineFixture(settings)
	body := bytes.Replace(machineRequestFixture(configuration, false), []byte("schema_version: 1"), []byte("schema_version: 2"), 1)
	command := machineCommand{Verb: "VALIDATE", Resource: "CONFIG"}
	request, err := decodeMachineRequest(body, command)
	if err != nil {
		t.Fatal(err)
	}
	response := s.applyMachineRequest(c, command, request)
	if !strings.Contains(response, "schema_version: 2\r\n") || !strings.Contains(response, "operation: VALIDATE\r\n") || strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	afterRevision, _ := c.configurationRevisionToken()
	afterDisk, err := os.ReadFile(path)
	if err != nil || revision != afterRevision || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || !bytes.Equal(disk, afterDisk) {
		t.Fatal("VALIDATE changed live, revision, or saved state")
	}
	for _, field := range []string{"dx_states", "de_states"} {
		lines := strings.Split(string(body), "\n")
		var incomplete []string
		for _, line := range lines {
			if !strings.HasPrefix(line, "    "+field+":") {
				incomplete = append(incomplete, line)
			}
		}
		if _, err := decodeMachineRequest([]byte(strings.Join(incomplete, "\n")), command); err == nil {
			t.Fatal("VALIDATE accepted incomplete state categories")
		}
	}
}

func TestStateExpansionPreservesLoginLicensePolicy(t *testing.T) {
	db := &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{"W1ABC-1": {Prefix: "K", ADIF: 291}, "KH6ABC-1": {Prefix: "KH6", ADIF: 110}}}
	checks := 0
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { return db }
		opts.USLicenseCheck = func(string) bool { checks++; return false }
	})
	if !s.validateLoginCallsign("KH6ABC-1").valid || checks != 0 {
		t.Fatal("spot territory coverage widened login gate")
	}
	if s.validateLoginCallsign("W1ABC-1").valid || checks != 1 {
		t.Fatal("enabled mainland login gate changed")
	}
	previous := uls.LicenseChecksEnabled()
	uls.SetLicenseChecksEnabled(false)
	t.Cleanup(func() { uls.SetLicenseChecksEnabled(previous) })
	s.usLicenseCheck = uls.IsLicensedUS
	if !s.validateLoginCallsign("W1ABC-1").valid {
		t.Fatal("disabled enforcement rejected login")
	}
}

func TestStateNearbyV1Restoration(t *testing.T) {
	s := presetTestServer(t)
	c, _, _ := publishTestClient(t, s)
	c.filter.DisableNearby()
	installExactStateFixture(c)
	if err := c.filter.EnableNearby(c.gridCell, c.gridCoarseCell); err != nil {
		t.Fatal(err)
	}
	snapshot := c.filter.NearbySnapshot
	for _, proposal := range []struct{ resource, content string }{
		{"FILTER", "  bands: {allow: {20m: true}}\n"},
		{"SETTINGS", "  noise_class: URBAN\n"},
	} {
		command, request := stateProposal(t, c, 1, "PATCH", proposal.resource, proposal.content)
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || c.filter.NearbySnapshot != snapshot {
			t.Fatalf("v1 discarded restoration: %s", response)
		}
	}
	// A legacy client can PUT back its complete visible FILTER while NEARBY
	// retains the independently asserted hidden state restoration snapshot.
	visible, err := yaml.Marshal(machineV1Filter(filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Filters))
	if err != nil {
		t.Fatal(err)
	}
	command, request := stateProposal(t, c, 1, "PUT", "FILTER", indentMachineFixture(string(visible)))
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || c.filter.NearbySnapshot != snapshot {
		t.Fatalf("v1 PUT discarded restoration: %s", response)
	}
	if response, _ := newFilterCommandEngine().Handle(c, "PASS DXSTATE TX"); response != nearbyLocationFilterWarning {
		t.Fatalf("state command escaped NEARBY: %s", response)
	}
	newFilterCommandEngine().Handle(c, "PASS NEARBY OFF")
	if !reflect.DeepEqual(c.filter.DXStates, map[string]bool{"CA": true, "TX": false}) || !c.filter.BlockAllDEStates {
		t.Fatal("NEARBY lost exact state restoration")
	}
}

func TestStateMachineBoundsAndStoredProtection(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	for _, code := range strings.Fields(stateCodesFixture) {
		c.filter.DXStates[code], c.filter.BlockDXStates[code] = false, false
		c.filter.DEStates[code], c.filter.BlockDEStates[code] = false, false
	}
	// Search the exact existing v1 admission edge, without deriving it from v2.
	low, high := 0, 65536
	for low < high {
		mid := (low + high + 1) / 2
		c.filter.DXCallsigns = []string{strings.Repeat("A", mid)}
		cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
		if s.configurationReadbackFitsVersion(c, cfg, c, 1) == nil {
			low = mid
		} else {
			high = mid - 1
		}
	}
	c.filter.DXCallsigns = []string{strings.Repeat("A", low)}
	cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	if s.configurationReadbackFitsVersion(c, cfg, c, 1) != nil || !errors.Is(s.configurationReadbackFitsVersion(c, cfg, c, 2), errReadbackTooLarge) {
		t.Fatal("v1 projection admission was reduced or v2 ignored state bytes")
	}
	if response, err := s.renderYAMLReadbackVersion(c, "CONFIG", "states-1", "session-0", 2); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("oversized v2 GET produced partial output")
	}
	command, request := stateProposal(t, c, 1, "PATCH", "SETTINGS", "  noise_class: QUIET\n")
	if _, _, err := c.prepareMachineCandidate(request); err != nil {
		t.Fatalf("v1 safe hidden state clone rejected: %v", err)
	}
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	v2command, v2request := stateProposal(t, c, 2, "PATCH", "SETTINGS", "  noise_class: QUIET\n")
	if response := s.applyMachineRequest(c, v2command, v2request); !strings.Contains(response, "code: response_too_large") || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
		t.Fatalf("oversized v2 write changed live preferences: %s", response)
	}
	c.filter.DXStates["ZZ"] = true
	if _, _, err := c.prepareMachineCandidate(request); err == nil {
		t.Fatal("hidden invalid state copied before admission")
	}
	_ = command
	protected := []byte("configuration_version: 3\ndxstates: {CA: true}\n")
	path := filepath.Join(filter.UserDataDir, "W2ABC-1.yaml")
	if err := os.WriteFile(path, protected, 0o600); err != nil {
		t.Fatal(err)
	}
	other := configurationTestClient(s, "W2ABC-1")
	if _, err := s.restoreAndRegisterClient(other, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !other.recordProtected {
		t.Fatal("future stored record not protected")
	}
	newFilterCommandEngine().Handle(other, "PASS DXSTATE CA")
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, protected) {
		t.Fatal("human state command rewrote future file")
	}
}

func TestStateMachineV1LiteralGolden(t *testing.T) {
	_, document, err := readbackResourceConfigurationVersion(filter.Configuration{}, "FILTER", 1)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := encodeBoundedYAML(document)
	if err != nil {
		t.Fatal(err)
	}
	// Literal original field order and names; no production field inventory is
	// used to derive the expected projection, including empty maps and toggles.
	names := strings.Fields("bands modes sources events confidence path_classes dx_continents de_continents dx_zones de_zones dx_grid2 de_grid2 dx_dxcc de_dxcc")
	want := "---\r\n"
	for _, name := range names {
		want += name + ":\r\n  allow_all: false\r\n  block_all: false\r\n  allow: {}\r\n  block: {}\r\n"
	}
	want += "dx_callsigns: []\r\nblock_dx_callsigns: []\r\nde_callsigns: []\r\nblock_de_callsigns: []\r\ninclude_beacons: DEFAULT\r\nallow_wwv: DEFAULT\r\nallow_wcy: DEFAULT\r\nallow_announce: DEFAULT\r\nallow_self: DEFAULT\r\nallow_toxic: DEFAULT\r\nnearby_enabled: false\r\n...\r\n"
	if actual != want {
		t.Fatalf("v1 literal field golden changed:\n%s", actual)
	}
	var decoded map[string]any
	if err := yaml.Unmarshal([]byte(actual), &decoded); err != nil || len(decoded) != 25 {
		t.Fatal("v1 field count drift")
	}
}
