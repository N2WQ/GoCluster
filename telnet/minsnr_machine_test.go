package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

const minSNRCompleteStateFixture = "dx_states: {allow_all: true, block_all: false, allow: {}, block: {}}\nde_states: {allow_all: true, block_all: false, allow: {}, block: {}}\n"

func minSNRMachineFixture(content string, version int, revision bool) []byte {
	return bytes.Replace(machineRequestFixture(content, revision), []byte("schema_version: 1"), []byte(fmt.Sprintf("schema_version: %d", version)), 1)
}

func minSNRValidCompleteFilter() string {
	return strings.ReplaceAll(machineCompleteFilterFixture, "K1?", "K1*") + minSNRCompleteStateFixture
}

func TestMinSNRMachineV3(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	for _, header := range []string{"GET YAML FILTER SCHEMA 3", "get yaml config schema 3 id Snr-Ab1", "GET YAML CAPABILITIES SCHEMA 3", "GET YAML SETTINGS SCHEMA 3"} {
		command, handled, err := parseMachineHeader(header)
		if err != nil || !handled || command.SchemaVersion != 3 {
			t.Fatalf("schema 3 header: %q %v", header, err)
		}
		response := s.prepareMachineReadback(c, command)
		if !strings.Contains(response, "schema_version: 3\r\n") || strings.Contains(response, "error:") {
			t.Fatal(response)
		}
	}
	command, request := stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {CW: 0, FT8: -10, FT4: 12}\n")
	if response := s.applyMachineRequest(c, command, request); !strings.Contains(response, "schema_version: 3\r\n") || !strings.Contains(response, "persisted: true") {
		t.Fatal(response)
	}
	want := map[string]int{"CW": 0, "FT8": -10, "FT4": 12}
	readback, err := s.renderYAMLReadbackVersion(c, "FILTER", "snr-1", "session-0", 3)
	if err != nil || !reflect.DeepEqual(stateReadbackFilter(t, readback).MinSNR, want) || !reflect.DeepEqual(c.filter.MinSNR, want) {
		t.Fatalf("exact signed map readback: %v %s", err, readback)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !reflect.DeepEqual(record.MinSNR, want) {
		t.Fatalf("signed map persisted: %v", err)
	}
	// A supplied collection replaces the whole map, while an omitted collection
	// remains exact. These expectations are independent of request.apply.
	command, request = stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {CW: -3}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"CW": -3}) {
		t.Fatal(response)
	}
	command, request = stateProposal(t, c, 3, "PATCH", "SETTINGS", "  noise_class: URBAN\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"CW": -3}) {
		t.Fatal(response)
	}
	command, request = stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || len(c.filter.MinSNR) != 0 {
		t.Fatal(response)
	}
	for _, verb := range []string{"PUT", "VALIDATE"} {
		fixture := minSNRValidCompleteFilter()
		resource := "FILTER"
		if verb == "VALIDATE" {
			fixture = "filters:\n" + indentMachineFixture(fixture) + "settings:\n" + indentMachineFixture("dialect: go\ngrid: \"\"\nnoise_class: QUIET\ndedupe_policy: FAST\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n")
			resource = "CONFIG"
		}
		if _, err := decodeMachineRequest(minSNRMachineFixture(fixture, 3, true), machineCommand{Verb: verb, Resource: resource}); err == nil {
			t.Fatalf("schema 3 %s accepted omitted min_snr", verb)
		}
	}
	for _, resource := range []string{"FILTER", "CONFIG"} {
		fixture := minSNRValidCompleteFilter() + "min_snr: {CW: 0, FT8: -10}\n"
		if resource == "CONFIG" {
			fixture = "filters:\n" + indentMachineFixture(fixture) + "settings:\n" + indentMachineFixture("dialect: go\ngrid: \"\"\nnoise_class: QUIET\ndedupe_policy: FAST\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n")
		}
		command, request := stateProposal(t, c, 3, "PUT", resource, indentMachineFixture(fixture))
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"CW": 0, "FT8": -10}) {
			t.Fatalf("complete schema 3 %s replacement failed: %s", resource, response)
		}
	}
	caps, err := s.renderYAMLReadbackVersion(c, "CAPABILITIES", "caps-3", "session-0", 3)
	if err != nil {
		t.Fatal(err)
	}
	var capability struct {
		Configuration struct {
			FilterFields     []string `yaml:"filter_fields"`
			FilterCategories []string `yaml:"filter_categories"`
			Choices          struct {
				MinSNR readbackMinSNRChoices `yaml:"min_snr"`
			} `yaml:"choices"`
		} `yaml:"configuration"`
	}
	if err := yaml.Unmarshal([]byte(caps), &capability); err != nil || len(capability.Configuration.FilterFields) != 28 || capability.Configuration.FilterFields[27] != "min_snr" || capability.Configuration.FilterCategories[len(capability.Configuration.FilterCategories)-1] != "MINSNR" {
		t.Fatalf("schema 3 capability vocabulary: %v %s", err, caps)
	}
	limits := capability.Configuration.Choices.MinSNR
	if limits.Minimum != math.MinInt || limits.Maximum != math.MaxInt || limits.MaxEntries != 128 || limits.MaxKeyBytes != 65536 {
		t.Fatalf("schema 3 capability limits drifted: %+v", limits)
	}

}

func TestMinSNRMachineOldProjections(t *testing.T) {
	cfg := filter.Configuration{Filters: filter.FilterConfiguration{MinSNR: map[string]int{"FT8": -10, "DORMANT": 0}}}
	for _, version := range []int{1, 2} {
		_, selected, err := readbackResourceConfigurationVersion(cfg, "FILTER", version)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := encodeBoundedYAML(selected)
		if err != nil {
			t.Fatal(err)
		}
		// Schema 2's states appear before continents, preserving its literal
		// historical wire order rather than using the parser field inventory.
		names := "bands modes sources events confidence path_classes "
		if version == 2 {
			names += "dx_states de_states "
		}
		names += "dx_continents de_continents dx_zones de_zones dx_grid2 de_grid2 dx_dxcc de_dxcc"
		want := "---\r\n"
		for _, name := range strings.Fields(names) {
			want += name + ":\r\n  allow_all: false\r\n  block_all: false\r\n  allow: {}\r\n  block: {}\r\n"
		}
		want += "dx_callsigns: []\r\nblock_dx_callsigns: []\r\nde_callsigns: []\r\nblock_de_callsigns: []\r\ninclude_beacons: DEFAULT\r\nallow_wwv: DEFAULT\r\nallow_wcy: DEFAULT\r\nallow_announce: DEFAULT\r\nallow_self: DEFAULT\r\nallow_toxic: DEFAULT\r\nnearby_enabled: false\r\n...\r\n"
		if actual != want {
			t.Fatalf("schema %d literal projection drifted:\n%s", version, actual)
		}
		for _, resource := range []string{"FILTER", "CONFIG"} {
			_, data, err := readbackResourceConfigurationVersion(cfg, resource, version)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encodeBoundedYAML(data)
			if err != nil || strings.Contains(wire, "min_snr") {
				t.Fatalf("schema %d leaked hidden field: %v", version, err)
			}
		}
		s := presetTestServer(t)
		c := configurationTestClient(s, "W1ABC-1")
		caps, err := s.renderYAMLReadbackVersion(c, "CAPABILITIES", "caps-1", "session-0", version)
		if err != nil || strings.Contains(caps, "min_snr") || strings.Contains(caps, "MINSNR") {
			t.Fatalf("schema %d leaked capabilities: %v %s", version, err, caps)
		}
	}
}

func TestMinSNRMachinePreservation(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = map[string]int{"FT8": -10, "DORMANT": 0, "FT-8": 17}
	for _, version := range []int{1, 2} {
		fixture := strings.ReplaceAll(machineCompleteFilterFixture, "K1?", "K1*")
		if version == 2 {
			fixture += minSNRCompleteStateFixture
		}
		command, request := stateProposal(t, c, version, "PUT", "FILTER", indentMachineFixture(fixture))
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"FT8": -10, "DORMANT": 0, "FT-8": 17}) {
			t.Fatal(response)
		}
		if _, err := decodeMachineRequest(minSNRMachineFixture("min_snr: {FT8: -10}\n", version, true), machineCommand{Verb: "PATCH", Resource: "FILTER"}); err == nil {
			t.Fatalf("schema %d accepted hidden field", version)
		}
	}
	command, stale := stateProposal(t, c, 1, "PATCH", "SETTINGS", "  noise_class: URBAN\n")
	newFilterCommandEngine().Handle(c, "PASS MINSNR CW 4")
	if response := s.applyMachineRequest(c, command, stale); !strings.Contains(response, "code: revision_conflict") {
		t.Fatalf("hidden minimum edit escaped revision: %s", response)
	}
	// Exact old keys can remain dormant even when their spelling is now an alias.
	command, request := stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {DORMANT: -2, FT-8: 18}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"DORMANT": -2, "FT-8": 18}) {
		t.Fatal(response)
	}
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	for _, newlyUnavailable := range []string{"UNAVAILABLE", "FT-4"} {
		command, request := stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {"+newlyUnavailable+": 0}\n")
		if response := s.applyMachineRequest(c, command, request); !strings.Contains(response, "error:") || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
			t.Fatalf("candidate map proved its own retention: %s", response)
		}
	}
	command, request = stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {FT8: -3}\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !reflect.DeepEqual(c.filter.MinSNR, map[string]int{"FT8": -3}) {
		t.Fatalf("could not remove retained dormant keys: %s", response)
	}
}

func TestMinSNRMachinePersistenceFailure(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = map[string]int{"FT8": -10, "DORMANT": 0}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk := presetDiskBytes(t, path)
	revision, _ := c.configurationRevisionToken()
	persistCalls := 0
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		persistCalls++
		return os.ErrPermission
	}
	command, request := stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {CW: 0, FT8: -1}\n")
	if response := s.applyMachineRequest(c, command, request); !strings.Contains(response, "code: persistence_failed") || !strings.Contains(response, "schema_version: 3\r\n") {
		t.Fatal(response)
	}
	fixture := "filters:\n" + indentMachineFixture(minSNRValidCompleteFilter()+"min_snr: {CW: 0, DORMANT: -3}\n") + "settings:\n" + indentMachineFixture("dialect: go\ngrid: \"\"\nnoise_class: QUIET\ndedupe_policy: FAST\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n")
	command, request = stateProposal(t, c, 3, "VALIDATE", "CONFIG", indentMachineFixture(fixture))
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || !strings.Contains(response, "applied: false") || !strings.Contains(response, "persisted: false") {
		t.Fatal(response)
	}
	afterRevision, _ := c.configurationRevisionToken()
	if persistCalls != 1 || revision != afterRevision || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || !bytes.Equal(disk, presetDiskBytes(t, path)) {
		t.Fatal("failed write or VALIDATE changed live, revision, or durable state")
	}
}

func TestMinSNRMachineRawBoundsAndInvalidValues(t *testing.T) {
	for _, invalid := range []string{"null", "[]", "{FT8: null}", "{FT8: true}", "{FT8: \"-10\"}", "{FT8: 1.5}", "{FT8: 9223372036854775808}", "{FT8: -9223372036854775809}", "{FT8: 0, FT8: -1}", "{ft8: -10}", "{FT 8: -10}", "{1: -10}"} {
		request, err := decodeMachineRequest(minSNRMachineFixture("min_snr: "+invalid+"\n", 3, true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
		if err == nil || request.SchemaVersion != 3 || len(err.Error()) > 256 {
			t.Fatalf("invalid map admitted or wrong error version: %s: %v", invalid, err)
		}
	}
	request, err := decodeMachineRequest(minSNRMachineFixture(fmt.Sprintf("min_snr: {CW: %d, FT8: %d}\n", math.MinInt, math.MaxInt), 3, true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil || request.Configuration.Filters.MinSNR["CW"] != math.MinInt || request.Configuration.Filters.MinSNR["FT8"] != math.MaxInt {
		t.Fatalf("integer limits rejected: %v", err)
	}
	// Construct raw nodes independently to reach the aggregate key-byte boundary
	// beyond the envelope's own smaller usable payload size. Bounds fail before
	// any typed map is returned, including when values themselves are invalid.
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	add := func(key, value string) {
		node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: value})
	}
	for i := range 128 {
		add("MODE"+strconv.Itoa(i), "0")
	}
	if got, err := machineMinSNRMap(node, "configuration.min_snr"); err != nil || len(got) != 128 {
		t.Fatalf("entry cap rejected: %v", err)
	}
	add("EXCESS", "not-an-integer")
	if got, err := machineMinSNRMap(node, "configuration.min_snr"); err == nil || got != nil || !strings.Contains(err.Error(), "entry limit") {
		t.Fatal("entry limit was not checked before typed construction/value decode")
	}
	node.Content = nil
	add(strings.Repeat("A", 65536), "0")
	if got, err := machineMinSNRMap(node, "configuration.min_snr"); err != nil || len(got) != 1 {
		t.Fatalf("aggregate key cap rejected: %v", err)
	}
	add("B", "not-an-integer")
	if got, err := machineMinSNRMap(node, "configuration.min_snr"); err == nil || got != nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatal("key bytes were not checked before typed construction/value decode")
	}
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = map[string]int{strings.Repeat("A", 65536): 0}
	cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	if err := machineConfigurationFits(cfg, 2); err != nil {
		t.Fatalf("hidden finite map reduced old visible admission: %v", err)
	}
	if err := s.configurationReadbackFitsVersion(c, cfg, c, 3); !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("schema 3 did not enforce full encoded result: %v", err)
	}
	c.filter.MinSNR["B"] = 0
	_, omitted := stateProposal(t, c, 2, "PATCH", "SETTINGS", "  noise_class: QUIET\n")
	if _, _, err := c.prepareMachineCandidate(omitted); err == nil {
		t.Fatal("old schema cloned an oversized hidden threshold map")
	}
	command, reducing := stateProposal(t, c, 3, "PATCH", "FILTER", "  min_snr: {}\n")
	if response := s.applyMachineRequest(c, command, reducing); strings.Contains(response, "error:") || len(c.filter.MinSNR) != 0 {
		t.Fatalf("could not repair oversized human map with replacement: %s", response)
	}
}

func TestMinSNRMachineV3FramedSessions(t *testing.T) {
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
			write("GET YAML FILTER SCHEMA 3 ID Snr-Ab1\r\n")
			response := readMachineTestFrame(t, reader)
			if !strings.Contains(response, "schema_version: 3\r\n") || !strings.Contains(response, "min_snr: {}") || !strings.Contains(response, "request_id: Snr-Ab1") {
				t.Fatal(response)
			}
			revision := machineTestRevision(t, response)
			write(fmt.Sprintf("PATCH YAML FILTER\r\n---\r\nschema_version: 3\r\nrequest_id: snr-edit\r\nif_revision: %s\r\nconfiguration: {min_snr: {CW: 0, FT8: -10}}\r\n...\r\n", revision))
			response = readMachineTestFrame(t, reader)
			if !strings.Contains(response, "schema_version: 3\r\n") || !strings.Contains(response, "persisted: true") {
				t.Fatal(response)
			}
			revision = machineTestRevision(t, response)
			write(fmt.Sprintf("PATCH YAML FILTER\r\n---\r\nschema_version: 3\r\nrequest_id: snr-bad\r\nif_revision: %s\r\nconfiguration: {min_snr: {FT8: null}}\r\n...\r\n", revision))
			if response = readMachineTestFrame(t, reader); !strings.Contains(response, "schema_version: 3\r\n") || !strings.Contains(response, "code: invalid_document") {
				t.Fatal(response)
			}
			write("GET YAML FILTER ID old-1\r\n")
			response = readMachineTestFrame(t, reader)
			if strings.Contains(response, "min_snr") || machineTestRevision(t, response) != revision || !strings.Contains(response, "schema_version: 1\r\n") {
				t.Fatal("schema 3 negotiated session state or invalid upload changed revision")
			}
			write("GET YAML FILTER SCHEMA 3 ID new-1\r\n")
			response = readMachineTestFrame(t, reader)
			if machineTestRevision(t, response) != revision || !reflect.DeepEqual(stateReadbackFilter(t, response).MinSNR, map[string]int{"CW": 0, "FT8": -10}) {
				t.Fatal(response)
			}
		})
	}
}
