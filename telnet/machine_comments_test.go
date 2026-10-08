package telnet

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestCommentMachineSchemaAdmission(t *testing.T) {
	valid := `comments: ["POTA", "pota", "up  5", " !:*? ", "POTA"]
block_comments: ["POTA", "QRT"]
`
	request, err := decodeMachineRequest(minSNRMachineFixture(valid, 4, true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"POTA", "pota", "up  5", " !:*? ", "POTA"}
	if !reflect.DeepEqual(request.Configuration.Filters.Comments, want) || !reflect.DeepEqual(request.Configuration.Filters.BlockComments, []string{"POTA", "QRT"}) {
		t.Fatal("exact phrases changed")
	}
	before := filter.Configuration{Filters: filter.FilterConfiguration{Comments: []string{"OLD"}, BlockComments: []string{"KEEP"}}}
	replaced, err := request.apply(before)
	if err != nil || !reflect.DeepEqual(replaced.Filters.Comments, want) || !reflect.DeepEqual(before.Filters.Comments, []string{"OLD"}) {
		t.Fatal("replacement changed caller")
	}
	for _, invalid := range []string{"comments: null", "comments: {}", "comments: [2]", `comments: [""]`, `comments: ["   "]`, `comments: ["\u00e9"]`, `comments: ["\t"]`, fmt.Sprintf("comments: [%q]", strings.Repeat("A", 65)), "comments: [" + strings.Repeat(`"same",`, 33) + "]"} {
		if _, err := decodeMachineRequest(minSNRMachineFixture(invalid+"\n", 4, true), machineCommand{Verb: "PATCH", Resource: "FILTER"}); err == nil {
			t.Fatalf("accepted invalid phrases: %s", invalid)
		}
	}
	full := minSNRValidCompleteFilter() + "min_snr: {}\n"
	for _, verb := range []string{"PUT", "VALIDATE"} {
		resource := "FILTER"
		fixture := full
		if verb == "VALIDATE" {
			resource = "CONFIG"
			fixture = "filters:\n" + indentMachineFixture(full) + "settings:\n" + indentMachineFixture(machineCompleteSettingsFixture)
		}
		if _, err := decodeMachineRequest(minSNRMachineFixture(fixture, 4, true), machineCommand{Verb: verb, Resource: resource}); err == nil {
			t.Fatalf("%s accepted missing comment lists", verb)
		}
	}
	for _, tail := range []string{"comments: []\n", "block_comments: []\n"} {
		if _, err := decodeMachineRequest(minSNRMachineFixture(full+tail, 4, true), machineCommand{Verb: "PUT", Resource: "FILTER"}); err == nil {
			t.Fatal("PUT accepted one missing list")
		}
	}
	if _, err := decodeMachineRequest(minSNRMachineFixture(full+"comments: []\nblock_comments: []\n", 4, true), machineCommand{Verb: "PUT", Resource: "FILTER"}); err != nil {
		t.Fatal(err)
	}
}

func TestCommentMachineTransactionsAndOldPreservation(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	original := []string{"POTA", "pota", "up  5", "POTA"}
	c.filter.Comments, c.filter.BlockComments = append([]string(nil), original...), []string{"QRT", "POTA"}
	for _, version := range []int{1, 2, 3} {
		fixture := strings.ReplaceAll(machineCompleteFilterFixture, "K1?", "K1*")
		if version >= 2 {
			fixture += minSNRCompleteStateFixture
		}
		if version >= 3 {
			fixture += "min_snr: {}\n"
		}
		command, request := stateProposal(t, c, version, "PUT", "FILTER", indentMachineFixture(fixture))
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
			t.Fatal(response)
		}
		command, request = stateProposal(t, c, version, "PATCH", "FILTER", "  include_beacons: false\n")
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
			t.Fatal(response)
		}
		command, request = stateProposal(t, c, version, "VALIDATE", "CONFIG", "  filters:\n"+indentMachineFixture(indentMachineFixture(fixture))+"  settings:\n"+indentMachineFixture(indentMachineFixture(strings.ReplaceAll(machineCompleteSettingsFixture, "GO", "go"))))
		if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
			t.Fatal(response)
		}
		if !reflect.DeepEqual(c.filter.Comments, original) || !reflect.DeepEqual(c.filter.BlockComments, []string{"QRT", "POTA"}) {
			t.Fatalf("schema %d destroyed hidden phrases", version)
		}
		if _, err := decodeMachineRequest(minSNRMachineFixture("comments: []\n", version, true), machineCommand{Verb: "PATCH", Resource: "FILTER"}); err == nil {
			t.Fatalf("schema %d accepts hidden comments", version)
		}
		wire, err := s.renderYAMLReadbackVersion(c, "CONFIG", "old", "session-0", version)
		if err != nil || strings.Contains(wire, "block_comments:") || strings.Contains(wire, "  comments:") {
			t.Fatalf("old schema leaked comments: %v %s", err, wire)
		}
		caps, err := s.renderYAMLReadbackVersion(c, "CAPABILITIES", "old", "session-0", version)
		if err != nil || strings.Contains(caps, "COMMENT") || strings.Contains(caps, "max_phrase") {
			t.Fatalf("old capabilities drifted: %v %s", err, caps)
		}
	}
	command, request := stateProposal(t, c, 4, "PATCH", "FILTER", "  comments: [\"up 5\", \"POTA\", \"up 5\"]\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") {
		t.Fatal(response)
	}
	if !reflect.DeepEqual(c.filter.Comments, []string{"up 5", "POTA", "up 5"}) || !reflect.DeepEqual(c.filter.BlockComments, []string{"QRT", "POTA"}) {
		t.Fatal("PATCH did not replace/preserve exact lists")
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !reflect.DeepEqual(record.Comments, c.filter.Comments) || !reflect.DeepEqual(record.BlockComments, c.filter.BlockComments) {
		t.Fatalf("persistence lost exact phrases: %v", err)
	}
	revision, _ := c.configurationRevisionToken()
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		return errors.New("write failed")
	}
	command, request = stateProposal(t, c, 4, "PATCH", "FILTER", "  comments: []\n  block_comments: []\n")
	if response := s.applyMachineRequest(c, command, request); !strings.Contains(response, "persistence_failed") {
		t.Fatal(response)
	}
	afterRevision, _ := c.configurationRevisionToken()
	if afterRevision != revision || !reflect.DeepEqual(c.filter.Comments, record.Comments) {
		t.Fatal("failed persistence changed live state")
	}
	s.saveConfigurationFn = nil
	command, request = stateProposal(t, c, 4, "PATCH", "FILTER", "  comments: []\n  block_comments: []\n")
	if response := s.applyMachineRequest(c, command, request); strings.Contains(response, "error:") || len(c.filter.Comments) != 0 || len(c.filter.BlockComments) != 0 {
		t.Fatal(response)
	}
}

func TestCommentReadbackBoundsAndCapabilities(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.Comments = []string{"ALL", "POTA", "up  5", strings.Repeat("!", 64)}
	c.filter.BlockComments = []string{"NONE", "POTA"}
	for _, category := range []string{"COMMENT", "FULL", ""} {
		response, err := s.renderHumanReadback(c, "FILTER", category, 0)
		if err != nil {
			t.Fatal(err)
		}
		assertHumanWire(t, response)
		if category != "" && (!strings.Contains(response, `"ALL"`) || !strings.Contains(response, `"NONE"`) || !strings.Contains(response, `"up  5"`)) {
			t.Fatal("literal phrase readback lost text")
		}
	}
	wire, err := s.renderYAMLReadbackVersion(c, "FILTER", "comments", "session-0", 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := stateReadbackFilter(t, wire); !reflect.DeepEqual(got.Comments, c.filter.Comments) || !reflect.DeepEqual(got.BlockComments, c.filter.BlockComments) {
		t.Fatal("schema4 lost exact lists")
	}
	var counts struct {
		Status struct {
			Comments readbackCommentStatus `yaml:"comments"`
		} `yaml:"status"`
	}
	if err := yaml.Unmarshal([]byte(wire), &counts); err != nil || counts.Status.Comments.PassCount != 4 || counts.Status.Comments.RejectCount != 2 {
		t.Fatalf("incorrect status counts: %v %+v", err, counts)
	}
	caps, err := s.renderYAMLReadbackVersion(c, "CAPABILITIES", "caps", "session-0", 4)
	if err != nil || !strings.Contains(caps, "MINSNR") || !strings.Contains(caps, "COMMENT") || !strings.Contains(caps, "max_phrases_per_list: 32") || !strings.Contains(caps, "max_phrase_bytes: 64") {
		t.Fatalf("schema4 capabilities: %v %s", err, caps)
	}
	c.filter.Comments = make([]string, 33)
	for i := range c.filter.Comments {
		c.filter.Comments[i] = "DUP"
	}
	for _, version := range []int{1, 2, 3, 4} {
		cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
		if err := machineConfigurationFits(cfg, version); err == nil {
			t.Fatalf("schema %d bypassed hidden rules preflight", version)
		}
		if _, err := s.renderYAMLReadbackVersion(c, "FILTER", "bad", "session-0", version); err == nil {
			t.Fatalf("schema %d bypassed readback preflight", version)
		}
	}
}

func TestCommentMachineFrozenV3Order(t *testing.T) {
	cfg := filter.Configuration{Filters: filter.FilterConfiguration{MinSNR: map[string]int{"CW": 3}, Comments: []string{"HIDDEN"}, BlockComments: []string{"HIDDEN"}}}
	_, selected, err := readbackResourceConfigurationVersion(cfg, "FILTER", 3)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := encodeBoundedYAML(selected)
	if err != nil {
		t.Fatal(err)
	}
	want := "---\r\nmin_snr:\r\n  CW: 3\r\n"
	for _, name := range strings.Fields("bands modes sources events confidence path_classes dx_states de_states dx_continents de_continents dx_zones de_zones dx_grid2 de_grid2 dx_dxcc de_dxcc") {
		want += name + ":\r\n  allow_all: false\r\n  block_all: false\r\n  allow: {}\r\n  block: {}\r\n"
	}
	want += "dx_callsigns: []\r\nblock_dx_callsigns: []\r\nde_callsigns: []\r\nblock_de_callsigns: []\r\ninclude_beacons: DEFAULT\r\nallow_wwv: DEFAULT\r\nallow_wcy: DEFAULT\r\nallow_announce: DEFAULT\r\nallow_self: DEFAULT\r\nallow_toxic: DEFAULT\r\nnearby_enabled: false\r\n...\r\n"
	if actual != want {
		t.Fatalf("schema 3 literal wire changed:\n%s", actual)
	}
}

func TestCommentMachineMaximumExactListAndDetach(t *testing.T) {
	repeated := strings.Repeat(fmt.Sprintf("%q,", strings.Repeat("A", 64)), 32)
	request, err := decodeMachineRequest(minSNRMachineFixture("comments: ["+repeated+"]\n", 4, true), machineCommand{Verb: "PATCH", Resource: "FILTER"})
	if err != nil || len(request.Configuration.Filters.Comments) != 32 {
		t.Fatalf("valid maximum raw list rejected: %v", err)
	}
	before := filter.Configuration{Filters: filter.FilterConfiguration{Comments: []string{"OWNED"}, BlockComments: []string{"RETAIN"}}}
	borrowed, err := request.apply(before)
	if err != nil {
		t.Fatal(err)
	}
	detached := borrowed.Clone()
	detached.Filters.Comments[0], detached.Filters.BlockComments[0] = "CHANGED", "CHANGED"
	if before.Filters.Comments[0] != "OWNED" || before.Filters.BlockComments[0] != "RETAIN" || request.Configuration.Filters.Comments[0] != strings.Repeat("A", 64) {
		t.Fatal("detached candidate aliases input")
	}
}
