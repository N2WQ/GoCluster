package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func assertNativeCCHumanWidth(t *testing.T, body string) {
	t.Helper()
	if len(body) > maxV7YAMLBytes || !strings.HasSuffix(body, "\r\n") {
		t.Fatalf("invalid native bounded response size/end: %d", len(body))
	}
	for _, line := range strings.Split(strings.TrimSuffix(body, "\r\n"), "\r\n") {
		if len(line) > humanReadbackWidth || strings.ContainsAny(line, "\r\n") {
			t.Fatalf("invalid native response line width: %q", line)
		}
		for _, value := range []byte(line) {
			if value < 32 || value > 126 {
				t.Fatalf("nonprintable/non-ASCII native readback: %q", line)
			}
		}
	}
}

func TestNativeCCReadbackDistinguishesInheritanceOffAndActiveBase(t *testing.T) {
	c, _, _ := nativeCCCommandFixture(t)
	configuration := nativeCCConfiguration(t, c)
	configuration.Geography["DOC"] = filter.CCGeography{
		Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{291}},
		Bands:  map[string]filter.CCCategory{"20": {Action: filter.CCOff}, "40": {Action: filter.CCReject, Countries: []int{230}}},
	}
	configuration.DXBM = filter.CCDXBM{Active: true, BaseAllow: true, Slots: map[string]bool{}}
	_, value, err := filter.NewCCFilter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	before, pointer := nativeCCConfiguration(t, c), c.filter
	response, err := c.server.nativeCCReadback(c, "FILTER", "FULL", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(response), " ")
	for _, want := range []string{
		"Active filter bank: CC", "DOC - spotter country", "Global: PASS", "ADIF values: 291",
		"Band 20: OFF", "Band 40: REJECT", "Band 160: INHERIT global", "DXBM: ACTIVE; base ALLOW",
		"20-CW: ALLOW (base)", "SELF: ON", "spots of your own call", "TOXIC still applies",
		"GET YAML CONFIG SCHEMA 7", "schema 6 is not a complete backup", "Type RESUME", "Missed spots are not replayed",
	} {
		if !strings.Contains(strings.ToLower(flat), strings.ToLower(want)) {
			t.Fatalf("native readback loses %q: %s", want, response)
		}
	}
	if strings.Contains(response, "Rule 0") || strings.Contains(response, "PASS RULE") || strings.Contains(response, "HELP RULE") {
		t.Fatal("active native CC readback advertises Boolean rule editing")
	}
	assertNativeCCHumanWidth(t, response)
	if pointer != c.filter || !before.Equal(nativeCCConfiguration(t, c)) || c.configurationRevision != 0 {
		t.Fatal("native readback changed configuration or revision")
	}
	for _, category := range []string{"DOC", "DOS", "DXCTY", "DXSTATE", "DXBM", "MODE", "SOURCE", "BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC"} {
		text, err := c.server.nativeCCReadback(c, "FILTER", category, 30*time.Second)
		if err != nil {
			t.Fatalf("advertised category %s unavailable: %v", category, err)
		}
		assertNativeCCHumanWidth(t, text)
	}
	if _, err := c.server.nativeCCReadback(c, "FILTER", "RULE", 30*time.Second); err == nil {
		t.Fatal("CC inspection accepted a Boolean RULE category")
	}
}

func TestNativeCCReadbackLargeAcceptedBankAndExactCount(t *testing.T) {
	c, _, _ := nativeCCCommandFixture(t)
	configuration := nativeCCConfiguration(t, c)
	values := make([]int, 512)
	for i := range values {
		values[i] = i + 1
	}
	for _, family := range []string{"DOC", "DXCTY", "DOS", "DXSTATE"} {
		selection := filter.CCCategory{Action: filter.CCPass}
		if family == "DOC" || family == "DXCTY" {
			selection.Countries = values
		} else {
			selection.States = spot.StateCodes()
		}
		geography := filter.CCGeography{Global: selection, Bands: map[string]filter.CCCategory{}}
		for _, band := range filter.CCSupportedBands() {
			geography.Bands[band] = selection
		}
		configuration.Geography[family] = geography
	}
	_, value, err := filter.NewCCFilter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	response, err := c.server.nativeCCReadback(c, "FILTER", "FULL", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(response) <= maxYAMLBytes || len(response) > maxV7YAMLBytes || !strings.HasSuffix(response, "\r\n") {
		t.Fatalf("large native bank did not use complete new bounded output: %d bytes", len(response))
	}
	assertNativeCCHumanWidth(t, response)
	calls := 0
	text, err := renderNativeCCBody(100, func(h *humanResponse) error {
		calls++
		if err := h.line(strings.Repeat("x", 70)); err != nil {
			return err
		}
		return h.line(strings.Repeat("y", 70))
	})
	if err == nil || text != "" || calls != 1 {
		t.Fatal("oversized counted response produced partial output or a writing pass")
	}
}

func TestNativeCCReadbackExplicitEmptyCountryAndStateSelections(t *testing.T) {
	c, _, _ := nativeCCCommandFixture(t)
	configuration := nativeCCConfiguration(t, c)
	configuration.Modes, configuration.Sources = nil, nil
	configuration.Geography["DOC"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCPass}}
	configuration.Geography["DOS"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCReject}}
	_, value, err := filter.NewCCFilter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	*c.filter = value
	response, err := c.server.nativeCCReadback(c, "FILTER", "FULL", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(response), " ")
	for _, want := range []string{"Modes: (none)", "Sources: (none)", "ADIF values: (none)", "State values: (none)", "Empty PASS matches no metadata", "Empty REJECT excludes no metadata", "DXBM: OFF"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("explicit empty state hidden: %q", want)
		}
	}
}
