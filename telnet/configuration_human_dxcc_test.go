package telnet

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
)

func TestHumanCanonicalDXCCConflictReadbacks(t *testing.T) {
	for _, tc := range []struct {
		name, first, second string
		alternatives        bool
	}{
		{name: "fallback", first: "Unknown DXCC (291)", second: "Unknown DXCC (999)"},
		{name: "alternatives", first: "W", second: "Z", alternatives: true},
	} {
		for _, domain := range []string{"DXDXCC", "DEDXCC"} {
			t.Run(tc.name+"/"+domain, func(t *testing.T) {
				db := &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
					"FIRST": {Prefix: "K", ADIF: 291}, "SECOND": {Prefix: " k ", ADIF: 999},
				}}
				if tc.alternatives {
					db.Data["ALT-FIRST"] = cty.PrefixInfo{Prefix: "W", ADIF: 291}
					db.Data["ALT-SECOND"] = cty.PrefixInfo{Prefix: "Z", ADIF: 999}
				}
				s, c := readbackTestClient()
				c.filter = filter.NewFilter()
				heading := "DX DXCC"
				if domain == "DXDXCC" {
					c.filter.AllDXDXCC, c.filter.BlockAllDXDXCC = false, true
					c.filter.DXDXCC = map[int]bool{291: true, 999: false}
					c.filter.BlockDXDXCC = map[int]bool{291: false, 999: true}
				} else {
					heading = "DE DXCC"
					c.filter.AllDEDXCC, c.filter.BlockAllDEDXCC = false, true
					c.filter.DEDXCC = map[int]bool{291: true, 999: false}
					c.filter.BlockDEDXCC = map[int]bool{291: false, 999: true}
				}
				before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
				calls := 0
				s.ctyLookup = func() *cty.CTYDatabase { calls++; return db }
				section := fmt.Sprintf("%s (exact rules)\r\n  allow_all: false\r\n  block_all: true\r\n  allow:\r\n    %q: true\r\n    %q: false\r\n  block:\r\n    %q: false\r\n    %q: true\r\n", heading, tc.first, tc.second, tc.first, tc.second)
				for _, command := range []string{"SHOW FILTER " + domain, "SHOW FILTER FULL"} {
					previousCalls := calls
					if !s.handleHumanReadback(c, command) {
						t.Fatal("production dispatch not handled")
					}
					message := <-c.controlChan
					response := string(message.raw)
					assertHumanWire(t, response)
					if !strings.Contains(response, section) || strings.Contains(response, `"K"`) {
						t.Fatalf("conflict labels/flags/false entries: %q", response)
					}
					if command != "SHOW FILTER FULL" && response != "User          W1ABC-1\r\nPreset        (none)\r\n\r\n"+section+readbackFooterLiteral {
						t.Fatalf("complete category response changed: %q", response)
					}
					if calls != previousCalls+1 || message.readback.epoch == 0 || !c.readPausePending.Load() {
						t.Fatal("snapshot/readback hold changed")
					}
				}
				if !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()) {
					t.Fatal("readback changed stored numeric rules")
				}
				if domain == "DXDXCC" {
					c.filter.AllDXDXCC, c.filter.BlockAllDXDXCC = true, false
					c.filter.DXDXCC = nil
					c.filter.BlockDXDXCC = map[int]bool{291: true, 999: true}
				} else {
					c.filter.AllDEDXCC, c.filter.BlockAllDEDXCC = true, false
					c.filter.DEDXCC = nil
					c.filter.BlockDEDXCC = map[int]bool{291: true, 999: true}
				}
				if !s.handleHumanReadback(c, "SHOW FILTER") {
					t.Fatal("overview not handled")
				}
				response := string((<-c.controlChan).raw)
				assertHumanWire(t, response)
				if !strings.Contains(response, "DXCC: All except "+tc.first+", "+tc.second) {
					t.Fatalf("overview lost entity identities: %q", response)
				}
				legacy := formatFilterSnapshot(c.filter, s.ctyLookup)
				if !strings.Contains(legacy, domain+": allow=ALL block="+tc.first+", "+tc.second) {
					t.Fatalf("legacy labels: %q", legacy)
				}
				rules := filter.IntRules{AllowAll: true, Block: map[int]bool{291: true, 999: true}}
				if got := humanDXCCSummary(rules, cty.NewDXCCIndex(db), 10); got != "All except 2 blocked DXCC entries" {
					t.Fatalf("overview counted labels instead of entities: %q", got)
				}
			})
		}
	}
}

func TestHumanCanonicalDXCCProductionReadbacks(t *testing.T) {
	for _, command := range []string{"SHOW FILTER", "SHOW/FILTER", "SH/FILTER", "SHOW FILTER FULL", "SHOW FILTER DXDXCC", "SHOW FILTER DEDXCC"} {
		t.Run(command, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			c.filter.BlockDXDXCC = map[int]bool{248: true, 12345: true, 291: false}
			c.filter.DEDXCC = map[int]bool{460: true, 291: false}
			c.filter.AllDEDXCC = false
			c.filter.BlockAllDEDXCC = true
			calls := 0
			s.ctyLookup = func() *cty.CTYDatabase { calls++; return canonicalTestCTY() }
			if !s.handleHumanReadback(c, command) {
				t.Fatal("production dispatch not handled")
			}
			message := <-c.controlChan
			response := string(message.raw)
			assertHumanWire(t, response)
			if calls != 1 || message.readback.epoch == 0 || !c.readPausePending.Load() {
				t.Fatal("snapshot/readback hold changed")
			}
			if strings.HasSuffix(command, "DEDXCC") {
				for _, want := range []string{`"3D2/R": true`, `"K": false`, `allow_all: false`, `block_all: true`} {
					if !strings.Contains(response, want) {
						t.Fatalf("missing %s: %q", want, response)
					}
				}
			} else {
				if !strings.Contains(response, "I, IG9, IT9") || !strings.Contains(response, "Unknown DXCC (12345)") {
					t.Fatalf("entity labels missing: %q", response)
				}
				if strings.Contains(command, "FULL") || strings.HasSuffix(command, "DXDXCC") {
					for _, want := range []string{`"I, IG9, IT9": true`, `"K": false`, `block_all: false`} {
						if !strings.Contains(response, want) {
							t.Fatalf("missing %s: %q", want, response)
						}
					}
				}
			}
		})
	}
}

func TestHumanCanonicalDXCCSnapshotAndUnavailable(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.BlockDXDXCC = map[int]bool{248: true}
	first, second := canonicalTestCTY(), canonicalTestCTY()
	for key, record := range second.Data {
		if record.ADIF == 248 {
			record.Prefix = "NEW"
			second.Data[key] = record
		}
	}
	calls := 0
	s.ctyLookup = func() *cty.CTYDatabase {
		calls++
		if calls == 1 {
			return first
		}
		return second
	}
	response, err := s.renderHumanReadback(c, "FILTER", "DXDXCC", time.Second)
	if err != nil || calls != 1 || !strings.Contains(response, `"I, IG9, IT9": true`) {
		t.Fatalf("preflight/generation drift: %q, %v, calls=%d", response, err, calls)
	}
	response, err = s.renderHumanReadback(c, "FILTER", "DXDXCC", time.Second)
	if err != nil || calls != 2 || !strings.Contains(response, `"NEW": true`) {
		t.Fatal("replacement CTY not visible")
	}
	s.ctyLookup = nil
	response, err = s.renderHumanReadback(c, "FILTER", "DXDXCC", time.Second)
	if err != nil || !strings.Contains(response, `"Unknown DXCC (248)": true`) {
		t.Fatal("unavailable CTY hid saved rule")
	}
}

func TestHumanCanonicalDXCCBoundsAndEntityCounts(t *testing.T) {
	index := cty.NewDXCCIndex(canonicalTestCTY())
	rules := filter.IntRules{AllowAll: true, Block: map[int]bool{248: true}}
	if got := humanDXCCSummary(rules, index, humanValueWidth); got != "All except I, IG9, IT9" {
		t.Fatalf("shared entity summary: %q", got)
	}
	if got := humanDXCCSummary(rules, index, 10); got != "All except 1 blocked DXCC entry" {
		t.Fatalf("counted prefixes as entities: %q", got)
	}
	rules.Allow = map[int]bool{291: false}
	if got := humanDXCCSummary(rules, index, humanValueWidth); got != "None" {
		t.Fatalf("false-only map became unrestricted: %q", got)
	}
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.BlockDXDXCC = map[int]bool{248: true}
	for _, label := range []string{strings.Repeat("X", 100), strings.Repeat("\"", 40000), strings.Repeat("X", maxYAMLBytes+1)} {
		s.ctyLookup = func() *cty.CTYDatabase {
			return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{"LABEL": {Prefix: label, ADIF: 248}}}
		}
		response, err := s.renderHumanReadback(c, "FILTER", "DXDXCC", time.Second)
		if len(label) == 100 {
			if err != nil {
				t.Fatal(err)
			}
			assertHumanWire(t, response)
		} else if response != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatalf("oversized/escaped labels produced partial output: %d %v", len(response), err)
		}
		response, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
		if err != nil || !strings.Contains(response, "1 blocked DXCC entry") {
			t.Fatalf("overview must count oversized label: %q %v", response, err)
		}
	}
}

func TestHumanCanonicalDXCCCompleteResponseBoundary(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.BlockDXDXCC = map[int]bool{248: true}
	render := func(size int) (string, error) {
		s.ctyLookup = func() *cty.CTYDatabase {
			return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{"LABEL": {Prefix: strings.Repeat("X", size), ADIF: 248}}}
		}
		return s.renderHumanReadback(c, "FILTER", "DXDXCC", time.Second)
	}
	// Find adjacent complete/error responses across the real wire-size boundary,
	// accounting for quoted-piece overhead, headers, CRLF and the reading footer.
	low, high := 1, maxYAMLBytes
	for low+1 < high {
		middle := (low + high) / 2
		response, err := render(middle)
		if err == nil {
			assertHumanWire(t, response)
			low = middle
		} else {
			if response != "" || !errors.Is(err, errReadbackTooLarge) {
				t.Fatalf("unexpected boundary failure: %v", err)
			}
			high = middle
		}
	}
	response, err := render(low)
	if err != nil || len(response) > maxYAMLBytes || !strings.Contains(response, "Type RESUME when ready.") {
		t.Fatal("last fitting response was incomplete")
	}
	if response, err := render(high); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("first oversized response was partially returned")
	}
}
