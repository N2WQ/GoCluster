package telnet

import (
	"errors"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
)

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
