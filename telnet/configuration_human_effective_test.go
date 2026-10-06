package telnet

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHumanEffectiveOrdinaryCategories(t *testing.T) {
	for _, tc := range []struct {
		category, section string
		configure         func(*filter.Filter)
	}{
		{"BAND", "Bands\r\n  PASS: 20m\r\n  REJECT: 40m\r\n", func(f *filter.Filter) {
			f.AllBands = true
			f.Bands = map[string]bool{"20m": true, "40m": true, "80m": false}
			f.BlockBands = map[string]bool{"40m": true, "160m": false}
		}},
		{"MODE", "Modes\r\n  PASS: CW, FT8\r\n  REJECT: USB\r\n  Unknown modes are hidden.\r\n", func(f *filter.Filter) {
			f.Modes = map[string]bool{"CW": true, "FT8": true, "USB": false}
			f.BlockModes = map[string]bool{"USB": true}
		}},
		{"DXZONE", "DX zones\r\n  PASS: 2, 10\r\n  REJECT: 40\r\n", func(f *filter.Filter) {
			f.DXZones = map[int]bool{10: true, 2: true, 3: false, 99: true}
			f.BlockDXZones = map[int]bool{40: true}
		}},
		{"BAND", "Bands\r\n  PASS: NONE\r\n  REJECT: NONE\r\n", func(f *filter.Filter) {
			f.AllBands = true
			f.Bands = map[string]bool{"20m": false}
		}},
		{"BAND", "Bands\r\n  PASS: NONE\r\n  REJECT: ALL\r\n", func(f *filter.Filter) {
			f.BlockAllBands = true
			f.Bands = map[string]bool{"20m": true}
		}},
	} {
		t.Run(tc.category+tc.section, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			tc.configure(c.filter)
			before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
			for _, category := range []string{tc.category, "FULL"} {
				got, err := s.renderHumanReadback(c, "FILTER", category, 30*time.Second)
				if err != nil || !strings.Contains(got, tc.section) {
					t.Fatalf("effective section: %v\n%s", err, got)
				}
				assertHumanWire(t, got)
				if strings.Contains(got, "allow_all:") || strings.Contains(got, "(exact rules)") {
					t.Fatal("stored-state detail leaked into effective view")
				}
			}
			if !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()) {
				t.Fatal("effective display changed configuration")
			}
		})
	}
}

func TestHumanEffectivePathMatcher(t *testing.T) {
	for _, tc := range []struct {
		allow, block map[string]bool
		pass, reject string
		outcomes     []bool // HIGH, MEDIUM, UNLIKELY, CLOSED, INSUFFICIENT
	}{
		{map[string]bool{"UNLIKELY": true}, nil, "CLOSED, UNLIKELY", "NONE", []bool{false, false, true, true, false}},
		{map[string]bool{"CLOSED": true}, map[string]bool{"UNLIKELY": true}, "CLOSED", "UNLIKELY", []bool{false, false, false, true, false}},
		{nil, map[string]bool{"UNLIKELY": true}, "ALL", "CLOSED, UNLIKELY", []bool{true, true, false, false, true}},
	} {
		s, c := readbackTestClient()
		c.filter = filter.NewFilter()
		c.filter.PathClasses, c.filter.BlockPathClasses = tc.allow, tc.block
		got, err := s.renderHumanReadback(c, "FILTER", "PATH", 30*time.Second)
		if err != nil || !strings.Contains(got, "Path\r\n  PASS: "+tc.pass+"\r\n  REJECT: "+tc.reject+"\r\n") {
			t.Fatalf("PATH: %v\n%s", err, got)
		}
		for i, class := range []string{"HIGH", "MEDIUM", "UNLIKELY", "CLOSED", "INSUFFICIENT"} {
			if c.filter.MatchesWithPath(spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW"), class) != tc.outcomes[i] {
				t.Fatalf("matcher disagrees for %s", class)
			}
		}
	}
}

func TestHumanEffectiveInactiveAdmission(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.Bands = map[string]bool{strings.Repeat("x", 1<<20): false}
	c.filter.DXDXCC = make(map[int]bool, 10000)
	for i := range 10000 {
		c.filter.DXDXCC[i] = false
	}
	for _, category := range []string{"BAND", "DXDXCC", "FULL"} {
		got, err := s.renderHumanReadback(c, "FILTER", category, 30*time.Second)
		if err != nil || len(got) > 4096 {
			t.Fatalf("inactive maps rejected effective view %s: %v", category, err)
		}
		assertHumanWire(t, got)
	}
	if got, err := s.renderYAMLReadback(c, "FILTER", "read-1", "session-0"); got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("machine admission was relaxed")
	}
	c.filter.BlockAllBands, c.filter.BlockAllDXDXCC = true, true
	c.filter.Bands[strings.Repeat("x", 1<<20)] = true
	for code := range c.filter.DXDXCC {
		c.filter.DXDXCC[code] = true
	}
	if got, err := s.renderHumanReadback(c, "FILTER", "FULL", 30*time.Second); err != nil || len(got) > 4096 {
		t.Fatalf("overridden maps rejected small FULL: %v", err)
	}
}

func TestHumanEffectivePatternsSwitchesAndSuspension(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.DXCallsigns, c.filter.BlockDXCallsigns = []string{"K*"}, []string{"K1*"}
	got, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil || !strings.Contains(got, "DX calls\r\n  PASS: K*\r\n  REJECT: K1*\r\n  REJECT takes precedence.\r\n") {
		t.Fatalf("patterns: %v\n%s", err, got)
	}
	for _, call := range []string{"K2ABC", "K1ABC", "W1ABC"} {
		if c.filter.Matches(spot.NewSpot(call, "W1XYZ", 14074, "CW")) != (call == "K2ABC") {
			t.Fatal("pattern matcher disagrees")
		}
	}
	for _, state := range []filter.DefaultBool{filter.DefaultBoolDefault, filter.DefaultBoolTrue, filter.DefaultBoolFalse} {
		c.filter.IncludeBeacons = state.Pointer()
		got, err = s.renderHumanReadback(c, "FILTER", "BEACON", 30*time.Second)
		want := "Beacons: ON\r\n"
		if state == filter.DefaultBoolFalse {
			want = "Beacons: OFF\r\n"
		}
		if err != nil || !strings.Contains(got, want) || strings.Contains(got, "DEFAULT") {
			t.Fatalf("toggle: %v\n%s", err, got)
		}
	}
	c.filter.NearbyEnabled = true
	c.filter.BlockAllDXDXCC = true
	for _, category := range []string{"DXCONT", "DECONT", "DXZONE", "DEZONE", "DXGRID2", "DEGRID2", "DXDXCC", "DEDXCC"} {
		got, err = s.renderHumanReadback(c, "FILTER", category, 30*time.Second)
		if err != nil || !strings.Contains(got, "  PASS: ALL\r\n  REJECT: NONE\r\n  Suspended by NEARBY; rules retained.\r\n") {
			t.Fatalf("suspended %s: %v\n%s", category, err, got)
		}
	}
}

func TestHumanEffectiveEventAndConfidenceExceptions(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.AllEvents = false
	c.filter.Events = map[string]bool{" pota ": false, "POTA": true}
	c.filter.BlockEvents = map[string]bool{" sota ": false}
	for _, blockAll := range []bool{false, true} {
		c.filter.BlockAllEvents = blockAll
		got, err := s.renderHumanReadback(c, "FILTER", "EVENT", 30*time.Second)
		want := "  PASS: POTA\r\n  REJECT: SOTA\r\n"
		if blockAll {
			want = "  PASS: NONE\r\n  REJECT: ALL\r\n"
		}
		if err != nil || !strings.Contains(got, want+"  Untagged spots are always included.\r\n") {
			t.Fatalf("EVENT: %v\n%s", err, got)
		}
		for _, tc := range []struct {
			mask spot.EventMask
			pass bool
		}{{0, true}, {spot.EventPOTA, !blockAll}, {spot.EventPOTA | spot.EventSOTA, false}} {
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			candidate.Events = tc.mask
			if c.filter.Matches(candidate) != tc.pass {
				t.Fatal("EVENT matcher disagrees")
			}
		}
	}
	c.filter = filter.NewFilter()
	c.filter.BlockAllConfidence = true
	c.filter.ResetModes()
	got, err := s.renderHumanReadback(c, "FILTER", "CONFIDENCE", 30*time.Second)
	if err != nil || !strings.Contains(got, "  PASS: NONE\r\n  REJECT: ALL\r\n  Exempt modes still pass.\r\n") {
		t.Fatalf("confidence: %v\n%s", err, got)
	}
	for _, mode := range []string{"CW", "MSK144"} {
		if c.filter.Matches(spot.NewSpot("K1ABC", "W1XYZ", 14074, mode)) != (mode == "MSK144") {
			t.Fatal("confidence matcher disagrees")
		}
	}
	c.filter.BlockAllConfidence = false
	c.filter.BlockConfidence = map[string]bool{"P": false, "UNREACHABLE": true}
	got, err = s.renderHumanReadback(c, "FILTER", "CONFIDENCE", 30*time.Second)
	if err != nil || !strings.Contains(got, "Confidence\r\n  PASS: ALL\r\n  REJECT: NONE\r\n") || strings.Contains(got, "Exempt modes") {
		t.Fatalf("inactive confidence state changed effective output: %v\n%s", err, got)
	}
}

func TestHumanEffectiveLiteralSentinelsAndActiveBudget(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.DXCallsigns = []string{"ALL", "NONE", "K,W", " café\t"}
	got, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil || !strings.Contains(got, `  PASS: "ALL", "NONE", "K,W", " caf\u00e9\t"`) {
		t.Fatalf("literal syntax: %v\n%s", err, got)
	}
	assertHumanWire(t, got)
	for _, value := range c.filter.DXCallsigns {
		var h humanResponse
		if err := writeHumanEffectiveList(&h, "PASS", []string{value}, false); err != nil || unquoteHumanPieces(t, string(h.data)) != value {
			t.Fatal("literal values lost bytes")
		}
	}
	c.filter = filter.NewFilter()
	c.filter.DXContinents = map[string]bool{strings.Repeat("X", 65537): true}
	for _, category := range []string{"DXCONT", "FULL"} {
		if got, err = s.renderHumanReadback(c, "FILTER", category, 30*time.Second); got != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatal("oversized active map produced partial response")
		}
	}
}

func TestHumanEffectiveDenseIntegerListAdmission(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.BlockDXZones = make(map[int]bool, 7800)
	for code := 10000; code < 17800; code++ {
		c.filter.BlockDXZones[code] = true
	}
	got, err := s.renderHumanReadback(c, "FILTER", "DXZONE", 30*time.Second)
	if err != nil {
		t.Fatalf("fitting active list rejected: %v", err)
	}
	assertHumanWire(t, got)
	var values []string
	for _, line := range strings.Split(got, "\r\n") {
		if strings.HasPrefix(line, "  REJECT:") {
			line = strings.TrimPrefix(line, "  REJECT:")
		} else if !strings.HasPrefix(line, "          ") {
			continue
		}
		for _, value := range strings.Fields(line) {
			values = append(values, strings.TrimSuffix(value, ","))
		}
	}
	if len(values) != 7800 {
		t.Fatalf("active integer entries omitted: %d", len(values))
	}
	for i, value := range values {
		if value != strconv.Itoa(10000+i) {
			t.Fatal("integer identity or ordering lost")
		}
	}
}

func TestHumanEffectiveLongListContinuation(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	long := strings.Repeat("X", 200)
	c.filter.DXCallsigns = []string{"K*", long, "W*"}
	got, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertHumanWire(t, got)
	if strings.Count(got, "  PASS:") != 1 || unquoteHumanPieces(t, got) != long || !strings.Contains(got, "        W*") {
		t.Fatal("list continuation repeated the label or lost a value")
	}
}

func TestHumanEffectiveDXCCOverlap(t *testing.T) {
	for _, domain := range []string{"DXDXCC", "DEDXCC"} {
		t.Run(domain, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			s.ctyLookup = func() *cty.CTYDatabase { return canonicalTestCTY() }
			if domain == "DXDXCC" {
				c.filter.DXDXCC = map[int]bool{248: true, 291: true, 460: false}
				c.filter.BlockDXDXCC = map[int]bool{248: true}
			} else {
				c.filter.DEDXCC = map[int]bool{248: true, 291: true, 460: false}
				c.filter.BlockDEDXCC = map[int]bool{248: true}
			}
			if !s.handleHumanReadback(c, "SHOW FILTER "+domain) {
				t.Fatal("category dispatch failed")
			}
			got := string((<-c.controlChan).raw)
			assertHumanWire(t, got)
			if !strings.Contains(got, "  PASS: K\r\n  REJECT: I, IG9, IT9\r\n") || strings.Contains(got, "3D2/R") {
				t.Fatalf("DXCC overlap or inactive entry: %s", got)
			}
			for _, code := range []int{248, 291, 460} {
				candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
				if domain == "DXDXCC" {
					candidate.DXMetadata.ADIF = code
				} else {
					candidate.DEMetadata.ADIF = code
				}
				if c.filter.Matches(candidate) != (code == 291) {
					t.Fatal("DXCC matcher disagrees")
				}
			}
		})
	}
}
