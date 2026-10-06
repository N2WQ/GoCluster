package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHumanFiniteOverviewWrapping(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		set        func(*filter.Filter)
	}{
		{"bands", "Bands         Only 1.25m, 10m, 12m, 13cm, 15m, 160m, 17m, 20m, 2200m, 23cm,\r\n              2m, 30m, 33cm, 40m, 60m, 630m, 6m, 70cm, 80m\r\n", func(f *filter.Filter) {
			f.AllBands = true
			for _, band := range spot.SupportedBandNames() {
				f.Bands[band] = true
			}
		}},
		{"modes", "Modes         CW, FT2, FT4, FT8, JS8, LSB, MSK144, PSK, RTTY, SSTV, UNKNOWN,\r\n              USB; unknown modes included\r\n", func(f *filter.Filter) {
			f.AllModes = true
			for _, mode := range filter.SupportedModes() {
				f.Modes[mode] = true
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			tc.set(c.filter)
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil || !strings.Contains(got, tc.want) {
				t.Fatalf("complete wrapped selections missing: %v\n%s", err, got)
			}
			assertHumanWire(t, got)
			if strings.Contains(got, "Disabled:") || strings.Contains(got, "Enabled:") {
				t.Fatal("overview added enabled/disabled inventories")
			}
		})
	}
}

func TestHumanFiniteMatcherSelections(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		set        func(*filter.Filter)
		spots      []*spot.Spot
		pass       []bool
	}{
		{"bands", "Bands         Only 40m; block 20m\r\n", func(f *filter.Filter) {
			f.AllBands = true
			f.Bands = map[string]bool{"20m": true, "40m": true, "80m": false}
			f.BlockBands = map[string]bool{"20m": true, "40m": false}
		}, []*spot.Spot{spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW"), spot.NewSpot("K1ABC", "W1XYZ", 7000, "CW"), spot.NewSpot("K1ABC", "W1XYZ", 3500, "CW")}, []bool{false, true, false}},
		{"modes", "Modes         FT8; block CW; unknown modes hidden\r\n", func(f *filter.Filter) {
			f.AllModes = true
			f.Modes = map[string]bool{"CW": true, "FT8": true, "USB": false, "UNKNOWN": false}
			f.BlockModes = map[string]bool{"CW": true, "FT8": false}
		}, []*spot.Spot{spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW"), spot.NewSpot("K1ABC", "W1XYZ", 14074, "FT8"), spot.NewSpot("K1ABC", "W1XYZ", 14074, "USB"), spot.NewSpot("K1ABC", "W1XYZ", 14074, "")}, []bool{false, true, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			tc.set(c.filter)
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil || !strings.Contains(got, tc.want) {
				t.Fatalf("passing selections disagree: %v\n%s", err, got)
			}
			for i, candidate := range tc.spots {
				if c.filter.Matches(candidate) != tc.pass[i] {
					t.Fatalf("matcher disagrees for spot %d", i)
				}
			}
			assertHumanWire(t, got)
		})
	}

	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.ResetModes()
	c.filter.Confidence = map[string]bool{"V": true, "P": true, "S": false}
	c.filter.BlockConfidence = map[string]bool{"P": true, "V": false}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "Confidence    Only V; block P; exempt modes still pass\r\n") {
		t.Fatalf("confidence selection misdescribed: %v\n%s", err, got)
	}
	for _, tc := range []struct {
		mode, glyph string
		pass        bool
	}{{"CW", "V", true}, {"CW", "P", false}, {"CW", "S", false}, {"MSK144", "P", true}} {
		candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, tc.mode)
		candidate.Confidence = tc.glyph
		if c.filter.Matches(candidate) != tc.pass {
			t.Fatalf("confidence matcher disagrees for %s/%s", tc.mode, tc.glyph)
		}
	}

	c.filter = filter.NewFilter()
	c.filter.PathClasses = map[string]bool{"UNLIKELY": true, "HIGH": true, "MEDIUM": false}
	c.filter.BlockPathClasses = map[string]bool{"HIGH": true}
	got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "Path          Only CLOSED, UNLIKELY\r\n              CLOSED follows UNLIKELY unless explicitly selected\r\n") {
		t.Fatalf("PATH selection misdescribed: %v\n%s", err, got)
	}
	for _, tc := range []struct {
		class string
		pass  bool
	}{{"HIGH", false}, {"MEDIUM", false}, {"UNLIKELY", true}, {"CLOSED", true}, {"INSUFFICIENT", false}} {
		if c.filter.MatchesWithPath(spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW"), tc.class) != tc.pass {
			t.Fatalf("PATH matcher disagrees for %s", tc.class)
		}
	}
}

func TestHumanFiniteEventTaxonomy(t *testing.T) {
	var definition strings.Builder
	definition.WriteString("modes:\n  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\nevents:\n")
	for i := range 15 {
		fmt.Fprintf(&definition, "  - name: EV%02d\n    filter_visible: %t\n", i, i != 14)
	}
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	if err := os.WriteFile(path, []byte(definition.String()), 0600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.AllEvents = false
	for i := range 15 {
		c.filter.Events[fmt.Sprintf("EV%02d", i)] = false
	}
	c.filter.Events[" ev03 "] = false
	c.filter.BlockEvents[" ev04 "] = false
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	want := "Events        EV00, EV01, EV02, EV03, EV05, EV06, EV07, EV08, EV09, EV10,\r\n              EV11, EV12, EV13, EV14; block EV04; untagged included\r\n"
	if err != nil || !strings.Contains(got, want) {
		t.Fatalf("configured EVENT families missing: %v\n%s", err, got)
	}
	assertHumanWire(t, got)
	for _, tc := range []struct {
		mask spot.EventMask
		pass bool
	}{{0, true}, {spot.EventMaskForName("EV00"), true}, {spot.EventMaskForName("EV14"), true}, {spot.EventMaskForName("EV04"), false}, {spot.EventMaskForName("EV00") | spot.EventMaskForName("EV04"), false}} {
		candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		candidate.Events = tc.mask
		if c.filter.Matches(candidate) != tc.pass {
			t.Fatalf("EVENT matcher disagrees for mask %x", tc.mask)
		}
	}
}

func TestHumanFiniteGeographyAndSourceMatching(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.DXContinents = map[string]bool{"NA": true, "EU": true, "OC": false}
	c.filter.BlockDXContinents = map[string]bool{"EU": true}
	c.filter.DEContinents = map[string]bool{"AS": true, "AF": false}
	c.filter.BlockDEContinents = map[string]bool{"AS": false}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	for _, want := range []string{
		"DX geography  Continents: Only NA; block EU | Zones: All | DXCC: All\r\n",
		"DE geography  Continents: Only AS | Zones: All | DXCC: All\r\n",
	} {
		if err != nil || !strings.Contains(got, want) {
			t.Fatalf("continent selection misdescribed: %v\n%s", err, got)
		}
	}
	for _, tc := range []struct {
		dx, de string
		pass   bool
	}{{"NA", "AS", true}, {"EU", "AS", false}, {"OC", "AS", false}, {"NA", "AF", false}} {
		candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		candidate.DXMetadata.Continent, candidate.DEMetadata.Continent = tc.dx, tc.de
		if c.filter.Matches(candidate) != tc.pass {
			t.Fatalf("continent matcher disagrees for %s/%s", tc.dx, tc.de)
		}
	}
	c.filter.NearbyEnabled = true
	got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "DX geography  Suspended by NEARBY; rules retained\r\nDE geography  Suspended by NEARBY; rules retained\r\n") {
		t.Fatalf("NEARBY did not suspend ordinary geography presentation: %v\n%s", err, got)
	}
	for _, restricted := range []bool{false, true} {
		c.filter = filter.NewFilter()
		c.filter.Sources = nil
		c.filter.BlockSources = map[string]bool{"SKIMMER": true}
		want := "Sources       All except SKIMMER\r\n"
		if restricted {
			c.filter.Sources = map[string]bool{"HUMAN": true, "SKIMMER": false}
			want = "Sources       Only HUMAN\r\n"
		}
		got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
		if err != nil || !strings.Contains(got, want) {
			t.Fatalf("source selection misdescribed: %v\n%s", err, got)
		}
		for _, human := range []bool{false, true} {
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			candidate.IsHuman = human
			if c.filter.Matches(candidate) != human {
				t.Fatal("source matcher disagrees")
			}
		}
	}
}

func TestHumanFiniteRestoredValues(t *testing.T) {
	for _, tc := range []struct {
		name, label, key string
		set              func(*filter.Filter, string)
		spot             func(string) *spot.Spot
	}{
		{"band Unicode", "Bands", "20mé", func(f *filter.Filter, key string) { f.Bands = map[string]bool{key: true} }, func(key string) *spot.Spot {
			s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			s.Band, s.BandNorm = key, key
			return s
		}},
		{"mode control", "Modes", "MODE\tX", func(f *filter.Filter, key string) { f.Modes = map[string]bool{key: true} }, func(key string) *spot.Spot { return spot.NewSpot("K1ABC", "W1XYZ", 14074, key) }},
		{"mode pieces", "Modes", strings.Repeat("X", 100) + "É\t \\\"X", func(f *filter.Filter, key string) { f.Modes = map[string]bool{key: true} }, func(key string) *spot.Spot { return spot.NewSpot("K1ABC", "W1XYZ", 14074, key) }},
		{"continent Unicode", "DX geography", "EUÉ", func(f *filter.Filter, key string) { f.DXContinents = map[string]bool{key: true} }, func(key string) *spot.Spot {
			s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			s.DXMetadata.Continent = key
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := presetTestServer(t)
			f := filter.NewFilter()
			f.ResetModes()
			tc.set(f, tc.key)
			cfg := filter.ConfigurationFromFilter(f, filter.SettingsConfiguration{Dialect: "go"})
			if err := filter.SaveConfiguration("W1ABC-1", cfg, nil, nil); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			c := configurationTestClient(s, "W1ABC-1")
			restored, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
			if err != nil || restored.loadError != nil {
				t.Fatalf("restore failed: %v; record: %v", err, restored.loadError)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			assertHumanWire(t, got)
			var row strings.Builder
			started := false
			for _, line := range strings.Split(got, "\r\n") {
				if strings.HasPrefix(line, fmt.Sprintf("%-14s", tc.label)) {
					started = true
				} else if started && !strings.HasPrefix(line, "              ") {
					break
				}
				if started {
					row.WriteString(line + "\r\n")
				}
			}
			if decoded := unquoteHumanPieces(t, row.String()); decoded != tc.key {
				t.Fatalf("overview lost retained bytes: %q != %q", decoded, tc.key)
			}
			if !c.filter.Matches(tc.spot(tc.key)) {
				t.Fatal("retained selection is not reachable in the matcher")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("readback changed the saved record: %v", err)
			}
		})
	}
}

func TestHumanFinitePreflightBounded(t *testing.T) {
	many := make(map[string]bool, 12000)
	for i := range 12000 {
		many[fmt.Sprintf("X%05d", i)] = true
	}
	for _, tc := range []struct {
		name  string
		rules filter.StringRules
	}{
		{"long key", filter.StringRules{Allow: map[string]bool{strings.Repeat("X", 1<<20): true}}},
		{"escaped expansion", filter.StringRules{Allow: map[string]bool{strings.Repeat("É", 12000): true}}},
		{"many keys", filter.StringRules{Allow: many}},
		{"large block after small allow", filter.StringRules{Allow: map[string]bool{"CW": true}, Block: map[string]bool{strings.Repeat("X", 1<<20): true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(10, func() {
				allow, block, err := humanFiniteRuleKeys(tc.rules, nil, nil, 65536)
				if !errors.Is(err, errReadbackTooLarge) || allow != nil || block != nil {
					t.Fatal("oversize selection copied or accepted")
				}
			})
			if allocations != 0 {
				t.Fatalf("keys allocated before aggregate fit: %f", allocations)
			}
		})
	}
}

func TestHumanFiniteResponseBudget(t *testing.T) {
	s, c := readbackTestClient()
	low, high := 0, 65537
	for low+1 < high {
		middle := (low + high) / 2
		c.filter.Modes = map[string]bool{strings.Repeat("X", middle): true}
		_, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
		if err == nil {
			low = middle
		} else if errors.Is(err, errReadbackTooLarge) {
			high = middle
		} else {
			t.Fatal(err)
		}
	}
	c.filter.Modes = map[string]bool{strings.Repeat("X", low): true}
	got, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.callsign += strings.Repeat("A", 65536-len(got))
	got, err = s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil || len(got) != 65536 {
		t.Fatalf("exact finite response boundary: %d, %v", len(got), err)
	}
	assertHumanWire(t, got)
	if decoded := unquoteHumanPieces(t, got); decoded != strings.Repeat("X", low) {
		t.Fatal("boundary response omitted bytes")
	}
	c.callsign += "A"
	if got, err = s.renderHumanReadback(c, "FILTER", "", 30*time.Second); got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("finite overview overflow returned partial success")
	}
	if !s.handleHumanReadback(c, "SHOW FILTER") {
		t.Fatal("size-error command unhandled")
	}
	message := <-c.controlChan
	if string(message.raw) != "Readback failed: response exceeds 65,536 bytes.\r\n"+readbackFooterLiteral || message.readback.epoch == 0 {
		t.Fatal("finite overview size error lost its explicit error or reading hold")
	}
	c.callsign = "W1ABC-1"
	for _, key := range []string{strings.Repeat("É", 12000), strings.Repeat("X", 1<<20)} {
		c.filter.Modes = map[string]bool{key: true}
		if got, err = s.renderHumanReadback(c, "FILTER", "", time.Second); got != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatal("oversized finite selection returned partial success")
		}
	}
}

func TestHumanFiniteKeepsLargeCategoryCounts(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	for i := range 100 {
		c.filter.DXCallsigns = append(c.filter.DXCallsigns, fmt.Sprintf("N%d*", i))
		c.filter.DXDXCC[i+1] = true
		c.filter.DXGrid2Prefixes[string([]byte{byte('A' + i/26), byte('A' + i%26)})] = true
	}
	for zone := 1; zone <= 40; zone++ {
		c.filter.DXZones[zone] = true
	}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"DX calls      Only 100 patterns", "Zones: Only 40 zones", "DXCC: Only 100 DXCC entries", "Grids: Only 100 grids"} {
		if !strings.Contains(got, want) {
			t.Fatalf("large category no longer has bounded counts: %s\n%s", want, got)
		}
	}
	assertHumanWire(t, got)
}

func TestHumanFiniteOverviewStates(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*filter.Filter)
		want []string
	}{
		{"default", func(*filter.Filter) {}, []string{
			"Bands         All\r\n", "Modes         CW, LSB, RTTY, UNKNOWN, USB; unknown modes included\r\n",
			"Sources       All (HUMAN, SKIMMER)\r\n", "Events        All; untagged included\r\n",
			"Confidence    All\r\n", "Path          All\r\n",
			"DX geography  Continents: All | Zones: All | DXCC: All\r\n",
			"DE geography  Continents: All | Zones: All | DXCC: All\r\n",
		}},
		{"unrestricted", func(f *filter.Filter) { f.ResetModes() }, []string{"Modes         All; unknown modes included\r\n"}},
		{"empty", func(f *filter.Filter) {
			f.AllBands, f.AllModes, f.AllSources, f.AllEvents = false, false, false, false
			f.AllConfidence, f.AllPathClasses, f.AllDXContinents, f.AllDEContinents = false, false, false, false
			f.Bands, f.Modes, f.Sources, f.Events = nil, nil, nil, nil
		}, []string{
			"Bands         None\r\n", "Modes         None; unknown modes hidden\r\n",
			"Sources       None\r\n", "Events        None; untagged included\r\n",
			"Confidence    None; exempt modes still pass\r\n", "Path          None\r\n",
			"DX geography  Continents: None | Zones: All | DXCC: All\r\n",
			"DE geography  Continents: None | Zones: All | DXCC: All\r\n",
		}},
		{"none", func(f *filter.Filter) {
			f.BlockAllBands, f.BlockAllModes, f.BlockAllSources, f.BlockAllEvents = true, true, true, true
			f.BlockAllConfidence, f.BlockAllPathClasses = true, true
			f.BlockAllDXContinents, f.BlockAllDEContinents = true, true
		}, []string{
			"Bands         None\r\n", "Modes         None; unknown modes hidden\r\n",
			"Sources       None\r\n", "Events        None; untagged included\r\n",
			"Confidence    None; exempt modes still pass\r\n", "Path          None\r\n",
			"DX geography  Continents: None | Zones: All | DXCC: All\r\n",
			"DE geography  Continents: None | Zones: All | DXCC: All\r\n",
		}},
		{"exclusions", func(f *filter.Filter) {
			f.ResetModes()
			f.BlockBands, f.BlockModes = map[string]bool{"80m": true}, map[string]bool{"FT8": true}
			f.Sources = nil
			f.BlockSources, f.BlockEvents = map[string]bool{"SKIMMER": true}, map[string]bool{"WWFF": false}
			f.BlockConfidence, f.BlockPathClasses = map[string]bool{"?": true}, map[string]bool{"UNLIKELY": true}
			f.BlockDXContinents, f.BlockDEContinents = map[string]bool{"EU": true}, map[string]bool{"NA": true}
		}, []string{
			"Bands         All except 80m\r\n", "Modes         All except FT8; unknown modes included\r\n",
			"Sources       All except SKIMMER\r\n", "Events        All except WWFF; untagged included\r\n",
			"Confidence    All except ?; exempt modes still pass\r\n",
			"Path          All except CLOSED, UNLIKELY\r\n",
			"              CLOSED follows UNLIKELY unless explicitly selected\r\n",
			"DX geography  Continents: All except EU | Zones: All | DXCC: All\r\n",
			"DE geography  Continents: All except NA | Zones: All | DXCC: All\r\n",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			tc.set(c.filter)
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("finite state %q missing:\n%s", want, got)
				}
			}
			assertHumanWire(t, got)
		})
	}
}
