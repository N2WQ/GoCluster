// Keep approved transcripts, the independent unquote oracle and whole-response
// boundary checks together: they validate the same fixtures, and splitting them
// would separate the expected output from its lossless and bounded assertions.
package telnet

import (
	"bytes"
	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const humanOverviewExample = "User          N2WQ-1\r\nPreset        CONTEST (modified)\r\n\r\nBands         Only 20m, 40m\r\nModes         CW, FT8; unknown modes hidden\r\nMin SNR       None\r\nComments      All\r\nSources       All (HUMAN, SKIMMER)\r\nEvents        POTA; block WWFF; untagged included\r\nConfidence    All\r\nPath          All\r\nDX states     Suspended by NEARBY; rules retained\r\nDE states     Suspended by NEARBY; rules retained\r\nDX geography  Suspended by NEARBY; rules retained\r\nDE geography  Suspended by NEARBY; rules retained\r\nDX calls      Only 12 patterns; 3 blocked\r\nDE calls      All\r\nNearby        On; grid FN31PR\r\nInclude       Beacons: Off | WWV: On | WCY: On | Announce: On\r\n              Self: Off | Toxic: Off\r\n\r\nDetailed selections: SHOW FILTER FULL\r\nOne category: SHOW FILTER <category>\r\n\r\nLive spots paused during delivery and for at least 30s afterward.\r\nType RESUME when ready. Missed spots are not replayed.\r\n"
const humanSettingsExample = "User          N2WQ-1\r\nPreset        CONTEST (modified)\r\n\r\nDialect       GO\r\nGrid          FN31PR\r\nNoise         SUBURBAN\r\nDedupe        SLOW; effective FAST while NEARBY is active\r\nPath samples  DEFAULT; stations 21, beacons 11\r\nSolar         Every 30 minutes\r\n\r\nSession only\r\nDiagnostics   Off\r\nLive spots    Paused for reading; at least 30s after delivery\r\n              135 spots suppressed\r\n\r\nLive spots paused during delivery and for at least 30s afterward.\r\nType RESUME when ready. Missed spots are not replayed.\r\n"

func humanExampleClient() (*Server, *Client) {
	s, c := readbackTestClient()
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 21, 11
	s.pathPredictor = pathreliability.NewPredictor(cfg, []string{"20m", "40m"})
	s.dedupeFastEnabled, s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true, true
	c.callsign, c.grid, c.gridDerived, c.noiseClass = "N2WQ-1", "FN31PR", false, "SUBURBAN"
	c.setDedupePolicy(dedupePolicySlow)
	c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", Grid: "FN31PR", NoiseClass: "SUBURBAN", DedupePolicy: "SLOW", SolarSummaryMinutes: 30}
	c.setSolarSummaryMinutes(30, s.now())
	c.filter = filter.NewFilter()
	f := c.filter
	f.AllBands, f.AllModes, f.AllEvents = false, false, false
	f.Bands = map[string]bool{"40m": true, "20m": true}
	f.Modes = map[string]bool{"FT8": true, "CW": true}
	f.Sources, f.BlockSources = map[string]bool{}, map[string]bool{}
	f.AllSources = true
	f.Events, f.BlockEvents = map[string]bool{"POTA": true}, map[string]bool{"WWFF": true}
	f.NearbyEnabled = true
	// Readback needs usable runtime cells, not global H3 mapping fixtures.
	f.NearbyUserFine, f.NearbyUserCoarse = 1, 2
	for i := range 12 {
		f.DXCallsigns = append(f.DXCallsigns, fmt.Sprintf("N%dABC*", i))
	}
	f.BlockDXCallsigns = []string{"W1XYZ", "K1XYZ", "N1XYZ"}
	f.IncludeBeacons = filter.DefaultBoolFalse.Pointer()
	f.AllowWWV, f.AllowWCY, f.AllowAnnounce = filter.DefaultBoolTrue.Pointer(), filter.DefaultBoolTrue.Pointer(), filter.DefaultBoolTrue.Pointer()
	f.AllowSelf, f.AllowToxic = filter.DefaultBoolFalse.Pointer(), filter.DefaultBoolFalse.Pointer()
	c.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: &filter.SavedPreset{Filter: *f, Dialect: "go", Grid: "FN31PR", NoiseClass: "QUIET", DedupePolicy: "SLOW", SolarSummaryMinutes: 30}}
	c.readPauseUntilUnixNano.Store(s.now().Add(5 * time.Second).UnixNano())
	c.readPauseSuppressed.Store(135)
	return s, c
}

func assertHumanWire(t *testing.T, response string) {
	t.Helper()
	if len(response) > 65536 || !strings.HasSuffix(response, "\r\n") {
		t.Fatalf("invalid response length/framing: %d", len(response))
	}
	for _, line := range strings.Split(strings.TrimSuffix(response, "\r\n"), "\r\n") {
		if len(line) > 78 {
			t.Fatalf("line exceeds78: %q", line)
		}
		for i := range len(line) {
			if line[i] < 32 || line[i] > 126 {
				t.Fatalf("nonprintable/nonASCII line: %q", line)
			}
		}
	}
}

func TestHumanOverviewAndSettingsApprovedExamples(t *testing.T) {
	for _, tc := range []struct{ command, want string }{{"SHOW FILTER", humanOverviewExample}, {"SHOW SETTINGS", humanSettingsExample}} {
		t.Run(tc.command, func(t *testing.T) {
			s, c := humanExampleClient()
			if !s.handleHumanReadback(c, tc.command) {
				t.Fatal("command not handled")
			}
			message := <-c.controlChan
			got := string(message.raw)
			assertHumanWire(t, got)
			if got != tc.want {
				t.Fatalf("approved transcript mismatch:\n%s\nwant:\n%s", got, tc.want)
			}
			if len(c.controlChan) != 0 || message.readback.epoch == 0 {
				t.Fatal("response split or pause metadata lost")
			}
		})
	}
}

func TestHumanSettingsRestoredPathMinimum(t *testing.T) {
	for _, minimum := range []int{0, 15, 21, 30} {
		t.Run(strconv.Itoa(minimum), func(t *testing.T) {
			s := presetTestServer(t)
			cfg := pathreliability.DefaultConfig()
			cfg.MinObservationCount, cfg.BeaconMinObservationCount = 21, 11
			s.pathPredictor = pathreliability.NewPredictor(cfg, []string{"20m"})
			record := &filter.UserRecord{Filter: *filter.NewFilter(), Dialect: "go", PathMinObservationCount: minimum}
			if err := filter.SaveUserRecord("W1ABC-1", record); err != nil {
				t.Fatal(err)
			}
			c := configurationTestClient(s, "W1ABC-1")
			if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !s.handleHumanReadback(c, "SHOW SETTINGS") {
				t.Fatal("settings not handled")
			}
			got := string((<-c.controlChan).raw)
			assertHumanWire(t, got)
			want := "Path samples  DEFAULT; stations 21, beacons 11\r\n"
			if minimum > 0 && minimum <= 21 {
				want = fmt.Sprintf("Path samples  %d configured; effective stations 21, beacons 11\r\n              Override inactive: not above station minimum 21\r\n", minimum)
			}
			if minimum == 30 {
				want = "Path samples  30 (user minimum); stations 30, beacons 30\r\n              Cluster minimums: stations 21, beacons 11\r\n"
			}
			if !strings.Contains(got, want) {
				t.Fatalf("runtime minima misdescribed:\n%s", got)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("readback mutated durable preferences")
			}
			runtimeMinimum := 0
			if minimum > 21 {
				runtimeMinimum = minimum
			}
			if c.pathSnapshot().pathMinObservationCount != runtimeMinimum {
				t.Fatal("readback changed runtime preparation")
			}
		})
	}
}

// Independent standard-library unquoting is the oracle, not renderer escaping.
func unquoteHumanPieces(t *testing.T, response string) string {
	t.Helper()
	var value strings.Builder
	for _, line := range strings.Split(response, "\r\n") {
		start := strings.IndexByte(line, '"')
		if start < 0 {
			continue
		}
		quoted, err := strconv.QuotedPrefix(line[start:])
		if err != nil {
			t.Fatalf("broken piece: %q: %v", line, err)
		}
		decoded, err := strconv.Unquote(quoted)
		if err != nil {
			t.Fatal(err)
		}
		value.WriteString(decoded)
	}
	return value.String()
}

func TestHumanQuotedValuesRoundTripAndWidths(t *testing.T) {
	values := []string{"", "  leading and trailing  ", "quotes \" and backslash \\", "café 😀\t\n\r", string([]byte{0xff, 0, 0x1b}), strings.Repeat(" \"\\é😀\t ", 100)}
	for _, length := range []int{77, 78, 79} {
		values = append(values, strings.Repeat("A", length-16))
	}
	for i, value := range values {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var h humanResponse
			if err := h.quoted("Value         ", value, ""); err != nil {
				t.Fatal(err)
			}
			assertHumanWire(t, string(h.data))
			if got := unquoteHumanPieces(t, string(h.data)); got != value {
				t.Fatalf("lost bytes: %q != %q", got, value)
			}
			preflight := humanResponse{countOnly: true}
			if err := preflight.quoted("Value         ", value, ""); err != nil || preflight.size != len(h.data) || len(preflight.data) != 0 {
				t.Fatal("preflight differs from CRLF response size")
			}
		})
	}
}

func TestHumanEffectiveLongKeysAndPatterns(t *testing.T) {
	s, c := readbackTestClient()
	long := " " + strings.Repeat("é\\\"\t", 70) + " "
	c.filter.Bands = map[string]bool{long: false}
	got, err := s.renderHumanReadback(c, "FILTER", "BAND", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertHumanWire(t, got)
	if unquoteHumanPieces(t, got) != "" || !strings.Contains(got, "  PASS: NONE\r\n  REJECT: NONE\r\n") {
		t.Fatal("inactive map key was displayed")
	}
	c.filter.DXCallsigns = []string{long, long}
	got, err = s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertHumanWire(t, got)
	if unquoteHumanPieces(t, got) != long+long || !strings.Contains(got, "A + joins quoted pieces") {
		t.Fatal("pattern order/multiplicity lost")
	}
}

func TestHumanFinalResponseExactAndPlusOne(t *testing.T) {
	s, c := readbackTestClient()
	low, high := 0, 65537
	for low+1 < high {
		middle := (low + high) / 2
		c.filter.DXCallsigns = []string{strings.Repeat("A", middle)}
		_, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
		if err == nil {
			low = middle
		} else if errors.Is(err, errReadbackTooLarge) {
			high = middle
		} else {
			t.Fatal(err)
		}
	}
	c.filter.DXCallsigns = []string{strings.Repeat("A", low)}
	got, err := s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.callsign += strings.Repeat("A", 65536-len(got))
	got, err = s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second)
	if err != nil || len(got) != 65536 {
		t.Fatalf("exact boundary: %d,%v", len(got), err)
	}
	assertHumanWire(t, got)
	c.callsign += "A"
	if got, err = s.renderHumanReadback(c, "FILTER", "DXCALL", 30*time.Second); got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("limit+1 returned partial success")
	}
	if !s.handleHumanReadback(c, "SHOW FILTER DXCALL") {
		t.Fatal("size-error command unhandled")
	}
	message := <-c.controlChan
	assertHumanWire(t, string(message.raw))
	if string(message.raw) != "Readback failed: response exceeds 65,536 bytes.\r\n"+readbackFooterLiteral || message.readback.epoch == 0 {
		t.Fatal("size error differs or lacks pause completion")
	}
}

func TestHumanPreviewStableAndBounded(t *testing.T) {
	rules := filter.StringRules{AllowAll: true, Allow: map[string]bool{"40m": true, "20m": true}}
	for range 100 {
		if got := humanRuleSummary(rules, "bands", 64); got != "Only 20m, 40m" {
			t.Fatal(got)
		}
	}
	large := make(map[string]bool, 10000)
	for i := range 10000 {
		large[strconv.Itoa(i)] = true
	}
	if summary := humanRuleSummary(filter.StringRules{AllowAll: true, Allow: large}, "bands", 64); summary != "Only 10000 bands" {
		t.Fatal(summary)
	}
	if preview, ok := shortHumanValue(strings.Repeat("é", 1<<19), 64); ok || preview != "" {
		t.Fatal("oversized string prepared as preview")
	}
	ints := filter.IntRules{Allow: map[int]bool{10: true, 2: true}}
	if got := humanRuleSummary(ints, "zones", 64); got != "Only 2, 10" {
		t.Fatal(got)
	}
}

func TestHumanMatcherSummaryContracts(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.AllBands = true
	c.filter.Bands = map[string]bool{"20m": true, "40m": false}
	c.filter.AllModes = true
	c.filter.Modes = map[string]bool{"CW": true, filter.UnknownModeToken: false}
	got, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil || !strings.Contains(got, "Bands         Only 20m\r\n") || !strings.Contains(got, "Modes         CW; unknown modes hidden\r\n") {
		t.Fatal("nonempty allow map summarized as unrestricted")
	}
	for _, tc := range []struct {
		frequency float64
		mode      string
		want      bool
	}{{14074, "CW", true}, {7000, "CW", false}, {14074, "", false}} {
		if got := c.filter.Matches(spot.NewSpot("K1ABC", "W1XYZ", tc.frequency, tc.mode)); got != tc.want {
			t.Fatal("ordinary matcher disagrees")
		}
	}
	c.filter = filter.NewFilter()
	c.filter.AllEvents = true
	c.filter.Events = map[string]bool{"POTA": false}
	c.filter.BlockEvents = map[string]bool{"WWFF": false}
	got, err = s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil || !strings.Contains(got, "Events        All except WWFF; untagged included\r\n") {
		t.Fatal("EVENT allow_all not respected")
	}
	for _, tc := range []struct {
		event spot.EventMask
		want  bool
	}{{spot.EventSOTA, true}, {spot.EventWWFF, false}, {0, true}} {
		candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		candidate.Events = tc.event
		if c.filter.Matches(candidate) != tc.want {
			t.Fatal("EVENT matcher disagrees")
		}
	}
	for _, rules := range []filter.StringRules{
		{Allow: map[string]bool{"UNLIKELY": true}},
		{Allow: map[string]bool{"CLOSED": true}, Block: map[string]bool{"UNLIKELY": true}},
		{AllowAll: true, Block: map[string]bool{"UNLIKELY": true}},
		{Allow: map[string]bool{"UNLIKELY": true}, Block: map[string]bool{"CLOSED": true}},
	} {
		f := filter.NewFilter()
		f.AllPathClasses, f.BlockAllPathClasses, f.PathClasses, f.BlockPathClasses = rules.AllowAll, rules.BlockAll, rules.Allow, rules.Block
		for _, class := range filter.SupportedPathClasses {
			actual := f.MatchesWithPath(spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW"), class)
			if humanPathPass(class, rules) != actual {
				t.Fatalf("PATH summary disagrees for %s: %v", class, rules)
			}
		}
	}
}

func TestHumanSettingsAvailabilityAndPauseDescriptions(t *testing.T) {
	s, c := humanExampleClient()
	s.dedupeFastEnabled = false
	if !s.handleHumanReadback(c, "SHOW SETTINGS") {
		t.Fatal("unhandled")
	}
	got := string((<-c.controlChan).raw)
	assertHumanWire(t, got)
	if !strings.Contains(got, "Dedupe        SLOW; effective MED while NEARBY is active\r\n              FAST is unavailable on this server\r\n") {
		t.Fatal("fallback dedupe misdescribed")
	}
	s.dedupeMedEnabled, s.dedupeSlowEnabled = false, false
	c.recordProtected = true
	c.readPauseUntilUnixNano.Store(s.now().Add(time.Hour).UnixNano())
	if !s.handleHumanReadback(c, "SHOW SETTINGS") {
		t.Fatal("unhandled")
	}
	got = string((<-c.controlChan).raw)
	assertHumanWire(t, got)
	for _, want := range []string{"Dedupe        SLOW; secondary duplicate suppression disabled\r\n", "Persistence   Temporary defaults; changes will not be saved\r\n", "Existing pause: 3600s remaining\r\n"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	disabled := pathreliability.DefaultConfig()
	disabled.Enabled = false
	s.pathPredictor = pathreliability.NewPredictor(disabled, []string{"20m"})
	got, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil || !strings.Contains(got, "Path samples  DEFAULT; prediction disabled\r\n") {
		t.Fatal("disabled prediction misdescribed")
	}
}

func BenchmarkHumanOverviewBoundedPreparation(b *testing.B) {
	for _, size := range []int{128, 1 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			c.filter.DXCallsigns = []string{strings.Repeat("X", size)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
				if err != nil || !strings.Contains(response, "DX calls      Only 1 pattern\r\n") {
					b.Fatal("incorrect bounded overview")
				}
			}
		})
	}
}

func TestHumanEffectiveEventApprovedExample(t *testing.T) {
	s, c := humanExampleClient()
	c.filter.Events = map[string]bool{"POTA": true, "SOTA": false}
	c.filter.BlockEvents = map[string]bool{"WWFF": false}
	if !s.handleHumanReadback(c, "SHOW FILTER EVENT") {
		t.Fatal("unhandled")
	}
	got := string((<-c.controlChan).raw)
	want := "User          N2WQ-1\r\nPreset        CONTEST (modified)\r\n\r\nEvents\r\n  PASS: POTA, SOTA\r\n  REJECT: WWFF\r\n  Untagged spots are always included.\r\n" + readbackFooterLiteral
	if got != want {
		t.Fatalf("effective EVENT example mismatch:\n%s", got)
	}
	assertHumanWire(t, got)
}

func TestHumanGeographyAndShortPatternsApprovedExample(t *testing.T) {
	s, c := humanExampleClient()
	f := c.filter
	f.NearbyEnabled = false
	f.AllDXContinents = false
	f.DXContinents = map[string]bool{"NA": true, "EU": true}
	f.AllDEZones = false
	f.DEZones = map[int]bool{8: true, 5: true}
	f.DXCallsigns = nil
	f.BlockDXCallsigns = make([]string, 100)
	for i := range f.BlockDXCallsigns {
		f.BlockDXCallsigns[i] = fmt.Sprintf("N%dABC", i)
	}
	f.DECallsigns = []string{"W1*", "K1*"}
	f.BlockDECallsigns = []string{"W1XYZ"}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := "DX geography  Continents: Only EU, NA | Zones: All | DXCC: All\r\n              Grids: All\r\nDE geography  Continents: All | Zones: Only 5, 8 | DXCC: All\r\n              Grids: All\r\nDX calls      All except 100 blocked patterns\r\nDE calls      Only W1*, K1*; block W1XYZ\r\nNearby        Off\r\n"
	if !strings.Contains(got, want) {
		t.Fatalf("geography example mismatch:\n%s", got)
	}
	assertHumanWire(t, got)
}

func TestHumanErrorsAndEscapedExpansionStayBounded(t *testing.T) {
	s, c := readbackTestClient()
	c.filter.DXCallsigns = []string{strings.Repeat("é", 12000)}
	if response, err := s.renderHumanReadback(c, "FILTER", "DXCALL", time.Second); response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("ASCII expansion returned oversized success")
	}
	for _, command := range []string{"SHOW FILTER UNKNOWN", "SHOW SETTINGS EXTRA", "SHOW/FILTER BAND EXTRA"} {
		if !s.handleHumanReadback(c, command) {
			t.Fatal("unhandled")
		}
		message := <-c.controlChan
		assertHumanWire(t, string(message.raw))
		if message.readback.epoch == 0 || !strings.Contains(string(message.raw), "Type RESUME when ready.") {
			t.Fatal("human error lost reading pause")
		}
	}
}

func TestHumanSavedMixedCaseSettingsRemainExact(t *testing.T) {
	s := presetTestServer(t)
	cfg := filter.ConfigurationFromFilter(filter.NewFilter(), filter.SettingsConfiguration{Dialect: "go", NoiseClass: "MixedCase"})
	if err := filter.SaveConfiguration("W1ABC-1", cfg, nil, nil); err != nil {
		t.Fatal(err)
	}
	c := configurationTestClient(s, "W1ABC-1")
	if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Noise         MixedCase\r\n") || strings.Contains(got, "MIXEDCASE") {
		t.Fatal("saved case lost")
	}
	assertHumanWire(t, got)
}

func TestHumanEventNormalizedOverlapMatches(t *testing.T) {
	s, c := readbackTestClient()
	for _, block := range []string{"pota", " POTA ", "pOtA"} {
		c.filter = filter.NewFilter()
		c.filter.AllEvents = false
		c.filter.Events = map[string]bool{"POTA": true, " pota ": false}
		c.filter.BlockEvents = map[string]bool{block: false}
		got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
		if err != nil || !strings.Contains(got, "Events        None; untagged included\r\n") {
			t.Fatalf("normalized EVENT overlap misdescribed: %s", got)
		}
		candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
		candidate.Events = spot.EventPOTA
		if c.filter.Matches(candidate) {
			t.Fatal("matcher unexpectedly allows blocked POTA")
		}
		c.filter.BlockEvents = nil
		got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
		if err != nil || !strings.Contains(got, "Events        POTA; untagged included\r\n") || !c.filter.Matches(candidate) {
			t.Fatal("canonical aliases were counted as different effective events")
		}
	}
}

func TestHumanZoneAndUnmatchableStringRules(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.AllDXZones = true
	c.filter.DXZones = map[int]bool{0: true}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "Zones: None") {
		t.Fatal("invalid allowed zone described as usable")
	}
	candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	for _, zone := range []int{0, 5} {
		candidate.DXMetadata.CQZone = zone
		if c.filter.Matches(candidate) {
			t.Fatal("invalid zone allow unexpectedly passes")
		}
	}
	c.filter = filter.NewFilter()
	c.filter.AllBands = true
	c.filter.Bands = map[string]bool{"": true, "20m ": true}
	got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "Bands         None\r\n") || c.filter.Matches(candidate) {
		t.Fatal("unmatchable string rules misdescribed")
	}
}

func TestHumanPreviewPreflightBeforeCopy(t *testing.T) {
	values := []string{"W1*", "café", strings.Repeat("x", 1<<20)}
	if allocations := testing.AllocsPerRun(100, func() {
		if preview, fits := humanPatternList(values, 64); fits || preview != "" {
			t.Fatal("late oversize pattern accepted")
		}
	}); allocations != 0 {
		t.Fatalf("pattern preview copied before aggregate fit: %f allocations", allocations)
	}
	entries := map[string]bool{"café": true, strings.Repeat("x", 1<<20): true}
	if allocations := testing.AllocsPerRun(100, func() {
		if preview, count, fits := humanRuleList(entries, func(_ string, value bool) bool { return value }, 64); fits || preview != "" || count != 2 {
			t.Fatal("late oversize map accepted")
		}
	}); allocations != 0 {
		t.Fatalf("map preview copied before aggregate fit: %f allocations", allocations)
	}
}
